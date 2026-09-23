// Copyright 2020-2025 SubQuery Pte Ltd authors & contributors
// SPDX-License-Identifier: GPL-3.0

import assert from 'assert';
import {Inject, Injectable, OnApplicationShutdown} from '@nestjs/common';
import {hashName} from '@subql/utils';
import {Transaction, Sequelize} from '@subql/x-sequelize';
import {Connection} from '@subql/x-sequelize/types/dialects/abstract/connection-manager';
import {Notification, PoolClient} from 'pg';
import {IBlockchainService} from '../blockchain.service';
import {NodeConfig} from '../configure';
import {createRewindTrigger, createRewindTriggerFunction, getTriggers} from '../db';
import {MultiChainRewindEvent} from '../events';
import {getLogger} from '../logger';
import {delay, mainThreadOnly, timeout} from '../utils';
import {MultiChainRewindStatus} from './entities';
import {StoreService} from './store.service';
import {MULTICHAIN_REWIND_LOCK_KEY, PlainGlobalModel} from './storeModelProvider/global/global';
import {Header} from './types';

const logger = getLogger('MultiChainRewindService');

const REWIND_POLL_INTERVAL_MS = 30_000;
const LISTENER_HEARTBEAT_INTERVAL_MS = 30_000;
const LISTENER_HEARTBEAT_TIMEOUT_SEC = 10;
const LISTENER_RECONNECT_MAX_DELAY_SEC = 30;

/**
 * Working principle:
 * multiChainRewindService is primarily responsible for coordinating multi-chain projects.
 * When global.rewindLock changes, a PG trigger sends a notification, and all subscribed chain projects will receive the rollback notification.
 * This triggers a rollback process, where the fetch service handles the message by clearing the queue.
 * During the next fillNextBlockBuffer loop, if it detects the rewinding state, it will execute the rollback.
 *
 * The lock table is the source of truth and notifications only make the reaction faster: every path that changes the
 * in-memory status (a notification, the periodic poll, a listener reconnect, startup) goes through reconcile(), which
 * reads this chain's row. A notification lost to a dead listener connection is therefore recovered at the next poll.
 */
@Injectable()
export class MultiChainRewindService implements OnApplicationShutdown {
  private _shutdown = false;
  private _status: MultiChainRewindStatus = MultiChainRewindStatus.Normal;
  private _chainId?: string;
  private dbSchema: string;
  private rewindTriggerName: string;
  private startHeight = 0;
  private pgListener?: PoolClient;
  private _globalModel?: PlainGlobalModel = undefined;
  private processingPromise: Promise<void> = Promise.resolve();
  private enabled = false;
  private _waitingFor: string[] = [];
  // Timestamp of the rewind this chain last released, to tell an uncommitted release apart from a new, earlier lock
  private lastCompletedRewindTimestamp?: Date;
  private pollTimer?: NodeJS.Timeout;
  private heartbeatTimer?: NodeJS.Timeout;
  private reconnecting = false;
  pollIntervalMs = REWIND_POLL_INTERVAL_MS;
  heartbeatIntervalMs = LISTENER_HEARTBEAT_INTERVAL_MS;
  heartbeatTimeoutSec = LISTENER_HEARTBEAT_TIMEOUT_SEC;
  waitRewindHeader?: Header;
  constructor(
    private nodeConfig: NodeConfig,
    private sequelize: Sequelize,
    private storeService: StoreService,
    @Inject('IBlockchainService') private readonly blockchainService: IBlockchainService
  ) {
    this.dbSchema = this.nodeConfig.dbSchema;
    this.rewindTriggerName = hashName(this.dbSchema, 'rewind_trigger', '_global');
  }

  get chainId(): string {
    assert(this._chainId, 'chainId is not set');
    return this._chainId;
  }

  get disableRewindLock(): boolean {
    return this.nodeConfig.disableMultichainRewindLock;
  }

  private set status(status: MultiChainRewindStatus) {
    this._status = status;
  }

  get status(): MultiChainRewindStatus {
    assert(this._status, 'status is not set');
    return this._status;
  }

  // Chains that still have to complete the current rewind, as of the last read of the lock table
  get waitingFor(): string[] {
    return this._waitingFor;
  }

  get globalModel(): PlainGlobalModel {
    if (!this._globalModel) {
      this._globalModel = new PlainGlobalModel(this.dbSchema, this.chainId, this.storeService.globalDataRepo);
    }
    return this._globalModel;
  }

  async onApplicationShutdown(): Promise<void> {
    this._shutdown = true;
    if (this.pollTimer) clearInterval(this.pollTimer);
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    await this.processingPromise;
    if (this.pgListener) {
      this.sequelize.connectionManager.releaseConnection(this.pgListener as Connection);
      this.pgListener = undefined;
    }
  }

  @mainThreadOnly()
  async init(chainId: string, reindex?: (targetHeader: Header) => Promise<void>): Promise<Header | undefined> {
    this._chainId = chainId;

    if (reindex === undefined) {
      // When using the reindex command, this parameter is not required.
      return;
    }
    if (!this.storeService.isMultichain) return;
    if (this.disableRewindLock) {
      logger.info(`Multichain rewind lock is disabled, chainId: ${this.chainId}`);
      await this.registerParticipation(false);
      return;
    }

    await this.sequelize.query(`${createRewindTriggerFunction(this.dbSchema)}`);
    const rewindTriggers = await getTriggers(this.sequelize, this.rewindTriggerName);
    if (rewindTriggers.length === 0) {
      await this.sequelize.query(`${createRewindTrigger(this.dbSchema)}`);
    }

    const startHeight = await this.storeService.modelProvider.metadata.find('startHeight');
    assert(startHeight !== undefined, 'startHeight is not set');
    this.startHeight = startHeight;

    await this.registerParticipation(true);
    this.enabled = true;

    // Register a listener and create a schema notification sending function.
    await this.registerPgListener();

    // Check whether the current state is in rollback.
    // If a global lock situation occurs, prioritize setting it to the WaitOtherChain state. If a rollback is still required, then set it to the rewinding state.
    await this.reconcile('startup');

    this.pollTimer = setInterval(() => void this.reconcile('poll'), this.pollIntervalMs);
    this.heartbeatTimer = setInterval(() => void this.heartbeat(), this.heartbeatIntervalMs);

    if (this.waitRewindHeader) {
      const rewindHeader = {...this.waitRewindHeader};
      await reindex(rewindHeader);
      return rewindHeader;
    }
  }

  /**
   * Other chains only enroll this chain in a rewind when this key is true, so it is written straight to the table
   * rather than through the store cache, which is not flushed until later in startup.
   */
  private async registerParticipation(participates: boolean): Promise<void> {
    await this.storeService.modelProvider.metadata.model.upsert({key: MULTICHAIN_REWIND_LOCK_KEY, value: participates});
  }

  private async registerPgListener(): Promise<void> {
    if (this.pgListener) return;

    // Creating a new pgClient is to avoid using the same database connection as the block scheduler,
    // which may prevent real-time listening to rollback events.
    const listener = (await this.sequelize.connectionManager.getConnection({
      type: 'read',
    })) as PoolClient;
    this.pgListener = listener;

    listener.on('notification', this.notifyHandle.bind(this));
    listener.on('error', (e) => void this.reconnectListener(listener, e));
    listener.on('end', () => void this.reconnectListener(listener, new Error('connection ended')));

    await listener.query(`LISTEN "${this.rewindTriggerName}"`);
    logger.info(`Register rewind listener success, chainId: ${this.chainId}`);
  }

  /**
   * A connection that only listens sends no traffic, so a proxy or idle timeout can drop it without the client ever
   * noticing. The heartbeat both keeps it alive and detects a half-open socket.
   */
  private async heartbeat(): Promise<void> {
    const listener = this.pgListener;
    if (!listener || this.reconnecting || this._shutdown) return;
    try {
      await timeout(listener.query('SELECT 1'), this.heartbeatTimeoutSec, 'Rewind listener heartbeat timed out');
    } catch (e: any) {
      await this.reconnectListener(listener, e);
    }
  }

  /**
   * The pool keeps a dead listener connection checked out, so notifications sent after it dropped would be lost.
   * Replace it, then read the lock table because the state may have moved while the listener was down.
   */
  private async reconnectListener(listener: PoolClient, e: Error): Promise<void> {
    if (this._shutdown || this.reconnecting || this.pgListener !== listener) return;
    this.reconnecting = true;
    logger.warn(`Rewind listener connection lost, chainId: ${this.chainId}: ${e.message}`);
    this.pgListener = undefined;
    try {
      // Ending a half-open socket can block on the dead write until the TCP retries give up, so don't wait for it
      await timeout(
        this.sequelize.connectionManager.destroyConnection(listener as Connection),
        this.heartbeatTimeoutSec,
        'destroy timed out'
      );
    } catch (destroyErr: any) {
      logger.debug(`Failed to destroy rewind listener connection: ${destroyErr.message}`);
    }

    try {
      for (let attempt = 1; !this._shutdown; attempt++) {
        try {
          await this.registerPgListener();
          await this.reconcile('listener reconnect');
          return;
        } catch (registerErr: any) {
          const wait = Math.min(attempt, LISTENER_RECONNECT_MAX_DELAY_SEC);
          logger.warn(
            `Failed to re-register rewind listener (attempt ${attempt}): ${registerErr.message}, retry in ${wait}s`
          );
          await delay(wait);
        }
      }
    } finally {
      this.reconnecting = false;
    }
  }

  private notifyHandle(msg: Notification) {
    if (this._shutdown || !msg.payload) return;
    const {chainId, event} = JSON.parse(msg.payload) as {chainId: string; event: MultiChainRewindEvent};
    if (chainId !== this.chainId) return;
    logger.info(`Received rewind event: ${event}, chainId: ${this.chainId}`);
    void this.reconcile(`event ${event}`);
  }

  // Serialise status changes; a failure is logged and never blocks the calls queued behind it
  private async enqueue(fn: () => Promise<void>): Promise<void> {
    const run = this.processingPromise.then(fn).catch((e: any) => {
      logger.error(e, `Failed to reconcile rewind status, chainId: ${this.chainId}`);
    });
    this.processingPromise = run;
    return run;
  }

  /**
   * Bring the in-memory status in line with this chain's row in the lock table. Idempotent, so it is safe to run on
   * every notification, poll and reconnect. No-op when the lock is not in use.
   */
  async reconcile(reason = 'manual'): Promise<void> {
    if (!this.enabled) return;
    return this.enqueue(async () => {
      if (this._shutdown) return;
      const chains = await this.globalModel.listChains();
      const own = chains.find((chain) => chain.chainId === this.chainId);
      this._waitingFor = chains
        .filter((chain) => chain.status === MultiChainRewindStatus.Incomplete)
        .map((chain) => chain.chainId);
      const before = this.status;

      if (!own) {
        if (before === MultiChainRewindStatus.Incomplete) {
          // The other chains already deleted this chain's rows after the target, so the pending rewind must still
          // run; it will take a new lock when it does
          logger.warn(`Rewind lock was cleared before this chain rewound, chainId: ${this.chainId}`);
          return;
        }
        await this.setStatus(MultiChainRewindStatus.Normal);
      } else if (own.status === MultiChainRewindStatus.Complete) {
        await this.setStatus(MultiChainRewindStatus.Complete);
      } else {
        const rewindTimestamp = own.rewindTimestamp;
        // Our own release is not committed yet, so the row still reads incomplete at the timestamp we just rewound to
        if (
          before === MultiChainRewindStatus.Complete &&
          this.lastCompletedRewindTimestamp?.getTime() === rewindTimestamp.getTime()
        ) {
          return;
        }
        await this.setStatus(MultiChainRewindStatus.Incomplete, rewindTimestamp);
      }

      if (before !== this.status) {
        logger.info(`Rewind status changed from ${before} to ${this.status} (${reason}), chainId: ${this.chainId}`);
      }
    });
  }

  private async searchWaitRewindHeader(rewindTimestamp: Date): Promise<Header> {
    const rewindBlockHeader = await this.getHeaderByBinarySearch(rewindTimestamp);
    // The blockHeader.timestamp obtained from the query cannot be used directly, as it will cause an infinite loop.
    // Different chains have timestamp discrepancies, which will result in infinite backward tracing.
    return {...rewindBlockHeader, timestamp: rewindTimestamp};
  }

  /**
   * If the set rewindTimestamp is greater than or equal to the current blockHeight, we do nothing because we will roll back to an earlier time.
   * If the set rewindTimestamp is less than the current blockHeight, we should roll back to the earlier rewindTimestamp.
   * @param rewindTimestamp rewindTimestamp in milliseconds
   */
  @mainThreadOnly()
  async acquireGlobalRewindLock(rewindTimestamp: Date): Promise<boolean> {
    if (this.disableRewindLock) {
      return true;
    }
    const {lockTimestamp} = await this.globalModel.acquireGlobalRewindLock(rewindTimestamp);

    const existEarlierLock = lockTimestamp < rewindTimestamp;
    if (!existEarlierLock) {
      logger.info(`setGlobalRewindLock success chainId: ${this.chainId}, rewindTimestamp: ${rewindTimestamp}`);
    }
    return !existEarlierLock;
  }

  @mainThreadOnly()
  /**
   * Check if the height is consistent before unlocking.
   * @param tx
   * @param rewindTimestamp The timestamp to roll back to.
   * @param allowRewindTimestamp Set a rewind-allowed height; only heights greater than or equal this can be released.
   * @returns the number of remaining rewind chains
   */
  async releaseChainRewindLock(tx: Transaction, rewindTimestamp: Date, allowRewindTimestamp?: Date): Promise<number> {
    if (this.disableRewindLock) {
      logger.info(
        `Rewind lock disabled, skipping rewind release chainId: ${this.chainId}, rewindTimestamp: ${rewindTimestamp}`
      );
      return 0;
    }
    const chainsCount = await this.globalModel.releaseChainRewindLock(tx, rewindTimestamp, allowRewindTimestamp);
    // The current chain has completed the rewind, and we still need to wait for other chains to finish.
    // When fully synchronized, set the status back to normal by pgListener.
    this.lastCompletedRewindTimestamp = rewindTimestamp;
    await this.setStatus(MultiChainRewindStatus.Complete);
    logger.info(`Rewind success chainId: ${JSON.stringify({chainsCount, chainId: this.chainId, rewindTimestamp})}`);
    return chainsCount;
  }

  private async setStatus(status: MultiChainRewindStatus, rewindTimestamp?: Date) {
    if (status === MultiChainRewindStatus.Incomplete) {
      assert(rewindTimestamp, 'rewindTimestamp is not set');
      this.status = MultiChainRewindStatus.Incomplete;
      // The header is a function of the timestamp, so a repeated notification for the same target needs no new search
      if (this.waitRewindHeader?.timestamp.getTime() !== rewindTimestamp.getTime()) {
        this.waitRewindHeader = await this.searchWaitRewindHeader(rewindTimestamp);
      }
    } else {
      this.status = status;
      this.waitRewindHeader = undefined;
      if (status === MultiChainRewindStatus.Normal) {
        this.lastCompletedRewindTimestamp = undefined;
      }
    }
  }

  /**
   * Get the block header closest to the given timestamp
   * @param timestamp To find the block closest to a given timestamp
   * @returns
   */
  private async getHeaderByBinarySearch(timestamp: Header['timestamp']): Promise<Header> {
    let left = this.startHeight;
    let {height: right} = await this.storeService.getLastProcessedBlock();
    let searchNum = 0;
    while (left < right) {
      searchNum++;
      const mid = Math.floor((left + right) / 2);
      const header = await this.blockchainService.getHeaderForHeight(mid);

      if (header.timestamp === timestamp) {
        return header;
      } else if (header.timestamp < timestamp) {
        left = mid + 1;
      } else {
        right = mid;
      }
    }

    const targetHeader = await this.blockchainService.getHeaderForHeight(left);
    logger.info(`Binary search times: ${searchNum}, target Header: ${JSON.stringify(targetHeader)}`);

    return targetHeader;
  }
}
