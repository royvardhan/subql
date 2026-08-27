// Copyright 2020-2025 SubQuery Pte Ltd authors & contributors
// SPDX-License-Identifier: GPL-3.0

import assert from 'assert';
import {isMainThread} from 'node:worker_threads';
import {OnApplicationShutdown, Injectable} from '@nestjs/common';
import {Interval} from '@nestjs/schedule';
import chalk from 'chalk';
import {ApiConnectionError, ApiErrorType} from '../api.connection.error';
import {IApiConnectionSpecific} from '../api.service';
import {NodeConfig} from '../configure';
import {getLogger} from '../logger';
import {delay} from '../utils';
import {ConnectionPoolStateManager} from './connectionPoolState.manager';

const logger = getLogger('connection-pool');

const LOG_INTERVAL_MS = 60 * 1000; // Log every 60 seconds
const MAX_RECONNECT_BACKOFF_SEC = 60;
// A disconnected endpoint reconnects for this long before it is given up on and removed from the
// pool. Long enough to ride out a node restart or maintenance window; bounded so an endpoint that
// will never heal (revoked key, dead host) is eventually removed, which is what triggers the
// "all endpoints removed" exit for the last one instead of retrying forever.
const DEFAULT_RECONNECT_GIVE_UP_MS = 30 * 60 * 1000;
function reconnectGiveUpMs(): number {
  const override = Number(process.env.SUBQL_RECONNECT_GIVE_UP_MS);
  return Number.isFinite(override) && override > 0 ? override : DEFAULT_RECONNECT_GIVE_UP_MS;
}

export const errorTypeToScoreAdjustment = {
  [ApiErrorType.Timeout]: -10,
  [ApiErrorType.Connection]: -20,
  [ApiErrorType.RateLimit]: -10,
  [ApiErrorType.Default]: -5,
};

type ResultCacheEntry<T> = {
  endpoint: string;
  type: 'success' | 'error';
  data: T;
};

@Injectable()
export class ConnectionPoolService<T extends IApiConnectionSpecific<any, any, any>> implements OnApplicationShutdown {
  private allApi: Record<string, T> = {};
  private cachedEndpoint: string | undefined;
  private reconnectingEndpoints: Record<string, Promise<void> | undefined> = {};
  private resultCache: Array<ResultCacheEntry<number | ApiConnectionError['errorType']>> = [];
  private lastCacheFlushTime: number = Date.now();
  private cacheSizeThreshold = 10;
  private cacheFlushInterval = 60 * 100;

  constructor(
    private nodeConfig: NodeConfig,
    private poolStateManager: ConnectionPoolStateManager<T>
  ) {
    this.cacheSizeThreshold = this.nodeConfig.batchSize;
  }

  async onApplicationShutdown(): Promise<void> {
    await Promise.all(Object.values(this.allApi).map((api) => api?.apiDisconnect()));
  }

  async addToConnections(api: T, endpoint: string): Promise<void> {
    this.allApi[endpoint] = api;
    await this.poolStateManager.addToConnections(endpoint, endpoint === this.nodeConfig.primaryNetworkEndpoint?.[0]);
    if (api !== null) {
      await this.updateNextConnectedApiIndex();
    }
  }

  async addBatchToConnections(endpointToApiIndex: Record<string, T>): Promise<void> {
    for (const endpoint in endpointToApiIndex) {
      await this.addToConnections(endpointToApiIndex[endpoint], endpoint);
    }
  }

  async updateConnection(api: T, endpoint: string): Promise<void> {
    if (this.allApi[endpoint] === undefined) {
      throw new Error(`Attempting to update connection that does not exist: ${endpoint}`);
    }
    this.allApi[endpoint] = api;
    await this.updateNextConnectedApiIndex();
  }

  async connectToApi(endpoint: string): Promise<void> {
    await this.allApi[endpoint].apiConnect();
  }

  private async updateNextConnectedApiIndex(): Promise<void> {
    const newEndpoint = await this.poolStateManager.getNextConnectedEndpoint(Object.keys(this.allApi));
    if (!!newEndpoint && this.allApi[newEndpoint] === null) {
      return this.updateNextConnectedApiIndex();
    }
    this.cachedEndpoint = newEndpoint;
  }

  get api(): T {
    const endpoint = this.cachedEndpoint;

    if (endpoint === undefined) {
      throw new Error(
        'All endpoints in the pool are either suspended due to rate limits or attempting to reconnect. Please wait or add healthier endpoints.'
      );
    }
    const api = this.allApi[endpoint];

    assert(api, `Api for endpoint ${endpoint} not found`);

    const wrappedApi = new Proxy(api, {
      get: (target, prop, receiver) => {
        if (prop === 'fetchBlocks') {
          return async (heights: number[], ...args: any): Promise<any> => {
            try {
              // Check if the endpoint is rate-limited
              if (await this.poolStateManager.getFieldValue(endpoint, 'rateLimited')) {
                logger.info(`throtling on ratelimited endpoint 10s`);
                await delay(10);
              }

              const start = Date.now();
              const result = await target.fetchBlocks(heights, ...args);
              const end = Date.now();
              await this.handleApiSuccess(endpoint, end - start);
              await this.poolStateManager.setFieldValue(endpoint, 'lastRequestTime', end); // Update the last request time
              return result;
            } catch (error: any) {
              await this.handleApiError(endpoint, target.handleError(error));
              throw error;
            }
          };
        }

        // Delegate other property access and method calls to the original api object
        return Reflect.get(target, prop, receiver);
      },
    });
    return wrappedApi as T;
  }

  get numConnections(): number {
    return this.poolStateManager.numConnections;
  }

  /**
   * Reconnects a disconnected endpoint, backing off up to MAX_RECONNECT_BACKOFF_SEC between
   * attempts, for up to the give-up window (SUBQL_RECONNECT_GIVE_UP_MS, default 30 min). While
   * disconnected the endpoint is simply not selected for requests; a transient outage is expected
   * to end well inside the window. If it does not, the endpoint is removed from the pool, which for
   * the last remaining endpoint triggers the "all connections removed" exit rather than an endpoint
   * that retries and logs forever.
   *
   * Only relevant to multi-endpoint pools: with a single endpoint `handleApiError` short-circuits
   * (pool size 1) and never marks it disconnected, so this is never called. Single-endpoint
   * resilience comes from the resilient websocket provider (which waits out the reconnect) and the
   * api service's fetch retry.
   */
  async handleApiDisconnects(endpoint: string): Promise<void> {
    logger.warn(`disconnected from ${endpoint}`);

    const giveUpMs = reconnectGiveUpMs();
    const deadline = Date.now() + giveUpMs;

    const reconnect = async (): Promise<void> => {
      for (let attempt = 0; ; attempt++) {
        try {
          logger.info(`Attempting to reconnect to ${endpoint} (attempt ${attempt + 1})`);

          await this.allApi[endpoint].apiConnect();
          await this.poolStateManager.setFieldValue(endpoint, 'connected', true);
          this.reconnectingEndpoints[endpoint] = undefined;
          await this.updateNextConnectedApiIndex();
          logger.info(`Reconnected to ${endpoint} successfully`);
          return;
        } catch (e) {
          if (Date.now() >= deadline) {
            logger.error(
              `Reconnection to ${endpoint} kept failing for ${Math.round(giveUpMs / 1000)}s, removing it from the pool: ${e}`
            );
            this.reconnectingEndpoints[endpoint] = undefined;
            await this.poolStateManager.removeFromConnections(endpoint);
            return;
          }
          const backoff = Math.min(Math.pow(2, attempt), MAX_RECONNECT_BACKOFF_SEC);
          logger.error(`Reconnection to ${endpoint} failed, retrying in ${backoff}s: ${e}`);
          await delay(backoff);
        }
      }
    };

    this.reconnectingEndpoints[endpoint] = reconnect();

    await this.handleConnectionStateChange();
  }

  @Interval(LOG_INTERVAL_MS)
  async logEndpointStatus(): Promise<void> {
    if (!isMainThread) {
      return;
    }
    const suspendedEnpoints = await this.poolStateManager.getSuspendedEndpoints();

    if (suspendedEnpoints.length === 0) {
      return;
    }

    let suspendedEndpointsInfo = chalk.yellow('Suspended endpoints:\n');

    await Promise.all(
      suspendedEnpoints.map(async (e) => {
        const endpoint = chalk.cyan(new URL(e).hostname);
        const failures = chalk.red(`Failures: ${await this.poolStateManager.getFieldValue(e, 'failureCount')}`);
        const backoff = chalk.yellow(`Backoff (ms): ${await this.poolStateManager.getFieldValue(e, 'backoffDelay')}`);

        suspendedEndpointsInfo += `\n- ${endpoint}\n  ${failures}\n  ${backoff}\n`;
      })
    );

    logger.info(suspendedEndpointsInfo);
  }

  async handleApiSuccess(endpoint: string, responseTime: number): Promise<void> {
    this.addToResultCache(endpoint, 'success', responseTime);
    await this.flushResultCacheIfNeeded();
    await this.handleConnectionStateChange();
  }

  async handleApiError(endpoint: string, error: ApiConnectionError): Promise<void> {
    this.addToResultCache(endpoint, 'error', error.errorType);
    await this.flushResultCacheIfNeeded();
    await this.handleConnectionStateChange();
  }

  async handleConnectionStateChange(): Promise<void> {
    // Update the cached value
    await this.updateNextConnectedApiIndex();
    const disconnectedEndpoints = await this.poolStateManager.getDisconnectedEndpoints();
    disconnectedEndpoints.map((endpoint) => {
      if (this.reconnectingEndpoints[endpoint] !== undefined) {
        return;
      }
      void this.handleApiDisconnects(endpoint);
    });
  }

  private addToResultCache(
    endpoint: string,
    dataType: 'success' | 'error',
    data: number | ApiConnectionError['errorType']
  ): void {
    const entry: ResultCacheEntry<number | ApiConnectionError['errorType']> = {
      endpoint,
      type: dataType,
      data,
    };
    this.resultCache.push(entry);
  }

  private async flushResultCacheIfNeeded(): Promise<void> {
    const currentTime = Date.now();
    const timeSinceLastFlush = currentTime - this.lastCacheFlushTime;

    if (this.resultCache.length >= this.cacheSizeThreshold || timeSinceLastFlush >= this.cacheFlushInterval) {
      await this.flushResultCache();
    }
  }

  private async flushResultCache(): Promise<void> {
    const successResults: Array<{endpoint: string; responseTime: number}> = [];
    const errorResults: Array<{endpoint: string; errorType: ApiErrorType}> = [];

    for (const result of this.resultCache) {
      if (result.type === 'success') {
        successResults.push({endpoint: result.endpoint, responseTime: result.data as number});
      } else {
        errorResults.push({endpoint: result.endpoint, errorType: result.data as ApiErrorType});
      }
    }

    await this.poolStateManager.handleBatchApiSuccess(successResults);
    await this.poolStateManager.handleBatchApiError(errorResults);

    this.resultCache = [];
    this.lastCacheFlushTime = Date.now();
    await this.handleConnectionStateChange();
  }

  async updateChainTypes(newChainTypes: unknown): Promise<void> {
    for (const endpoint in this.allApi) {
      await this.allApi[endpoint].updateChainTypes?.(newChainTypes);
    }
    logger.info(`Network chain types updated!`);
  }
}
