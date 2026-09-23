// Copyright 2020-2025 SubQuery Pte Ltd authors & contributors
// SPDX-License-Identifier: GPL-3.0

import {buildSchemaFromString} from '@subql/utils';
import {QueryTypes, Sequelize} from '@subql/x-sequelize';
import {NodeConfig} from '../configure';
import {DbOption} from '../db';
import {delay} from '../utils';
import {MultiChainRewindStatus} from './entities';
import {MultiChainRewindService} from './multiChainRewind.service';
import {StoreService} from './store.service';
import {PlainStoreModelService} from './storeModelProvider';

const option: DbOption = {
  host: process.env.DB_HOST ?? '127.0.0.1',
  port: process.env.DB_PORT ? Number(process.env.DB_PORT) : 5432,
  username: process.env.DB_USER ?? 'postgres',
  password: process.env.DB_PASS ?? 'postgres',
  database: process.env.DB_DATABASE ?? 'postgres',
  timezone: 'utc',
};

jest.setTimeout(60000);
// Mock 1740100000 is the timestamp of the genesis block
const genBlockTimestamp = (height: number) => {
  const rewindTimestamp = (1740100000 + height) * 1000;
  return {rewindTimestamp, rewindDate: new Date(rewindTimestamp)};
};

const testSchemaName = 'test_multi_chain_rewind';
const schema = buildSchemaFromString(`
  type Account @entity {
    id: ID! # Account address
    balance: Int
  }
`);

async function createChainProject(chainId: string, mockBlockchainService: any, sequelize: Sequelize) {
  const nodeConfig = new NodeConfig({
    subquery: 'test',
    dbSchema: testSchemaName,
    proofOfIndex: true,
    enableCache: false,
    multiChain: true,
  });
  const project = {network: {chainId}, schema} as any;
  const dbModel = new PlainStoreModelService(sequelize, nodeConfig);
  const storeService = new StoreService(sequelize, nodeConfig, dbModel, project);
  await storeService.initCoreTables(testSchemaName);
  const tx = await sequelize.transaction();
  await storeService.init(testSchemaName, tx);
  await storeService.modelProvider.metadata.set('chain', chainId, tx);
  await storeService.modelProvider.metadata.set('startHeight', 1, tx);
  await storeService.modelProvider.metadata.set('lastProcessedHeight', 10000, tx);
  await storeService.modelProvider.metadata.set(
    'lastProcessedBlockTimestamp',
    genBlockTimestamp(10000).rewindTimestamp,
    tx
  );
  await tx.commit();

  const multiChainRewindService = new MultiChainRewindService(
    nodeConfig,
    sequelize,
    storeService,
    mockBlockchainService
  );

  const reindex = jest.fn();

  // Initialize the service
  await multiChainRewindService.init(chainId, reindex);

  return {sequelize, storeService, multiChainRewindService};
}

describe('MultiChain Rewind Service', () => {
  const notifyHandleDelay = 0.5;
  const lockInfoSql = `SELECT "chainId","rewindTimestamp","status" FROM "${testSchemaName}"."_global";`;

  const chainId1 = 'chain1';
  const chainId2 = 'chain2';
  let sequelize: Sequelize;
  let sequelize1: Sequelize;
  let sequelize2: Sequelize;
  let storeService1: StoreService, multiChainRewindService1: MultiChainRewindService;
  let storeService2: StoreService, multiChainRewindService2: MultiChainRewindService;

  // Mock IBlockchainService
  const mockBlockchainService = {
    getHeaderForHeight: jest.fn((height: number) => ({
      blockHeight: height,
      timestamp: genBlockTimestamp(height).rewindDate,
      blockHash: `hash${height}`,
      parentHash: height > 0 ? `hash${height - 1}` : '',
    })),
  };

  beforeEach(async () => {
    sequelize = new Sequelize(
      `postgresql://${option.username}:${option.password}@${option.host}:${option.port}/${option.database}`,
      option
    );
    await sequelize.authenticate();
    await sequelize.query(`CREATE SCHEMA ${testSchemaName};`);

    sequelize1 = new Sequelize(
      `postgresql://${option.username}:${option.password}@${option.host}:${option.port}/${option.database}`,
      option
    );
    const projectChain1 = await createChainProject(chainId1, mockBlockchainService, sequelize1);
    storeService1 = projectChain1.storeService;
    multiChainRewindService1 = projectChain1.multiChainRewindService;

    sequelize2 = new Sequelize(
      `postgresql://${option.username}:${option.password}@${option.host}:${option.port}/${option.database}`,
      option
    );
    const projectChain2 = await createChainProject(chainId2, mockBlockchainService, sequelize2);
    storeService2 = projectChain2.storeService;
    multiChainRewindService2 = projectChain2.multiChainRewindService;
  });

  afterEach(async () => {
    await multiChainRewindService1.onApplicationShutdown();
    await multiChainRewindService2.onApplicationShutdown();
    await sequelize.query(`DROP SCHEMA ${testSchemaName} CASCADE;`);
    await Promise.all([sequelize.close(), sequelize1.close(), sequelize2.close()]);
  });

  describe('acquireGlobalRewindLock', () => {
    it('setGlobalRewindLock should set the lock timestamp', async () => {
      const {rewindDate} = genBlockTimestamp(5);
      const result = await multiChainRewindService1.acquireGlobalRewindLock(rewindDate);
      expect(result).toEqual(true);

      const res = await sequelize.query(lockInfoSql, {type: QueryTypes.SELECT});
      expect(res).toEqual(
        expect.arrayContaining([
          {chainId: chainId1, rewindTimestamp: rewindDate, status: MultiChainRewindStatus.Incomplete},
          {chainId: chainId2, rewindTimestamp: rewindDate, status: MultiChainRewindStatus.Incomplete},
        ])
      );
    });

    it('A rewind can be moved further back when there is already one in progress.', async () => {
      const {rewindDate: rewindDate10} = genBlockTimestamp(10);
      const {rewindDate: rewindDate5} = genBlockTimestamp(5);

      await expect(multiChainRewindService1.acquireGlobalRewindLock(rewindDate10)).resolves.toBe(true);
      let res = await sequelize.query(lockInfoSql, {type: QueryTypes.SELECT});
      expect(res).toEqual(
        expect.arrayContaining([
          {chainId: chainId1, rewindTimestamp: rewindDate10, status: MultiChainRewindStatus.Incomplete},
          {chainId: chainId2, rewindTimestamp: rewindDate10, status: MultiChainRewindStatus.Incomplete},
        ])
      );

      await expect(multiChainRewindService1.acquireGlobalRewindLock(rewindDate5)).resolves.toBe(true);
      res = await sequelize.query(lockInfoSql, {type: QueryTypes.SELECT});
      expect(res).toEqual(
        expect.arrayContaining([
          {chainId: chainId1, rewindTimestamp: rewindDate5, status: MultiChainRewindStatus.Incomplete},
          {chainId: chainId2, rewindTimestamp: rewindDate5, status: MultiChainRewindStatus.Incomplete},
        ])
      );
    });

    it('Not allowed to lock further backward', async () => {
      const {rewindDate: rewindDate10} = genBlockTimestamp(10);
      const {rewindDate: rewindDate5} = genBlockTimestamp(5);

      await expect(multiChainRewindService1.acquireGlobalRewindLock(rewindDate5)).resolves.toBe(true);
      let res = await sequelize.query(lockInfoSql, {type: QueryTypes.SELECT});
      expect(res).toEqual(
        expect.arrayContaining([
          {chainId: chainId1, rewindTimestamp: rewindDate5, status: MultiChainRewindStatus.Incomplete},
          {chainId: chainId2, rewindTimestamp: rewindDate5, status: MultiChainRewindStatus.Incomplete},
        ])
      );

      await expect(multiChainRewindService1.acquireGlobalRewindLock(rewindDate10)).resolves.toBe(false);
      res = await sequelize.query(lockInfoSql, {type: QueryTypes.SELECT});
      expect(res).toEqual(
        expect.arrayContaining([
          {chainId: chainId1, rewindTimestamp: rewindDate5, status: MultiChainRewindStatus.Incomplete},
          {chainId: chainId2, rewindTimestamp: rewindDate5, status: MultiChainRewindStatus.Incomplete},
        ])
      );
    });

    it('Only one can successfully acquire the lock.', async () => {
      const {rewindDate: rewindDate1} = genBlockTimestamp(5);
      const {rewindDate: rewindDate2} = genBlockTimestamp(5);

      const results = await Promise.allSettled([
        multiChainRewindService1.acquireGlobalRewindLock(rewindDate1),
        multiChainRewindService2.acquireGlobalRewindLock(rewindDate2),
      ]);

      const success = results.filter((result) => result.status === 'fulfilled');
      const failures = results.filter((result) => result.status === 'rejected');

      expect(success.length).toBe(1);
      expect(failures.length).toBe(1);
    });
  });

  describe('releaseChainRewindLock', () => {
    const {rewindDate} = genBlockTimestamp(5);
    beforeEach(async () => {
      await multiChainRewindService1.acquireGlobalRewindLock(rewindDate);
    });
    it('Same height as the target, release lock', async () => {
      const tx = await sequelize1.transaction();
      const remaining = await multiChainRewindService1.releaseChainRewindLock(tx, rewindDate);
      await tx.commit();

      expect(remaining).toBe(1);
    });

    it('Different height from the target, release failed.', async () => {
      const tx = await sequelize1.transaction();
      await expect(multiChainRewindService1.releaseChainRewindLock(tx, new Date())).rejects.toThrow();
      await tx.rollback();
    });

    it('The height of rewindLock is greater than or equal to lastProcessHeight, it can be forcibly released.', async () => {
      const {rewindDate: allowLastDate} = genBlockTimestamp(5);

      const tx = await sequelize1.transaction();
      const remaining = await multiChainRewindService1.releaseChainRewindLock(tx, rewindDate, allowLastDate);
      await tx.commit();
      expect(remaining).toBe(1);
    });

    it('rewindLock is less than lastProcessHeight, it cannot be forcibly released.', async () => {
      const {rewindDate: allowLastDate} = genBlockTimestamp(6);
      const tx = await sequelize1.transaction();
      await expect(multiChainRewindService1.releaseChainRewindLock(tx, rewindDate, allowLastDate)).rejects.toThrow();
      await tx.rollback();
    });
  });

  describe('The situation where notifyHandle controls the state', () => {
    it('A chain rollback has been completed.', async () => {
      const {rewindDate} = genBlockTimestamp(5);
      await multiChainRewindService1.acquireGlobalRewindLock(rewindDate);
      await delay(notifyHandleDelay);
      expect(multiChainRewindService1.status).toBe(MultiChainRewindStatus.Incomplete);

      const tx = await sequelize1.transaction();
      const remaining = await multiChainRewindService1.releaseChainRewindLock(tx, rewindDate);
      await tx.commit();
      expect(multiChainRewindService1.status).toBe(MultiChainRewindStatus.Complete);

      await delay(notifyHandleDelay);
      expect(remaining).toBe(1);
      expect(multiChainRewindService2.status).toEqual(MultiChainRewindStatus.Incomplete);
      expect(multiChainRewindService2.waitRewindHeader).toEqual({
        blockHash: 'hash5',
        blockHeight: 5,
        parentHash: 'hash4',
        timestamp: rewindDate,
      });
    });

    it('should handle multiple concurrent rewind requests', async () => {
      const {rewindDate: rewindDate1} = genBlockTimestamp(5);
      const {rewindDate: rewindDate2} = genBlockTimestamp(3); // Earlier timestamp

      await multiChainRewindService1.acquireGlobalRewindLock(rewindDate1);
      await delay(notifyHandleDelay);
      expect(multiChainRewindService1.status).toBe(MultiChainRewindStatus.Incomplete);

      await multiChainRewindService2.acquireGlobalRewindLock(rewindDate2);
      await delay(notifyHandleDelay);
      expect(multiChainRewindService2.status).toBe(MultiChainRewindStatus.Incomplete);

      // The rewindTimestamp of the later chain will overwrite that of the earlier chain.
      const res = await sequelize.query(lockInfoSql, {type: QueryTypes.SELECT});
      expect(res).toEqual([
        {chainId: chainId1, rewindTimestamp: rewindDate2, status: MultiChainRewindStatus.Incomplete},
        {chainId: chainId2, rewindTimestamp: rewindDate2, status: MultiChainRewindStatus.Incomplete},
      ]);

      // Rollback to rewindDate1 is not allowed because it has already been overwritten.
      let remaining = 2;
      let tx = await sequelize1.transaction();
      await expect(multiChainRewindService1.releaseChainRewindLock(tx, rewindDate1)).rejects.toThrow();
      await tx.rollback();
      expect(multiChainRewindService1.status).toBe(MultiChainRewindStatus.Incomplete);

      // Rollback to rewindDate2 is allowed.
      tx = await sequelize1.transaction();
      remaining = await multiChainRewindService1.releaseChainRewindLock(tx, rewindDate2);
      await tx.commit();
      expect(remaining).toBe(1);
      expect(multiChainRewindService1.status).toBe(MultiChainRewindStatus.Complete);

      // Chain2 has started rolling back.
      tx = await sequelize2.transaction();
      remaining = await multiChainRewindService2.releaseChainRewindLock(tx, rewindDate2);
      await tx.commit();
      // This can fail because the notification has already come in and the status is back to normal
      expect(multiChainRewindService2.status).toBe(MultiChainRewindStatus.Complete);

      await delay(notifyHandleDelay);
      // The last chain rollback is complete, all chains have finished rolling back.
      expect(remaining).toBe(0);
      expect(multiChainRewindService1.status).toBe(MultiChainRewindStatus.Normal);
      expect(multiChainRewindService2.status).toBe(MultiChainRewindStatus.Normal);
    });

    it('should handle binary search edge cases for timestamp matching', async () => {
      // Mock the binary search to simulate a large gap between blocks
      const mockGetHeaderByBinarySearch = jest.spyOn(multiChainRewindService1 as any, 'getHeaderByBinarySearch');
      mockGetHeaderByBinarySearch.mockResolvedValueOnce({
        blockHeight: 4,
        timestamp: genBlockTimestamp(4).rewindDate,
        blockHash: 'hash4',
        parentHash: 'hash3',
      });

      const {rewindDate} = genBlockTimestamp(4.5); // Timestamp between blocks 4 and 5
      await multiChainRewindService1.acquireGlobalRewindLock(rewindDate);
      await delay(notifyHandleDelay);
      expect(multiChainRewindService1.status).toBe(MultiChainRewindStatus.Incomplete);
      expect(mockGetHeaderByBinarySearch).toHaveBeenCalledWith(expect.any(Date));
      expect(multiChainRewindService1.waitRewindHeader).toEqual({
        blockHeight: 4,
        timestamp: genBlockTimestamp(4.5).rewindDate,
        blockHash: 'hash4',
        parentHash: 'hash3',
      });

      const tx = await sequelize1.transaction();
      const remaining = await multiChainRewindService1.releaseChainRewindLock(tx, rewindDate);
      await tx.commit();
      expect(multiChainRewindService1.status).toBe(MultiChainRewindStatus.Complete);
      expect(remaining).toBe(1);
      expect(multiChainRewindService1.waitRewindHeader).toBeUndefined();
    });
  });

  describe('getHeaderByBinarySearch', () => {
    beforeEach(async () => {
      (multiChainRewindService1 as any).startHeight = 20;
      await storeService1.modelProvider.metadata.set('lastProcessedHeight', 10000);
    });

    it('Within the already processed interval', async () => {
      const {rewindDate} = genBlockTimestamp(23);
      const header = await (multiChainRewindService1 as any).getHeaderByBinarySearch(rewindDate);

      expect(header).toEqual({
        blockHeight: 23,
        timestamp: rewindDate,
        blockHash: 'hash23',
        parentHash: 'hash22',
      });
    });

    it('Not within the already processed interval', async () => {
      const {rewindDate: rewindDate19} = genBlockTimestamp(19);
      const {rewindDate: rewindDate20} = genBlockTimestamp(20);
      const header = await (multiChainRewindService1 as any).getHeaderByBinarySearch(rewindDate19);
      expect(header).toEqual({
        blockHeight: 20,
        timestamp: rewindDate20,
        blockHash: 'hash20',
        parentHash: 'hash19',
      });

      const {rewindDate: rewindDate10001} = genBlockTimestamp(10001);
      const {rewindDate: rewindDate10000} = genBlockTimestamp(10000);
      const header10001 = await (multiChainRewindService1 as any).getHeaderByBinarySearch(rewindDate10001);
      expect(header10001).toEqual({
        blockHeight: 10000,
        timestamp: rewindDate10000,
        blockHash: 'hash10000',
        parentHash: 'hash9999',
      });
    });
  });

  describe('Lock enrollment', () => {
    it('registers the chain for the lock on init', async () => {
      const res = await sequelize.query<{value: boolean}>(
        `SELECT "value" FROM "${testSchemaName}"."${storeService1.modelProvider.metadata.model.tableName}" WHERE "key" = 'multiChainRewindLock';`,
        {type: QueryTypes.SELECT}
      );
      expect(res).toEqual([{value: true}]);
    });

    it('does not enroll a chain that has not registered for the lock', async () => {
      // A node on an image without the lock never writes the key, one with the lock disabled writes false
      await storeService2.modelProvider.metadata.model.upsert({key: 'multiChainRewindLock', value: false});

      const {rewindDate} = genBlockTimestamp(5);
      await expect(multiChainRewindService1.acquireGlobalRewindLock(rewindDate)).resolves.toBe(true);
      const res = await sequelize.query(lockInfoSql, {type: QueryTypes.SELECT});
      expect(res).toEqual([
        {chainId: chainId1, rewindTimestamp: rewindDate, status: MultiChainRewindStatus.Incomplete},
      ]);

      // Nothing else to wait for, so releasing fully clears the lock
      const tx = await sequelize1.transaction();
      await expect(multiChainRewindService1.releaseChainRewindLock(tx, rewindDate)).resolves.toBe(0);
      await tx.commit();
      await delay(notifyHandleDelay);
      expect(multiChainRewindService1.status).toBe(MultiChainRewindStatus.Normal);
      await expect(sequelize.query(lockInfoSql, {type: QueryTypes.SELECT})).resolves.toEqual([]);
    });
  });

  describe('reconcile', () => {
    // Detach the notification handler so only the table read can change the status
    const muteNotifications = (service: MultiChainRewindService) =>
      (service as any).pgListener.removeAllListeners('notification');

    it('picks up a rewind started while notifications were not received', async () => {
      muteNotifications(multiChainRewindService2);
      const {rewindDate} = genBlockTimestamp(5);
      await multiChainRewindService1.acquireGlobalRewindLock(rewindDate);
      await delay(notifyHandleDelay);
      expect(multiChainRewindService2.status).toBe(MultiChainRewindStatus.Normal);

      await multiChainRewindService2.reconcile();
      expect(multiChainRewindService2.status).toBe(MultiChainRewindStatus.Incomplete);
      expect(multiChainRewindService2.waitRewindHeader).toEqual({
        blockHash: 'hash5',
        blockHeight: 5,
        parentHash: 'hash4',
        timestamp: rewindDate,
      });
      expect(multiChainRewindService2.waitingFor).toEqual([chainId1, chainId2]);
    });

    it('returns to normal when the lock was released without a notification', async () => {
      muteNotifications(multiChainRewindService1);
      const {rewindDate} = genBlockTimestamp(5);
      await multiChainRewindService1.acquireGlobalRewindLock(rewindDate);
      let tx = await sequelize1.transaction();
      await multiChainRewindService1.releaseChainRewindLock(tx, rewindDate);
      await tx.commit();
      expect(multiChainRewindService1.status).toBe(MultiChainRewindStatus.Complete);

      await multiChainRewindService1.reconcile();
      expect(multiChainRewindService1.waitingFor).toEqual([chainId2]);

      tx = await sequelize2.transaction();
      await multiChainRewindService2.releaseChainRewindLock(tx, rewindDate);
      await tx.commit();
      await delay(notifyHandleDelay);
      expect(multiChainRewindService1.status).toBe(MultiChainRewindStatus.Complete);

      await multiChainRewindService1.reconcile();
      expect(multiChainRewindService1.status).toBe(MultiChainRewindStatus.Normal);
      expect(multiChainRewindService1.waitingFor).toEqual([]);
    });

    it('polls the lock table on an interval', async () => {
      muteNotifications(multiChainRewindService2);
      clearInterval((multiChainRewindService2 as any).pollTimer);
      (multiChainRewindService2 as any).pollTimer = setInterval(
        () => void multiChainRewindService2.reconcile('poll'),
        100
      );
      const {rewindDate} = genBlockTimestamp(5);
      await multiChainRewindService1.acquireGlobalRewindLock(rewindDate);
      await delay(notifyHandleDelay);
      expect(multiChainRewindService2.status).toBe(MultiChainRewindStatus.Incomplete);
    });

    it('does not search for a header again when the rewind timestamp is unchanged', async () => {
      const search = jest.spyOn(multiChainRewindService2 as any, 'getHeaderByBinarySearch');
      const {rewindDate} = genBlockTimestamp(5);
      await multiChainRewindService1.acquireGlobalRewindLock(rewindDate);
      await delay(notifyHandleDelay);
      expect(multiChainRewindService2.status).toBe(MultiChainRewindStatus.Incomplete);
      expect(search).toHaveBeenCalledTimes(1);

      await multiChainRewindService2.reconcile();
      expect(search).toHaveBeenCalledTimes(1);
    });

    it('keeps a completed status while its own release is not yet committed', async () => {
      const {rewindDate} = genBlockTimestamp(5);
      await multiChainRewindService1.acquireGlobalRewindLock(rewindDate);
      await delay(notifyHandleDelay);
      expect(multiChainRewindService1.status).toBe(MultiChainRewindStatus.Incomplete);

      const tx = await sequelize1.transaction();
      await multiChainRewindService1.releaseChainRewindLock(tx, rewindDate);
      // The row still reads incomplete outside the transaction
      await multiChainRewindService1.reconcile();
      expect(multiChainRewindService1.status).toBe(MultiChainRewindStatus.Complete);
      expect(multiChainRewindService1.waitRewindHeader).toBeUndefined();
      await tx.commit();

      // An earlier lock taken by another chain is not mistaken for the uncommitted release
      const {rewindDate: earlierDate} = genBlockTimestamp(3);
      muteNotifications(multiChainRewindService1);
      await multiChainRewindService2.acquireGlobalRewindLock(earlierDate);
      await multiChainRewindService1.reconcile();
      expect(multiChainRewindService1.status).toBe(MultiChainRewindStatus.Incomplete);
      expect(multiChainRewindService1.waitRewindHeader?.timestamp).toEqual(earlierDate);
    });

    it('keeps a pending rewind when the lock table was cleared by hand', async () => {
      const {rewindDate} = genBlockTimestamp(5);
      await multiChainRewindService1.acquireGlobalRewindLock(rewindDate);
      await delay(notifyHandleDelay);
      expect(multiChainRewindService2.status).toBe(MultiChainRewindStatus.Incomplete);

      muteNotifications(multiChainRewindService2);
      await sequelize.query(`DELETE FROM "${testSchemaName}"."_global";`);
      await multiChainRewindService2.reconcile();
      expect(multiChainRewindService2.status).toBe(MultiChainRewindStatus.Incomplete);
      expect(multiChainRewindService2.waitRewindHeader?.blockHeight).toBe(5);
    });

    it('picks up a new lock at the timestamp it already completed', async () => {
      const {rewindDate} = genBlockTimestamp(5);
      await multiChainRewindService1.acquireGlobalRewindLock(rewindDate);
      await delay(notifyHandleDelay);
      const tx = await sequelize1.transaction();
      await multiChainRewindService1.releaseChainRewindLock(tx, rewindDate);
      await tx.commit();
      await delay(notifyHandleDelay);
      expect(multiChainRewindService1.status).toBe(MultiChainRewindStatus.Complete);

      // The lock table is cleared by hand and the chain still pending takes the lock again at the same timestamp
      muteNotifications(multiChainRewindService1);
      await sequelize.query(`DELETE FROM "${testSchemaName}"."_global";`);
      await multiChainRewindService2.acquireGlobalRewindLock(rewindDate);
      await multiChainRewindService1.reconcile();
      expect(multiChainRewindService1.status).toBe(MultiChainRewindStatus.Incomplete);
      expect(multiChainRewindService1.waitRewindHeader?.timestamp).toEqual(rewindDate);
    });

    it('drops the previous target when the search for an earlier one fails', async () => {
      const {rewindDate} = genBlockTimestamp(5);
      await multiChainRewindService1.acquireGlobalRewindLock(rewindDate);
      await delay(notifyHandleDelay);
      expect(multiChainRewindService2.waitRewindHeader?.blockHeight).toBe(5);

      muteNotifications(multiChainRewindService1);
      muteNotifications(multiChainRewindService2);
      const {rewindDate: earlierDate} = genBlockTimestamp(3);
      await multiChainRewindService1.acquireGlobalRewindLock(earlierDate);
      mockBlockchainService.getHeaderForHeight.mockImplementationOnce(() => {
        throw new Error('rpc unavailable');
      });
      await multiChainRewindService2.reconcile();
      expect(multiChainRewindService2.status).toBe(MultiChainRewindStatus.Incomplete);
      expect(multiChainRewindService2.waitRewindHeader).toBeUndefined();

      await multiChainRewindService2.reconcile();
      expect(multiChainRewindService2.waitRewindHeader?.blockHeight).toBe(3);
    });

    it('a failed header search does not block later notifications', async () => {
      mockBlockchainService.getHeaderForHeight.mockImplementationOnce(() => {
        throw new Error('rpc unavailable');
      });
      const {rewindDate} = genBlockTimestamp(5);
      await multiChainRewindService1.acquireGlobalRewindLock(rewindDate);
      await delay(notifyHandleDelay);
      const failed = [multiChainRewindService1, multiChainRewindService2].filter(
        (service) => service.waitRewindHeader === undefined
      );
      expect(failed).toHaveLength(1);

      // The next reconcile retries the search
      await failed[0].reconcile();
      expect(failed[0].waitRewindHeader?.blockHeight).toBe(5);

      let tx = await sequelize1.transaction();
      await multiChainRewindService1.releaseChainRewindLock(tx, rewindDate);
      await tx.commit();
      tx = await sequelize2.transaction();
      await multiChainRewindService2.releaseChainRewindLock(tx, rewindDate);
      await tx.commit();
      await delay(notifyHandleDelay);
      expect(multiChainRewindService1.status).toBe(MultiChainRewindStatus.Normal);
      expect(multiChainRewindService2.status).toBe(MultiChainRewindStatus.Normal);
    });
  });

  describe('listener recovery', () => {
    const listenerOf = (service: MultiChainRewindService) => (service as any).pgListener;

    it('re-registers the listener after the server terminates the connection', async () => {
      const lostListener = listenerOf(multiChainRewindService2);
      const [{pid}] = (await lostListener.query('SELECT pg_backend_pid() AS pid')).rows;
      await sequelize.query(`SELECT pg_terminate_backend(${pid});`);
      await delay(1);
      expect(listenerOf(multiChainRewindService2)).toBeDefined();
      expect(listenerOf(multiChainRewindService2)).not.toBe(lostListener);

      const {rewindDate} = genBlockTimestamp(5);
      await multiChainRewindService1.acquireGlobalRewindLock(rewindDate);
      await delay(notifyHandleDelay);
      expect(multiChainRewindService2.status).toBe(MultiChainRewindStatus.Incomplete);
    });

    it('replaces a listener whose heartbeat hangs', async () => {
      const lostListener = listenerOf(multiChainRewindService2);
      jest.spyOn(lostListener, 'query').mockImplementation(() => new Promise(() => undefined));
      multiChainRewindService2.heartbeatTimeoutSec = 0.2;
      await (multiChainRewindService2 as any).heartbeat();
      expect(listenerOf(multiChainRewindService2)).toBeDefined();
      expect(listenerOf(multiChainRewindService2)).not.toBe(lostListener);

      const {rewindDate} = genBlockTimestamp(5);
      await multiChainRewindService1.acquireGlobalRewindLock(rewindDate);
      await delay(notifyHandleDelay);
      expect(multiChainRewindService2.status).toBe(MultiChainRewindStatus.Incomplete);
    });

    it('does not keep a new connection whose LISTEN failed', async () => {
      const connectionManager = sequelize2.connectionManager as any;
      const getConnection = connectionManager.getConnection.bind(connectionManager);
      let failedListener: any;
      jest.spyOn(connectionManager, 'getConnection').mockImplementationOnce(async (options: any) => {
        failedListener = await getConnection(options);
        jest.spyOn(failedListener, 'query').mockImplementationOnce(() => Promise.reject(new Error('listen failed')));
        return failedListener;
      });

      const lostListener = listenerOf(multiChainRewindService2);
      jest.spyOn(lostListener, 'query').mockImplementation(() => new Promise(() => undefined));
      multiChainRewindService2.heartbeatTimeoutSec = 0.2;
      await (multiChainRewindService2 as any).heartbeat();
      expect(failedListener).toBeDefined();
      expect(listenerOf(multiChainRewindService2)).toBeDefined();
      expect(listenerOf(multiChainRewindService2)).not.toBe(lostListener);
      expect(listenerOf(multiChainRewindService2)).not.toBe(failedListener);

      // Only a notification can change the status here, the poll interval is far longer than the test
      const {rewindDate} = genBlockTimestamp(5);
      await multiChainRewindService1.acquireGlobalRewindLock(rewindDate);
      await delay(notifyHandleDelay);
      expect(multiChainRewindService2.status).toBe(MultiChainRewindStatus.Incomplete);
    });
  });

  describe('Project initialization', () => {
    const reindex = jest.fn();
    let multiChainRewindService: MultiChainRewindService;

    beforeEach(async () => {
      const nodeConfig = new NodeConfig({
        subquery: 'test',
        dbSchema: testSchemaName,
        proofOfIndex: true,
        enableCache: false,
        multiChain: true,
      });
      const project = {network: {chainId: chainId1}, schema} as any;
      const dbModel = new PlainStoreModelService(sequelize, nodeConfig);
      const storeService = new StoreService(sequelize, nodeConfig, dbModel, project);
      await storeService.initCoreTables(testSchemaName);
      const tx = await sequelize.transaction();
      await storeService.init(testSchemaName, tx);
      await tx.commit();

      multiChainRewindService = new MultiChainRewindService(
        nodeConfig,
        sequelize,
        storeService,
        mockBlockchainService as any
      );
    });
    afterEach(async () => {
      await multiChainRewindService.onApplicationShutdown();
      jest.clearAllMocks();
    });

    it('Normal startup, starting will not trigger a reindex.', async () => {
      // Initialize the service
      await multiChainRewindService.init(chainId1, reindex);

      expect(reindex).toHaveBeenCalledTimes(0);
    });

    it('After another chain undergoes a rewind, the current chain starts, which can trigger a reindex.', async () => {
      const {rewindDate} = genBlockTimestamp(5);
      await multiChainRewindService1.acquireGlobalRewindLock(rewindDate);

      // Initialize the service
      await multiChainRewindService.init(chainId1, reindex);

      expect(reindex).toHaveBeenCalledTimes(1);
      expect(multiChainRewindService.status).toBe(MultiChainRewindStatus.Incomplete);
      expect(multiChainRewindService.waitRewindHeader).toEqual({
        blockHeight: 5,
        timestamp: rewindDate,
        blockHash: 'hash5',
        parentHash: 'hash4',
      });
    });

    it('The current chain has already completed the rewind, and there are still other chains that need to rewind. In this case, starting will not trigger a reindex.', async () => {
      const {rewindDate} = genBlockTimestamp(5);
      await multiChainRewindService1.acquireGlobalRewindLock(rewindDate);
      const tx = await sequelize1.transaction();
      await multiChainRewindService1.releaseChainRewindLock(tx, rewindDate);
      await tx.commit();

      // Initialize the service
      await multiChainRewindService.init(chainId1, reindex);

      expect(multiChainRewindService.status).toBe(MultiChainRewindStatus.Complete);
      expect(reindex).toHaveBeenCalledTimes(0);
    });
  });
});
