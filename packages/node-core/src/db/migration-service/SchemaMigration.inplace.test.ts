// Copyright 2020-2025 SubQuery Pte Ltd authors & contributors
// SPDX-License-Identifier: GPL-3.0

import {EventEmitter2} from '@nestjs/event-emitter';
import {buildSchemaFromString} from '@subql/utils';
import {QueryTypes, Sequelize} from '@subql/x-sequelize';
import {NodeConfig} from '../../configure';
import {ISubqueryProject, StoreCacheService, StoreService} from '../../indexer';
import {DbOption} from '../db.module';

const option: DbOption = {
  host: process.env.DB_HOST ?? '127.0.0.1',
  port: process.env.DB_PORT ? Number(process.env.DB_PORT) : 5432,
  username: process.env.DB_USER ?? 'postgres',
  password: process.env.DB_PASS ?? 'postgres',
  database: process.env.DB_DATABASE ?? 'postgres',
  timezone: 'utc',
};

const SDL_V1 = `type Account @entity {
  id: ID!
  balance: BigInt!
}`;

// Additive: a new nullable field on the existing entity
const SDL_V2 = `type Account @entity {
  id: ID!
  balance: BigInt!
  nickname: String
}`;

// Destructive: removes an existing field (drops its column and data)
const SDL_V3 = `type Account @entity {
  id: ID!
  nickname: String
}`;

// Additive: a new entity holding a relation to the unchanged Account entity
const SDL_REL = `type Account @entity {
  id: ID!
  balance: BigInt!
}

type Transfer @entity {
  id: ID!
  account: Account!
}`;

// historical:false mirrors the substrate node in single-chain tests; the historical variant is
// covered separately.
const migrationConfig = new NodeConfig({allowSchemaMigration: true, historical: false} as any);

function newSequelize(): Sequelize {
  return new Sequelize(
    `postgresql://${option.username}:${option.password}@${option.host}:${option.port}/${option.database}`,
    {...option, logging: false}
  );
}

// Boots a node against an existing db-schema with its OWN Sequelize instance, so no model
// definitions leak between boots — a boot only has the models it defines during init, exactly
// like a process restart. Returns the instance so the test can inspect it and close it.
async function bootNode(
  schemaName: string,
  sdl: string,
  config: NodeConfig
): Promise<{sequelize: Sequelize; storeService: StoreService}> {
  const sequelize = newSequelize();
  await sequelize.authenticate();
  const project = {
    schema: buildSchemaFromString(sdl),
    schemaSDL: sdl,
    network: {chainId: 'chainId'},
  } as unknown as ISubqueryProject;

  const storeCache = new StoreCacheService(sequelize, config, new EventEmitter2());
  const storeService = new StoreService(sequelize, config, storeCache, project);

  await storeService.initCoreTables(schemaName);
  const tx = await sequelize.transaction();
  try {
    await storeService.init(schemaName, tx);
    await tx.commit();
  } catch (e) {
    // init() calls exitWithError (process.exit, stubbed to throw in tests) without rolling
    // back; release the transaction so the connection is not held open.
    await tx.rollback().catch(() => undefined);
    throw e;
  }
  return {sequelize, storeService};
}

async function columns(sequelize: Sequelize, schemaName: string, table: string) {
  return sequelize.query<{column_name: string; is_nullable: string}>(
    `SELECT column_name, is_nullable FROM information_schema.columns WHERE table_schema = :schema AND table_name = :table;`,
    {type: QueryTypes.SELECT, replacements: {schema: schemaName, table}}
  );
}

jest.setTimeout(900000);
describe('In-place schema migration on startup', () => {
  // A separate connection for setup, raw inserts, and assertions, independent of any boot.
  let sequelize: Sequelize;
  let schemaName: string;

  beforeEach(async () => {
    sequelize = newSequelize();
    await sequelize.authenticate();
    schemaName = `test_inplace_${Date.now()}`;
    await sequelize.createSchema(`"${schemaName}"`, {});
  });

  afterEach(async () => {
    delete process.env.SUBQL_ALLOW_DESTRUCTIVE_MIGRATION;
    await sequelize.dropSchema(schemaName, {logging: false});
    await sequelize?.close();
  });

  it('adds a nullable field in place, preserves rows, and keeps every model defined', async () => {
    const boot1 = await bootNode(schemaName, SDL_V1, migrationConfig);
    // The applied schema is recorded so the next boot has a baseline to diff against
    const [seeded] = await sequelize.query<{value: string}>(
      `SELECT value FROM "${schemaName}"._metadata WHERE key = 'appliedSchemaSDL';`,
      {type: QueryTypes.SELECT}
    );
    expect(seeded.value).toEqual(SDL_V1);
    await boot1.sequelize.close();

    // Seed data under the v1 schema
    await sequelize.query(`INSERT INTO "${schemaName}"."accounts" (id, balance) VALUES ('acc-1', 100);`);

    // Restart (fresh Sequelize) with the additive schema
    const boot2 = await bootNode(schemaName, SDL_V2, migrationConfig);

    // The partial diff must still leave every model defined on this fresh instance
    expect(() => boot2.sequelize.model('Account')).not.toThrow();

    const cols = await columns(sequelize, schemaName, 'accounts');
    const nickname = cols.find((c) => c.column_name === 'nickname');
    expect(nickname).toBeDefined();
    expect(nickname?.is_nullable).toEqual('YES');

    // The pre-existing row survives, with the new column defaulting to null
    const rows = await sequelize.query<{id: string; balance: string; nickname: string | null}>(
      `SELECT id, balance, nickname FROM "${schemaName}"."accounts";`,
      {type: QueryTypes.SELECT}
    );
    expect(rows).toHaveLength(1);
    expect(rows[0].id).toEqual('acc-1');
    expect(rows[0].balance).toEqual('100');
    expect(rows[0].nickname).toBeNull();

    // The baseline advances to the new schema
    const [updated] = await sequelize.query<{value: string}>(
      `SELECT value FROM "${schemaName}"._metadata WHERE key = 'appliedSchemaSDL';`,
      {type: QueryTypes.SELECT}
    );
    expect(updated.value).toEqual(SDL_V2);
    await boot2.sequelize.close();
  });

  it('keeps every model defined on a restart with no schema change', async () => {
    const boot1 = await bootNode(schemaName, SDL_V1, migrationConfig);
    await boot1.sequelize.close();

    // Second boot, identical schema, fresh Sequelize: the "No Schema changes" path must still
    // register the models or every store access throws "Account has not been defined".
    const boot2 = await bootNode(schemaName, SDL_V1, migrationConfig);
    expect(() => boot2.sequelize.model('Account')).not.toThrow();
    await boot2.sequelize.close();
  });

  it('adds a relation to an unchanged entity without crashing at boot', async () => {
    const boot1 = await bootNode(schemaName, SDL_V1, migrationConfig);
    await boot1.sequelize.close();

    // Adding Transfer with a relation to the unchanged Account: createRelation resolves
    // sequelize.model('Account'), which only exists if unchanged models are defined first.
    const boot2 = await bootNode(schemaName, SDL_REL, migrationConfig);
    expect(() => boot2.sequelize.model('Account')).not.toThrow();
    expect(() => boot2.sequelize.model('Transfer')).not.toThrow();

    // The new table exists and carries the relation's foreign-key column
    const transferCols = await columns(sequelize, schemaName, 'transfers');
    expect(transferCols.length).toBeGreaterThan(0);
    expect(transferCols.find((c) => c.column_name === 'account_id')).toBeDefined();
    await boot2.sequelize.close();
  });

  it('refuses a destructive change by default and leaves the column intact', async () => {
    const boot1 = await bootNode(schemaName, SDL_V1, migrationConfig);
    await boot1.sequelize.close();
    await sequelize.query(`INSERT INTO "${schemaName}"."accounts" (id, balance) VALUES ('acc-1', 100);`);

    const exitSpy = jest.spyOn(process, 'exit').mockImplementation(((code?: number) => {
      throw new Error(`process.exit:${code}`);
    }) as any);

    await expect(bootNode(schemaName, SDL_V3, migrationConfig)).rejects.toThrow('process.exit:1');

    // The dropped-in-schema column and its data are still there
    const cols = await columns(sequelize, schemaName, 'accounts');
    expect(cols.find((c) => c.column_name === 'balance')).toBeDefined();
    const rows = await sequelize.query(`SELECT balance FROM "${schemaName}"."accounts";`, {type: QueryTypes.SELECT});
    expect(rows).toHaveLength(1);

    exitSpy.mockRestore();
  });

  it('applies a destructive change when explicitly opted in', async () => {
    const boot1 = await bootNode(schemaName, SDL_V1, migrationConfig);
    await boot1.sequelize.close();
    process.env.SUBQL_ALLOW_DESTRUCTIVE_MIGRATION = 'true';

    const boot2 = await bootNode(schemaName, SDL_V3, migrationConfig);

    const cols = await columns(sequelize, schemaName, 'accounts');
    expect(cols.find((c) => c.column_name === 'balance')).toBeUndefined();
    expect(cols.find((c) => c.column_name === 'nickname')).toBeDefined();
    await boot2.sequelize.close();
  });

  it('does not migrate in place when the feature is disabled', async () => {
    const plainConfig = new NodeConfig({allowSchemaMigration: false, historical: false} as any);
    const boot1 = await bootNode(schemaName, SDL_V1, plainConfig);
    await boot1.sequelize.close();

    // No baseline is recorded, so a later boot cannot diff and add the column
    const seeded = await sequelize.query(
      `SELECT value FROM "${schemaName}"._metadata WHERE key = 'appliedSchemaSDL';`,
      {type: QueryTypes.SELECT}
    );
    expect(seeded).toHaveLength(0);

    const boot2 = await bootNode(schemaName, SDL_V2, plainConfig);
    const cols = await columns(sequelize, schemaName, 'accounts');
    expect(cols.find((c) => c.column_name === 'nickname')).toBeUndefined();
    await boot2.sequelize.close();
  });

  // The multichain substrate node forces historical=timestamp, so the tables carry a _block_range.
  // This is the mode the real deployment runs in, verified live; keep it covered as a regression.
  it('adds a field in place under historical indexing and preserves rows', async () => {
    const historicalConfig = new NodeConfig({allowSchemaMigration: true, historical: 'timestamp'} as any);
    const boot1 = await bootNode(schemaName, SDL_V1, historicalConfig);
    await boot1.sequelize.close();

    await sequelize.query(
      `INSERT INTO "${schemaName}"."accounts" (id, balance, _id, _block_range) VALUES ('acc-1', 100, gen_random_uuid(), int8range(1, NULL));`
    );

    const boot2 = await bootNode(schemaName, SDL_V2, historicalConfig);
    expect(() => boot2.sequelize.model('Account')).not.toThrow();

    const cols = await columns(sequelize, schemaName, 'accounts');
    const nickname = cols.find((c) => c.column_name === 'nickname');
    expect(nickname).toBeDefined();
    expect(nickname?.is_nullable).toEqual('YES');
    // _block_range is untouched by an additive column migration
    expect(cols.find((c) => c.column_name === '_block_range')).toBeDefined();

    const rows = await sequelize.query<{id: string; nickname: string | null}>(
      `SELECT id, nickname FROM "${schemaName}"."accounts" WHERE id = 'acc-1';`,
      {type: QueryTypes.SELECT}
    );
    expect(rows).toHaveLength(1);
    expect(rows[0].nickname).toBeNull();
    await boot2.sequelize.close();
  });
});
