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

// Runs the startup schema sync (StoreService.init) for a given SDL, the same call the
// node makes on boot. A fresh StoreService each time mirrors a process restart.
async function initSchema(schemaName: string, sdl: string, sequelize: Sequelize, config: NodeConfig): Promise<void> {
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
}

async function columns(sequelize: Sequelize, schemaName: string, table: string) {
  return sequelize.query<{column_name: string; is_nullable: string}>(
    `SELECT column_name, is_nullable FROM information_schema.columns WHERE table_schema = :schema AND table_name = :table;`,
    {type: QueryTypes.SELECT, replacements: {schema: schemaName, table}}
  );
}

jest.setTimeout(900000);
describe('In-place schema migration on startup', () => {
  let sequelize: Sequelize;
  let schemaName: string;
  // historical:false mirrors the substrate node, which runs without --historical
  const migrationConfig = new NodeConfig({allowSchemaMigration: true, historical: false} as any);

  beforeEach(async () => {
    sequelize = new Sequelize(
      `postgresql://${option.username}:${option.password}@${option.host}:${option.port}/${option.database}`,
      {...option, logging: false}
    );
    await sequelize.authenticate();
    schemaName = `test_inplace_${Date.now()}`;
    await sequelize.createSchema(`"${schemaName}"`, {});
  });

  afterEach(async () => {
    delete process.env.SUBQL_ALLOW_DESTRUCTIVE_MIGRATION;
    await sequelize.dropSchema(schemaName, {logging: false});
    await sequelize?.close();
  });

  it('adds a nullable field to an existing entity in place and preserves rows', async () => {
    await initSchema(schemaName, SDL_V1, sequelize, migrationConfig);

    // The applied schema is recorded so the next boot has a baseline to diff against
    const [seeded] = await sequelize.query<{value: string}>(
      `SELECT value FROM "${schemaName}"._metadata WHERE key = 'appliedSchemaSDL';`,
      {type: QueryTypes.SELECT}
    );
    expect(seeded.value).toEqual(SDL_V1);

    // Seed data under the v1 schema
    await sequelize.query(`INSERT INTO "${schemaName}"."accounts" (id, balance) VALUES ('acc-1', 100);`);

    // Restart with the additive schema
    await initSchema(schemaName, SDL_V2, sequelize, migrationConfig);

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
  });

  it('refuses a destructive change by default and leaves the column intact', async () => {
    await initSchema(schemaName, SDL_V1, sequelize, migrationConfig);
    await sequelize.query(`INSERT INTO "${schemaName}"."accounts" (id, balance) VALUES ('acc-1', 100);`);

    const exitSpy = jest.spyOn(process, 'exit').mockImplementation(((code?: number) => {
      throw new Error(`process.exit:${code}`);
    }) as any);

    await expect(initSchema(schemaName, SDL_V3, sequelize, migrationConfig)).rejects.toThrow('process.exit:1');

    // The dropped-in-schema column and its data are still there
    const cols = await columns(sequelize, schemaName, 'accounts');
    expect(cols.find((c) => c.column_name === 'balance')).toBeDefined();
    const rows = await sequelize.query(`SELECT balance FROM "${schemaName}"."accounts";`, {type: QueryTypes.SELECT});
    expect(rows).toHaveLength(1);

    exitSpy.mockRestore();
  });

  it('applies a destructive change when explicitly opted in', async () => {
    await initSchema(schemaName, SDL_V1, sequelize, migrationConfig);
    process.env.SUBQL_ALLOW_DESTRUCTIVE_MIGRATION = 'true';

    await initSchema(schemaName, SDL_V3, sequelize, migrationConfig);

    const cols = await columns(sequelize, schemaName, 'accounts');
    expect(cols.find((c) => c.column_name === 'balance')).toBeUndefined();
    expect(cols.find((c) => c.column_name === 'nickname')).toBeDefined();
  });

  it('does not migrate in place when the feature is disabled', async () => {
    const plainConfig = new NodeConfig({allowSchemaMigration: false, historical: false} as any);
    await initSchema(schemaName, SDL_V1, sequelize, plainConfig);

    // No baseline is recorded, so a later boot cannot diff and add the column
    const seeded = await sequelize.query(
      `SELECT value FROM "${schemaName}"._metadata WHERE key = 'appliedSchemaSDL';`,
      {type: QueryTypes.SELECT}
    );
    expect(seeded).toHaveLength(0);

    await initSchema(schemaName, SDL_V2, sequelize, plainConfig);
    const cols = await columns(sequelize, schemaName, 'accounts');
    expect(cols.find((c) => c.column_name === 'nickname')).toBeUndefined();
  });

  // The multichain substrate node forces historical=timestamp, so the tables carry a _block_range.
  // This is the mode the real deployment runs in, verified live; keep it covered as a regression.
  it('adds a field in place under historical indexing and preserves rows', async () => {
    const historicalConfig = new NodeConfig({allowSchemaMigration: true, historical: 'timestamp'} as any);
    await initSchema(schemaName, SDL_V1, sequelize, historicalConfig);

    await sequelize.query(
      `INSERT INTO "${schemaName}"."accounts" (id, balance, _id, _block_range) VALUES ('acc-1', 100, gen_random_uuid(), int8range(1, NULL));`
    );

    await initSchema(schemaName, SDL_V2, sequelize, historicalConfig);

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
  });
});
