import { StartedTestContainer } from 'testcontainers';
import { CubeStoreDBRunner, PostgresDBRunner } from '@cubejs-backend/testing-shared';
import { pausePromise } from '@cubejs-backend/shared';
import cubejs, { Query } from '@cubejs-client/core';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, describe, expect, jest, test } from '@jest/globals';
import { sign } from 'jsonwebtoken';
import { Client as PgClient } from 'pg';
import { BirdBox, getBirdbox } from '../src';
import {
  DEFAULT_CONFIG,
  JEST_AFTER_ALL_DEFAULT_TIMEOUT,
  JEST_BEFORE_ALL_DEFAULT_TIMEOUT,
  stopIfStarted,
} from './smoke-tests';

const CubeStoreDriver = require('@cubejs-backend/cubestore-driver');

type CubeStoreId = 'blue' | 'green';

// Mirrors the data model: blue serves even days since the epoch, green odd ones.
function cubeStoreFor(businessDate: string): CubeStoreId {
  return (Date.parse(`${businessDate}T00:00:00Z`) / 86_400_000) % 2 === 0 ? 'blue' : 'green';
}

// Three consecutive business days across a month end, so the clusters alternate
// over it and the monthly partitions of `orders.daily` hold more than one month.
const DAY_1 = '2026-10-31';
const DAY_2 = '2026-11-01';
const DAY_3 = '2026-11-02';

const ORDERS: Record<string, { status: string, amount: number }[]> = {
  [DAY_1]: [
    { status: 'new', amount: 10 },
    { status: 'shipped', amount: 20 },
    { status: 'shipped', amount: 30 },
  ],
  [DAY_2]: [
    { status: 'new', amount: 100 },
    { status: 'shipped', amount: 200 },
  ],
  [DAY_3]: [
    { status: 'new', amount: 1000 },
    { status: 'new', amount: 2000 },
    { status: 'shipped', amount: 3000 },
    { status: 'shipped', amount: 4000 },
  ],
};

const TOTALS_QUERY: Query = {
  measures: ['orders.count', 'orders.total_amount', 'orders.last_business_date'],
};

const DAILY_QUERY: Query = {
  measures: ['orders.count', 'orders.total_amount'],
  timeDimensions: [{ dimension: 'orders.business_date', granularity: 'day' }],
  order: { 'orders.business_date': 'asc' },
};

describe('blue/green pre-aggregations', () => {
  jest.setTimeout(60 * 5 * 1000);

  let db: StartedTestContainer;
  let cubeStores: Record<CubeStoreId, StartedTestContainer>;
  let birdbox: BirdBox;
  let pg: PgClient;

  const token = (cubeStoreId: string) => sign(
    { cube_store_id: cubeStoreId },
    DEFAULT_CONFIG.CUBEJS_API_SECRET,
    { expiresIn: '2 days' },
  );

  const client = (cubeStoreId: string) => cubejs(
    async () => token(cubeStoreId),
    { apiUrl: birdbox.configuration.apiUrl },
  );

  async function load(cubeStoreId: CubeStoreId, query: Query) {
    const resultSet = await client(cubeStoreId).load(query);
    // @ts-ignore loadResponse is not part of the public type
    const { usedPreAggregations } = resultSet.loadResponse.results[0];

    return {
      rows: resultSet.tablePivot(),
      tables: Object.values(usedPreAggregations).map((p: any) => p.targetTableName).sort(),
    };
  }

  async function totals(cubeStoreId: CubeStoreId) {
    const { rows: [row], tables } = await load(cubeStoreId, TOTALS_QUERY);

    return {
      count: Number(row['orders.count']),
      totalAmount: Number(row['orders.total_amount']),
      lastBusinessDate: String(row['orders.last_business_date']).slice(0, 10),
      tables,
    };
  }

  const expectedTotals = (...days: string[]) => {
    const orders = days.flatMap((day) => ORDERS[day]);

    return {
      count: orders.length,
      totalAmount: orders.reduce((sum, { amount }) => sum + amount, 0),
      lastBusinessDate: days[days.length - 1],
    };
  };

  async function snapshot(cubeStoreId: CubeStoreId) {
    const { tables: dailyTables, rows } = await load(cubeStoreId, DAILY_QUERY);
    const { tables: totalsTables, ...total } = await totals(cubeStoreId);

    return {
      total,
      daily: rows.map((row) => ({
        day: String(row['orders.business_date.day']).slice(0, 10),
        count: Number(row['orders.count']),
        totalAmount: Number(row['orders.total_amount']),
      })),
      tables: { totals: totalsTables, daily: dailyTables },
    };
  }

  const expectedSnapshot = (days: string[]) => ({
    total: expectedTotals(...days),
    daily: days.map((day) => {
      const { count, totalAmount } = expectedTotals(day);
      return { day, count, totalAmount };
    }),
  });

  // What the application does before moving users to a cluster: wait until
  // every pre-aggregation of it serves the day the ETL has just finished.
  async function waitForSnapshot(cubeStoreId: CubeStoreId, days: string[]) {
    const expected = expectedSnapshot(days);
    let last: unknown;

    for (let attempt = 0; attempt < 180; attempt++) {
      try {
        const current = await snapshot(cubeStoreId);

        last = current;
        if (JSON.stringify({ total: current.total, daily: current.daily }) === JSON.stringify(expected)) {
          return current;
        }
      } catch (e) {
        // Rollup-only queries fail until the cluster has built its first version.
        last = e;
      }

      await pausePromise(500);
    }

    throw new Error(
      `${cubeStoreId} did not reach ${JSON.stringify(expected)}, last seen: ${
        last instanceof Error ? last.message : JSON.stringify(last)
      }`
    );
  }

  // The ETL loads a day's orders first and marks the day finished last.
  async function startEtl(businessDate: string) {
    await pg.query('INSERT INTO etl_runs (business_date) VALUES ($1)', [businessDate]);

    for (const { status, amount } of ORDERS[businessDate]) {
      await pg.query(
        'INSERT INTO orders (business_date, status, amount) VALUES ($1, $2, $3)',
        [businessDate, status, amount],
      );
    }
  }

  async function finishEtl(businessDate: string) {
    await pg.query('UPDATE etl_runs SET finished_at = now() WHERE business_date = $1', [businessDate]);
  }

  beforeAll(async () => {
    db = await PostgresDBRunner.startContainer({});
    cubeStores = {
      blue: await CubeStoreDBRunner.startContainer({}),
      green: await CubeStoreDBRunner.startContainer({}),
    };

    pg = new PgClient({
      host: db.getHost(),
      port: db.getMappedPort(5432),
      database: 'test',
      user: 'test',
      password: 'test',
    });
    await pg.connect();
    await pg.query(`
      CREATE TABLE orders (
        id SERIAL PRIMARY KEY,
        business_date DATE NOT NULL,
        status TEXT NOT NULL,
        amount INTEGER NOT NULL
      );
      CREATE TABLE etl_runs (
        business_date DATE PRIMARY KEY,
        started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        finished_at TIMESTAMPTZ
      );
    `);

    birdbox = await getBirdbox(
      'postgres',
      {
        ...DEFAULT_CONFIG,
        CUBEJS_DB_TYPE: 'postgres',
        CUBEJS_DB_HOST: db.getHost(),
        CUBEJS_DB_PORT: `${db.getMappedPort(5432)}`,
        CUBEJS_DB_NAME: 'test',
        CUBEJS_DB_USER: 'test',
        CUBEJS_DB_PASS: 'test',
        CUBESTORE_BLUE_HOST: cubeStores.blue.getHost(),
        CUBESTORE_BLUE_PORT: `${cubeStores.blue.getMappedPort(3030)}`,
        CUBESTORE_GREEN_HOST: cubeStores.green.getHost(),
        CUBESTORE_GREEN_PORT: `${cubeStores.green.getMappedPort(3030)}`,
        // Never fall back to the source database: a query sees only what its
        // cluster has built.
        CUBEJS_ROLLUP_ONLY: 'true',
        // The refresh worker checks the refresh keys of both clusters every 30 seconds.
        CUBEJS_REFRESH_WORKER: 'true',
      },
      {
        schemaDir: 'blue-green/model',
        cubejsConfig: 'blue-green/cube.js',
      }
    );
  }, JEST_BEFORE_ALL_DEFAULT_TIMEOUT);

  afterAll(async () => {
    await stopIfStarted('birdbox', birdbox);
    await stopIfStarted('pg', pg && (() => pg.end()));
    await stopIfStarted('cubestore blue', cubeStores?.blue);
    await stopIfStarted('cubestore green', cubeStores?.green);
    await stopIfStarted('db', db);
  }, JEST_AFTER_ALL_DEFAULT_TIMEOUT);

  // Longer than a refresh worker pass (30s) plus the refresh keys' `every`
  // (10s), so the worker has seen any change of them.
  const REFRESH_KEY_CHECK_MS = 45 * 1000;

  let tablesBefore: Record<CubeStoreId, Awaited<ReturnType<typeof snapshot>>['tables']>;

  test('each cluster serves the last finished ETL day of its parity', async () => {
    expect([DAY_1, DAY_2, DAY_3].map(cubeStoreFor)).toEqual(['green', 'blue', 'green']);

    await startEtl(DAY_1);
    await finishEtl(DAY_1);
    await waitForSnapshot('green', [DAY_1]);

    await startEtl(DAY_2);
    await finishEtl(DAY_2);
    const blue = await waitForSnapshot('blue', [DAY_1, DAY_2]);
    const green = await snapshot('green');

    expect(green).toMatchObject(expectedSnapshot([DAY_1]));
    // Both pre-aggregations of a cluster are separate tables at the same day.
    expect(blue.tables.totals).not.toEqual(blue.tables.daily);

    tablesBefore = { blue: blue.tables, green: green.tables };
  });

  test('a running ETL changes neither cluster', async () => {
    await startEtl(DAY_3);
    await pausePromise(REFRESH_KEY_CHECK_MS);

    const blue = await snapshot('blue');
    const green = await snapshot('green');

    expect(blue).toMatchObject(expectedSnapshot([DAY_1, DAY_2]));
    expect(blue.tables).toEqual(tablesBefore.blue);
    expect(green).toMatchObject(expectedSnapshot([DAY_1]));
    expect(green.tables).toEqual(tablesBefore.green);
  });

  test('a finished ETL rebuilds only the cluster of its parity', async () => {
    expect(cubeStoreFor(DAY_3)).toBe('green');
    await finishEtl(DAY_3);

    // Users switch to green once it serves the new day.
    const green = await waitForSnapshot('green', [DAY_1, DAY_2, DAY_3]);
    expect(green.tables.totals).not.toEqual(tablesBefore.green.totals);
    expect(green.tables.daily).not.toEqual(tablesBefore.green.daily);

    // Blue keeps the previous day untouched until the next even day.
    await pausePromise(REFRESH_KEY_CHECK_MS);
    const blue = await snapshot('blue');
    expect(blue).toMatchObject(expectedSnapshot([DAY_1, DAY_2]));
    expect(blue.tables).toEqual(tablesBefore.blue);
  });

  test('pre-aggregation tables live only in their own Cube Store', async () => {
    const tablesIn = async (cubeStoreId: CubeStoreId) => {
      const driver = new CubeStoreDriver({
        host: cubeStores[cubeStoreId].getHost(),
        port: cubeStores[cubeStoreId].getMappedPort(3030),
      });

      try {
        const rows: { table_name: string }[] = await driver.query(
          'SELECT table_name FROM information_schema.tables WHERE table_schema = \'dev_pre_aggregations\'',
          [],
        );
        return rows.map(({ table_name: name }) => name);
      } finally {
        await driver.release();
      }
    };

    const blue = await tablesIn('blue');
    const green = await tablesIn('green');

    expect(blue.length).toBeGreaterThan(0);
    expect(green.length).toBeGreaterThan(0);
    expect(blue.filter((name) => green.includes(name))).toEqual([]);
  });

  test('an unknown cube_store_id is rejected', async () => {
    await expect(client('red').load(TOTALS_QUERY)).rejects.toThrow(/Unknown cube_store_id: red/);
  });
});
