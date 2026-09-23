/**
 * E2E repros for old pre-aggregation bug reports: a real Cube server (local binary),
 * a real Postgres and a real Cube Store. Every query is run against a cube WITH a
 * pre-aggregation (served from Cube Store) and compared to the correct result.
 *
 * No Docker required. Expects:
 *   - Postgres reachable via CUBEJS_DB_HOST/PORT/NAME/USER/PASS
 *     (default localhost:5432, db `test`, user `root`, password `test`);
 *     the user must be able to CREATE TYPE (superuser) unless postgresql-hll is installed
 *   - Cube Store reachable via CUBEJS_CUBESTORE_HOST/PORT (default localhost:3030)
 *
 *   yarn tsc && yarn jest --runInBand dist/test/cli-postgresql-cubestore-old-bug-repros.test.js
 */
import { afterAll, beforeAll, describe, expect, jest, test } from '@jest/globals';
import fetch from 'node-fetch';
import { Client } from 'pg';

import { BirdBox, startBirdBoxFromCli } from '../src';

const pgConfig = {
  host: process.env.CUBEJS_DB_HOST || 'localhost',
  port: parseInt(process.env.CUBEJS_DB_PORT || '5432', 10),
  database: process.env.CUBEJS_DB_NAME || 'test',
  user: process.env.CUBEJS_DB_USER || 'root',
  password: process.env.CUBEJS_DB_PASS || 'test',
};

/**
 * `count_distinct_approx` on Postgres needs the postgresql-hll extension
 * (hll_hash_any / hll_add_agg returning the `hll` type). If it's not installed,
 * emulate it: a real `hll` base type backed by bytea I/O, and an aggregate that
 * emits a valid postgresql-hll EXPLICIT-format sketch (header 0x128b7f + sorted
 * 8-byte hashes), which Cube Store can parse and merge.
 */
async function ensureHll(client: Client) {
  const { rows } = await client.query('SELECT 1 FROM pg_type WHERE typname = \'hll\'');
  if (rows.length) {
    return;
  }
  const statements = [
    'CREATE TYPE hll',
    'CREATE FUNCTION hll_in(cstring) RETURNS hll AS \'byteain\' LANGUAGE internal IMMUTABLE STRICT',
    'CREATE FUNCTION hll_out(hll) RETURNS cstring AS \'byteaout\' LANGUAGE internal IMMUTABLE STRICT',
    'CREATE FUNCTION hll_recv(internal) RETURNS hll AS \'bytearecv\' LANGUAGE internal IMMUTABLE STRICT',
    'CREATE FUNCTION hll_send(hll) RETURNS bytea AS \'byteasend\' LANGUAGE internal IMMUTABLE STRICT',
    'CREATE TYPE hll (INPUT = hll_in, OUTPUT = hll_out, RECEIVE = hll_recv, SEND = hll_send, LIKE = bytea)',
    'CREATE CAST (bytea AS hll) WITHOUT FUNCTION',
    `CREATE OR REPLACE FUNCTION hll_hash_any(anyelement) RETURNS bigint AS $$
      SELECT ('x' || substr(md5($1::text), 1, 16))::bit(64)::bigint
    $$ LANGUAGE sql IMMUTABLE`,
    `CREATE OR REPLACE FUNCTION hll_shim_final(bigint[]) RETURNS hll AS $$
      SELECT ('\\x128b7f'::bytea || coalesce(
        (SELECT string_agg(int8send(h), ''::bytea ORDER BY h) FROM (SELECT DISTINCT unnest($1) h) s),
        ''::bytea
      ))::hll
    $$ LANGUAGE sql IMMUTABLE`,
    `CREATE AGGREGATE hll_add_agg(bigint) (
      SFUNC = array_append, STYPE = bigint[], INITCOND = '{}', FINALFUNC = hll_shim_final
    )`,
  ];

  for (const sql of statements) {
    await client.query(sql);
  }
}

describe('old pre-aggregation bug repros (Postgres + Cube Store)', () => {
  jest.setTimeout(3 * 60 * 1000);

  let birdbox: BirdBox;

  const load = async (query: Record<string, unknown>): Promise<any> => {
    const url = `${birdbox.configuration.apiUrl}/load?query=${encodeURIComponent(JSON.stringify(query))}`;

    for (let i = 0; i < 120; i++) {
      const res = await fetch(url, { headers: { Authorization: 'test' } });
      const body: any = await res.json();
      if (body.error !== 'Continue wait') {
        return body;
      }
      await new Promise((r) => setTimeout(r, 500));
    }
    throw new Error(`Timed out waiting for ${JSON.stringify(query)}`);
  };

  const usedPreAggs = (body: any) => Object.keys(body.usedPreAggregations || {});

  beforeAll(async () => {
    const client = new Client(pgConfig);
    await client.connect();

    try {
      await ensureHll(client);
    } finally {
      await client.end();
    }

    birdbox = await startBirdBoxFromCli({
      type: 'postgresql',
      useCubejsServerBinary: true,
      schemaDir: 'old-bug-repros/schema',
      cubejsConfig: 'postgresql/single/cube.js',
      env: {
        CUBEJS_DB_HOST: pgConfig.host,
        CUBEJS_DB_PORT: `${pgConfig.port}`,
        CUBEJS_DB_NAME: pgConfig.database,
        CUBEJS_DB_USER: pgConfig.user,
        CUBEJS_DB_PASS: pgConfig.password,
        CUBEJS_CUBESTORE_HOST: process.env.CUBEJS_CUBESTORE_HOST || 'localhost',
        CUBEJS_CUBESTORE_PORT: process.env.CUBEJS_CUBESTORE_PORT || '3030',
        CUBEJS_EXTERNAL_DEFAULT: 'true',
        CUBEJS_SCHEDULED_REFRESH_DEFAULT: 'false',
        CUBEJS_PRE_AGGREGATIONS_SCHEMA: `old_bug_repros_${Date.now()}`,
        CUBEJS_PG_SQL_PORT: '',
      },
    });
  });

  afterAll(async () => {
    await birdbox?.stop();
  });

  describe('issue #8580 rolling window offset: start is ignored by pre-aggregations', () => {
    const query = (cube: string) => ({
      measures: [`${cube}.current_month_sum`],
      timeDimensions: [{
        dimension: `${cube}.date`,
        granularity: 'month',
        dateRange: ['2023-11-01', '2024-07-01'],
      }],
      order: { [`${cube}.date`]: 'asc' },
      limit: 5000,
    });
    // trailing 3 months, offset start: previous 3 months, excluding the current one
    const expected = [null, '10', '20', '30', '50', '90', '160', '130', '80'];

    test('baseline without pre-aggregation', async () => {
      const body = await load(query('order_rolling_nopa'));
      expect(usedPreAggs(body)).toEqual([]);
      expect(body.data.map((r: any) => r['order_rolling_nopa.current_month_sum'])).toEqual(expected);
    });

    test('served from the pre-aggregation', async () => {
      const body = await load(query('order_rolling'));
      expect(body.error).toBeUndefined();
      expect(usedPreAggs(body)).toEqual([expect.stringContaining('order_rolling_main')]);
      expect(body.data.map((r: any) => r['order_rolling.current_month_sum'])).toEqual(expected);
    });
  });

  describe('issue #8745 trailing unbounded rolling window without granularity returns NULL from pre-aggregation', () => {
    const query = (cube: string) => ({
      measures: [`${cube}.rollingCount`],
      timeDimensions: [{ dimension: `${cube}.createdAt`, dateRange: ['2023-07-11', '2023-07-18'] }],
      limit: 5000,
    });

    test('baseline without pre-aggregation', async () => {
      const body = await load(query('OrderNoPa'));
      expect(body.data).toEqual([{ 'OrderNoPa.rollingCount': '4' }]);
    });

    test('served from the pre-aggregation', async () => {
      const body = await load(query('Order'));
      expect(body.error).toBeUndefined();
      expect(usedPreAggs(body)).toEqual([expect.stringContaining('order_total_by_day')]);
      expect(body.data).toEqual([{ 'Order.rollingCount': '4' }]);
    });
  });

  describe('issue #8916 pre-aggregation with only count_distinct_approx measures does not build', () => {
    test('control: count_distinct_approx + count builds and is served', async () => {
      const body = await load({ measures: ['my_cube_test_ctl.mes'] });
      expect(body.error).toBeUndefined();
      expect(usedPreAggs(body)).toEqual([expect.stringContaining('my_cube_test_ctl_main')]);
      expect(body.data).toEqual([{ 'my_cube_test_ctl.mes': '3' }]);
    });

    test('only count_distinct_approx builds and is served', async () => {
      const body = await load({ measures: ['my_cube_test.mes'] });
      // Bug: "Error during create table ...: Sort key size can't be 0 for default,
      // columns: [Column { name: "my_cube_test__mes", column_type: HyperLogLog(Postgres), ... }]"
      expect(body.error).toBeUndefined();
      expect(usedPreAggs(body)).toEqual([expect.stringContaining('my_cube_test_main')]);
      expect(body.data).toEqual([{ 'my_cube_test.mes': '3' }]);
    });
  });

  describe('issue #9462 string measure is re-aggregated with SUM from a pre-aggregation', () => {
    const expected = (cube: string) => [
      { [`${cube}.legal_entity_id`]: 'e1', [`${cube}.display_names`]: 'Alice, Bob' },
      { [`${cube}.legal_entity_id`]: 'e2', [`${cube}.display_names`]: 'Carol' },
      { [`${cube}.legal_entity_id`]: 'e3', [`${cube}.display_names`]: 'Dave, Eve' },
    ];
    const query = (cube: string) => ({
      dimensions: [`${cube}.legal_entity_id`],
      measures: [`${cube}.display_names`],
      order: { [`${cube}.legal_entity_id`]: 'asc' },
    });

    test('baseline without pre-aggregation', async () => {
      const body = await load(query('LENoPa'));
      expect(body.data).toEqual(expected('LENoPa'));
    });

    test('exact match is served from the pre-aggregation with correct strings', async () => {
      const body = await load(query('LE'));
      expect(body.error).toBeUndefined();
      expect(usedPreAggs(body)).toEqual([expect.stringContaining('l_e_rollup')]);
      expect(body.data).toEqual(expected('LE'));
    });

    test('roll-up over the string measure is not served from the pre-aggregation', async () => {
      const body = await load({ measures: ['LE.display_names'] });
      expect(body.error).toBeUndefined();
      expect(usedPreAggs(body)).toEqual([]);
      expect(body.data).toEqual([{ 'LE.display_names': 'Alice, Bob, Carol, Dave, Eve' }]);
    });
  });
});
