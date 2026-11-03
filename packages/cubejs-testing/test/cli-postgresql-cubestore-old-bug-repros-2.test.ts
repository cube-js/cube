/**
 * E2E repros for old pre-aggregation bug reports: a real Cube server (local binary),
 * a real Postgres and a real Cube Store. Each test asserts the correct result, so
 * it fails until its bug is fixed. #7421 no longer reproduces and is kept as a
 * regression guard.
 *
 * #11124 also needs a second Postgres database for the `secondary` data source
 * (CUBEJS_DS_SECONDARY_DB_NAME, default `test2`).
 *
 * No Docker required. Expects:
 *   - Postgres reachable via CUBEJS_DB_HOST/PORT/NAME/USER/PASS
 *     (default localhost:5432, db `test`, user `root`, password `test`)
 *   - Cube Store reachable via CUBEJS_CUBESTORE_HOST/PORT (default localhost:3030)
 *
 *   yarn tsc && yarn jest --runInBand dist/test/cli-postgresql-cubestore-old-bug-repros-2.test.js
 */
import { afterAll, beforeAll, describe, expect, jest, test } from '@jest/globals';
import fetch from 'node-fetch';

import { BirdBox, startBirdBoxFromCli } from '../src';

describe('old pre-aggregation bug repros 2 (Postgres + Cube Store)', () => {
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
    birdbox = await startBirdBoxFromCli({
      type: 'postgresql',
      useCubejsServerBinary: true,
      schemaDir: 'old-bug-repros-2/schema',
      cubejsConfig: 'postgresql/single/cube.js',
      env: {
        CUBEJS_DB_HOST: process.env.CUBEJS_DB_HOST || 'localhost',
        CUBEJS_DB_PORT: process.env.CUBEJS_DB_PORT || '5432',
        CUBEJS_DB_NAME: process.env.CUBEJS_DB_NAME || 'test',
        CUBEJS_DB_USER: process.env.CUBEJS_DB_USER || 'root',
        CUBEJS_DB_PASS: process.env.CUBEJS_DB_PASS || 'test',
        CUBEJS_CUBESTORE_HOST: process.env.CUBEJS_CUBESTORE_HOST || 'localhost',
        CUBEJS_CUBESTORE_PORT: process.env.CUBEJS_CUBESTORE_PORT || '3030',
        CUBEJS_EXTERNAL_DEFAULT: 'true',
        CUBEJS_SCHEDULED_REFRESH_DEFAULT: 'false',
        CUBEJS_PRE_AGGREGATIONS_SCHEMA: `old_bug_repros_2_${Date.now()}`,
        CUBEJS_PG_SQL_PORT: '',
        CUBEJS_DATASOURCES: 'default,secondary',
        CUBEJS_DS_SECONDARY_DB_TYPE: 'postgres',
        CUBEJS_DS_SECONDARY_DB_HOST: process.env.CUBEJS_DB_HOST || 'localhost',
        CUBEJS_DS_SECONDARY_DB_PORT: process.env.CUBEJS_DB_PORT || '5432',
        CUBEJS_DS_SECONDARY_DB_NAME: process.env.CUBEJS_DS_SECONDARY_DB_NAME || 'test2',
        CUBEJS_DS_SECONDARY_DB_USER: process.env.CUBEJS_DB_USER || 'root',
        CUBEJS_DS_SECONDARY_DB_PASS: process.env.CUBEJS_DB_PASS || 'test',
      },
    });
  });

  afterAll(async () => {
    await birdbox?.stop();
  });

  describe('issue #7421 use_original_sql_pre_aggregations with a Postgres-stored original_sql', () => {
    test('rollup built on top of the original_sql pre-aggregation is served', async () => {
      const body = await load({
        measures: ['base_positions.position_count'],
        timeDimensions: [{
          dimension: 'base_positions.position_date',
          granularity: 'month',
          dateRange: ['2024-01-01', '2024-02-29'],
        }],
        order: { 'base_positions.position_date': 'asc' },
      });
      expect(body.error).toBeUndefined();
      expect(usedPreAggs(body)).toEqual([
        expect.stringContaining('base_positions_main'),
        expect.stringContaining('base_positions_positions_per_month'),
      ]);
      expect(body.data.map((r: any) => r['base_positions.position_count'])).toEqual(['2', '1']);
    });
  });

  describe('issue #9638 rollup_lambda with a non-UTC query timezone', () => {
    const query = (timezone: string) => ({
      measures: ['lambda_tz.count'],
      timezone,
    });

    test('control: UTC returns every row', async () => {
      const body = await load(query('UTC'));
      expect(body.error).toBeUndefined();
      expect(usedPreAggs(body)).toEqual([expect.stringContaining('lambda_tz_main')]);
      expect(body.data).toEqual([{ 'lambda_tz.count': '5' }]);
    });

    test('Europe/Madrid returns every row (none lost between batch and lambda parts)', async () => {
      const body = await load(query('Europe/Madrid'));
      expect(body.error).toBeUndefined();
      expect(usedPreAggs(body)).toEqual([expect.stringContaining('lambda_tz_main')]);
      expect(body.data).toEqual([{ 'lambda_tz.count': '5' }]);
    });
  });

  describe('issue #11124 cross-data-source rollup_join with a measure from the secondary cube', () => {
    const query = (measures: string[]) => ({
      measures,
      dimensions: ['SecondaryCube.location_id'],
      order: { 'SecondaryCube.location_id': 'asc' },
    });

    test('control: primary measure + secondary dimension', async () => {
      const body = await load(query(['PrimaryCube.total_count']));
      expect(body.error).toBeUndefined();
      expect(body.data).toEqual([
        { 'SecondaryCube.location_id': 'loc_1', 'PrimaryCube.total_count': '1' },
        { 'SecondaryCube.location_id': 'loc_2', 'PrimaryCube.total_count': '1' },
      ]);
    });

    test('adding a secondary measure is still served by the rollup_join', async () => {
      const body = await load(query(['PrimaryCube.total_count', 'SecondaryCube.total_contract_value']));
      expect(body.error).toBeUndefined();
      expect(body.data).toEqual([
        { 'SecondaryCube.location_id': 'loc_1', 'PrimaryCube.total_count': '1', 'SecondaryCube.total_contract_value': '500' },
        { 'SecondaryCube.location_id': 'loc_2', 'PrimaryCube.total_count': '1', 'SecondaryCube.total_contract_value': '300' },
      ]);
    });
  });
});
