import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { ClickHouseQuery } from '../../../src/adapter/ClickHouseQuery';
import type { ClickHouseClient } from '@clickhouse/client';
import { ClickHouseDbRunner } from './ClickHouseDbRunner';

describe('ClickHouse boolean members in pre-aggregations', () => {
  jest.setTimeout(200000);

  const dbRunner = new ClickHouseDbRunner();

  const noDataSet = async () => Promise.resolve();

  afterAll(async () => {
    await dbRunner.tearDown();
  });

  const cubeSql = `
    SELECT 1 AS id, 17390 AS brand_id, 'a' AS brand, toDateTime('2025-04-01 10:00:00') AS create_dt
    UNION ALL SELECT 2, 5, 'b', toDateTime('2025-04-02 10:00:00')
  `;

  const members = `
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: brand
        sql: brand
        type: string
      - name: is_current_brand
        sql: "{CUBE}.brand_id = 17390"
        type: boolean
      - name: created_at
        sql: create_dt
        type: time
    measures:
      - name: count
        type: count
    segments:
      - name: curr_brand
        sql: "{CUBE}.brand_id = 17390"
`;

  // The same model twice: `metrics` is served from its rollup, `metrics_raw` from the source
  const compilers = prepareYamlCompiler(`
cubes:
  - name: metrics
    sql: "${cubeSql.replace(/\n/g, ' ')}"
${members}
    pre_aggregations:
      - name: main
        measures: [metrics.count]
        dimensions: [metrics.brand, metrics.is_current_brand]
        segments: [metrics.curr_brand]
        time_dimension: metrics.created_at
        granularity: day
        indexes:
          - name: brand_index
            columns: [metrics.brand]
  - name: metrics_raw
    sql: "${cubeSql.replace(/\n/g, ' ')}"
${members}  - name: flags
    sql_table: flags
    dimensions:
      - name: flag
        sql: flag
        type: boolean
    measures:
      - name: count
        type: count
`);

  beforeAll(async () => {
    await compilers.compile();
  });

  const query = (cube: string, options: { dimensions?: string[], segments?: string[] }) => compilers.createQuery(ClickHouseQuery, {
    measures: [`${cube}.count`],
    dimensions: (options.dimensions || []).map(d => `${cube}.${d}`),
    segments: (options.segments || []).map(s => `${cube}.${s}`),
    timeDimensions: [{
      dimension: `${cube}.created_at`,
      granularity: 'day',
      dateRange: ['2025-04-01', '2025-04-30'],
    }],
    order: [{ id: `${cube}.created_at` }],
    timezone: 'UTC',
    preAggregationsSchema: '',
  });

  const rawRows = async (options: { dimensions?: string[], segments?: string[] }) => {
    const rows = await dbRunner.testQuery(query('metrics_raw', options).buildSqlAndParams() as [string, unknown[]], noDataSet);
    return rows.map(row => Object.fromEntries(
      Object.entries(row).map(([key, value]) => [key.replace(/^metrics_raw__/, 'metrics__'), value])
    ));
  };

  const preAggregationRows = async (options: { dimensions?: string[], segments?: string[] }) => {
    const q = query('metrics', options);
    expect(q.buildSqlAndParams()[0]).toContain('metrics_main');
    const results = await dbRunner.evaluateQueryWithPreAggregations(q);
    return results[results.length - 1];
  };

  it('reads a boolean dimension back as a boolean from the rollup and the source alike', async () => {
    const options = { dimensions: ['brand', 'is_current_brand'] };
    const expected = [
      { metrics__brand: 'a', metrics__is_current_brand: true, metrics__created_at_day: '2025-04-01T00:00:00.000', metrics__count: '1' },
      { metrics__brand: 'b', metrics__is_current_brand: false, metrics__created_at_day: '2025-04-02T00:00:00.000', metrics__count: '1' },
    ];

    expect(await rawRows(options)).toEqual(expected);
    expect(await preAggregationRows(options)).toEqual(expected);
  });

  it('filters by a segment stored in the rollup', async () => {
    const options = { dimensions: ['brand'], segments: ['curr_brand'] };
    const expected = [
      { metrics__brand: 'a', metrics__created_at_day: '2025-04-01T00:00:00.000', metrics__count: '1' },
    ];

    expect(await rawRows(options)).toEqual(expected);
    expect(await preAggregationRows(options)).toEqual(expected);
  });

  it('keeps primary key pruning when filtering by a boolean dimension', async () => {
    const flagsDataSet = async (client: ClickHouseClient) => {
      await client.command({ query: 'CREATE TEMPORARY TABLE flags (flag Bool, v UInt64) ENGINE = MergeTree ORDER BY (flag, v) SETTINGS index_granularity = 1' });
      await client.command({ query: 'INSERT INTO flags SELECT number % 2 = 0, number FROM numbers(16)' });
    };
    const [sql, params] = compilers.createQuery(ClickHouseQuery, {
      measures: ['flags.count'],
      dimensions: ['flags.flag'],
      filters: [{ member: 'flags.flag', operator: 'equals', values: ['1'] }],
      timezone: 'UTC',
    }).buildSqlAndParams();

    const [rows, plan] = await dbRunner.testQueries([
      [sql, params],
      [`EXPLAIN indexes = 1 ${sql}`, params],
    ], flagsDataSet);
    expect(rows).toEqual([{ flags__flag: true, flags__count: '8' }]);

    const primaryKey = plan.map(row => String(row.explain).trim());
    const conditionAt = primaryKey.indexOf('PrimaryKey');
    expect(conditionAt).toBeGreaterThan(-1);
    expect(primaryKey.slice(conditionAt).find(line => line.startsWith('Condition:'))).not.toEqual('Condition: true');
  });

  it('builds the rollup with boolean columns', async () => {
    const q = query('metrics', { dimensions: ['brand', 'is_current_brand'], segments: ['curr_brand'] });
    q.buildSqlAndParams();
    const [loadSql, params] = q.preAggregations!.preAggregationsDescription()[0].loadSql;
    // The rollup's column types are the ones of the SELECT it is created from
    const selectSql = loadSql.replace(/^CREATE TABLE .*? ORDER BY \(.*?\) AS /s, '');
    const [row] = await dbRunner.testQuery(
      [`SELECT toTypeName(metrics__is_current_brand) AS dimension_type, toTypeName(metrics__curr_brand) AS segment_type FROM (${selectSql}) LIMIT 1`, params],
      noDataSet,
    );
    expect(row).toEqual({ dimension_type: 'Bool', segment_type: 'Bool' });
  });
});
