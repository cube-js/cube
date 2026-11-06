import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { ClickHouseQuery } from '../../../src/adapter/ClickHouseQuery';
import { ClickHouseDbRunner } from './ClickHouseDbRunner';

// A range ends at `23:59:59.999`, so a bound parsed to whole seconds drops the rows
// within the last second of the range.
describe('ClickHouse time range bounds', () => {
  jest.setTimeout(200000);

  const dbRunner = new ClickHouseDbRunner();

  const noDataSet = async () => Promise.resolve();

  afterAll(async () => {
    await dbRunner.tearDown();
  });

  const compilers = prepareYamlCompiler(`
cubes:
  - name: events
    sql: >
      SELECT 1 AS id, toDateTime64('2026-01-20 10:00:00.000', 3, 'UTC') AS ts, 1 AS amount
      UNION ALL
      SELECT 2 AS id, toDateTime64('2026-01-20 23:59:59.500', 3, 'UTC') AS ts, 10 AS amount
      UNION ALL
      SELECT 3 AS id, toDateTime64('2026-01-21 00:00:00.000', 3, 'UTC') AS ts, 100 AS amount
      UNION ALL
      SELECT 4 AS id, toDateTime64('2026-01-21 23:59:59.999', 3, 'UTC') AS ts, 1000 AS amount
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: ts
        sql: ts
        type: time
    measures:
      - name: amount
        sql: amount
        type: sum

  - name: events_rollup
    extends: events
    pre_aggregations:
      - name: daily
        measures:
          - amount
        dimensions:
          - id
        time_dimension: ts
        granularity: day
        partition_granularity: day
        indexes:
          - name: by_id
            columns:
              - id
`);

  beforeAll(async () => {
    await compilers.compiler.compile();
  });

  it('keeps the rows within the last second of a date range', async () => {
    const query = new ClickHouseQuery(compilers, {
      measures: ['events.amount'],
      timeDimensions: [{
        dimension: 'events.ts',
        dateRange: ['2026-01-20', '2026-01-20'],
      }],
      timezone: 'UTC',
    });

    const [row] = await dbRunner.testQuery(query.buildSqlAndParams(), noDataSet);

    expect(Number(row.events__amount)).toEqual(11);
  });

  it('keeps the rows within the last second of each partition', async () => {
    const query = new ClickHouseQuery(compilers, {
      measures: ['events_rollup.amount'],
      timeDimensions: [{
        dimension: 'events_rollup.ts',
        granularity: 'day',
        dateRange: ['2026-01-20', '2026-01-21'],
      }],
      order: [{ id: 'events_rollup.ts' }],
      timezone: 'UTC',
      preAggregationsSchema: '',
    });

    const preAggregations: any[] = query.preAggregations?.preAggregationsDescription() ?? [];
    expect(preAggregations.map(p => p.preAggregationId)).toEqual(['events_rollup.daily']);

    // The build range a refresh reads from the data: one partition per day
    preAggregations[0].dateRange = ['2026-01-20T00:00:00.000', '2026-01-21T23:59:59.999'];
    const seed = 1;
    const results = await dbRunner.testQueries([
      ...dbRunner.tempTablePreAggregations(preAggregations, seed),
      dbRunner.replaceTableName(query.buildSqlAndParams(), preAggregations, seed),
    ], noDataSet);
    const rows = results[results.length - 1];

    expect(rows.map((r: any) => [r.events_rollup__ts_day, Number(r.events_rollup__amount)])).toEqual([
      ['2026-01-20T00:00:00.000', 11],
      ['2026-01-21T00:00:00.000', 1100],
    ]);
  });
});
