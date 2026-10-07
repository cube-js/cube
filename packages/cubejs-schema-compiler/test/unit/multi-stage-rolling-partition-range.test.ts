import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from './PrepareCompiler';

// A partitioned rollup stores only the base monthly sum. Rolling and
// time-shifted measures built on top of it reach back before the queried
// date range, so the partitions loaded for the query must cover those
// earlier months too, not only the months inside the requested range.
//
// A rolling measure with its own SQL is detected as cumulative and leaves
// the partition range open. The same window written as a multi-stage measure
// that references the base measure is matched at its leaf, whose filter
// carries the widened band; that band has to reach the partition range, or
// the rolling sum silently degrades to the requested month alone.
describe('Multi-stage rolling measures over a partitioned rollup', () => {
  const model = `
cubes:
  - name: orders
    sql: "SELECT * FROM orders"
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: status
        sql: status
        type: string
      - name: created_at
        sql: created_at
        type: time
    measures:
      - name: amount
        type: sum
        sql: amount

      - name: amount_r3
        type: sum
        sql: amount
        rolling_window:
          trailing: 3 month

      - name: amount_r3_ms
        multi_stage: true
        type: sum
        sql: "{amount}"
        rolling_window:
          trailing: 3 month

      - name: amount_ytd_ms
        multi_stage: true
        type: sum
        sql: "{amount}"
        rolling_window:
          type: to_date
          granularity: year

      - name: amount_prev_month_ms
        multi_stage: true
        type: number
        sql: "{amount}"
        time_shift:
          - interval: 1 month
            type: prior
    pre_aggregations:
      - name: monthly
        measures:
          - amount
          - amount_r3
        dimensions:
          - status
        time_dimension: created_at
        granularity: month
        partition_granularity: month
`;

  // The query asks for June 2024 only, at month granularity.
  const partitionRangeFor = async (measure: string) => {
    const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(model);
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [measure],
      dimensions: ['orders.status'],
      timeDimensions: [{
        dimension: 'orders.created_at',
        granularity: 'month',
        dateRange: ['2024-06-01', '2024-06-30'],
      }],
      timezone: 'UTC',
      preAggregationsSchema: '',
      useNativeSqlPlanner: true,
    });

    query.buildSqlAndParams();
    const descriptions: any[] = query.preAggregations?.preAggregationsDescription() || [];
    const monthly = descriptions.filter(d => d.preAggregationId === 'orders.monthly');
    expect(monthly.length).toBeGreaterThan(0);

    return monthly.map(d => d.matchedTimeDimensionDateRange);
  };

  // Partitions must reach back to `from`: an undefined range means
  // "not bounded", which covers it as well.
  const expectPartitionsFrom = (ranges: ([string, string] | undefined)[], from: string) => {
    ranges.forEach(range => {
      if (range) {
        // Reports the range start itself when it begins after `from`.
        expect(range[0] <= from ? from : range[0]).toEqual(from);
      }
    });
  };

  it('a rolling measure with its own sql loads the whole window', async () => {
    expectPartitionsFrom(await partitionRangeFor('orders.amount_r3'), '2024-04-01T00:00:00.000');
  });

  it('a multi-stage rolling measure over the base measure loads the whole window', async () => {
    expectPartitionsFrom(await partitionRangeFor('orders.amount_r3_ms'), '2024-04-01T00:00:00.000');
  });

  it('a multi-stage to_date measure over the base measure loads the period from its start', async () => {
    expectPartitionsFrom(await partitionRangeFor('orders.amount_ytd_ms'), '2024-01-01T00:00:00.000');
  });

  it('a multi-stage time-shifted measure over the base measure loads the shifted period', async () => {
    expectPartitionsFrom(await partitionRangeFor('orders.amount_prev_month_ms'), '2024-05-01T00:00:00.000');
  });
});
