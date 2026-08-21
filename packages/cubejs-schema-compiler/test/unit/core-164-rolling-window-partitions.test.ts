import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { BigqueryQuery } from '../../src/adapter/BigqueryQuery';
import { prepareYamlCompiler } from './PrepareCompiler';

// CORE-164: a query with a narrow dateRange against a partitioned pre-aggregation
// should only cause the partitions needed to serve that timeframe to be built.
//
// For cumulative (rolling window) measures the pre-aggregation description used
// to omit `matchedTimeDimensionDateRange` entirely, which made the query
// orchestrator fall back to building the pre-aggregation's whole build range
// (every partition). For a BOUNDED trailing window only the requested range,
// expanded backwards by the trailing window, is actually needed.

const SCHEMA = `
cubes:
  - name: rent
    sql: >
      SELECT 1 AS id, '2020-01-01'::timestamp AS date, 100 AS in_place_rent
    measures:
      - name: count
        type: count
      - name: rolling_7d
        sql: in_place_rent
        type: sum
        rollingWindow:
          trailing: 7 day
      - name: rolling_1m
        sql: in_place_rent
        type: sum
        rollingWindow:
          trailing: 1 month
      - name: rolling_unbounded
        sql: in_place_rent
        type: sum
        rollingWindow:
          trailing: unbounded
    dimensions:
      - name: id
        sql: id
        type: number
        primaryKey: true
      - name: date
        sql: date
        type: time
    preAggregations:
      - name: rolling_7d_pa
        measures:
          - rolling_7d
        timeDimension: date
        granularity: day
        partitionGranularity: week
      - name: rolling_unbounded_pa
        measures:
          - rolling_unbounded
        timeDimension: date
        granularity: day
        partitionGranularity: week
      - name: rolling_mixed_pa
        measures:
          - rolling_7d
          - rolling_1m
        timeDimension: date
        granularity: day
        partitionGranularity: week
`;

describe('CORE-164 rolling-window pre-aggregation partition scope', () => {
  async function preAggDescription(query, QueryClass: any = PostgresQuery) {
    const { compiler, cubeEvaluator, joinGraph } = prepareYamlCompiler(SCHEMA);
    await compiler.compile();
    const q = new QueryClass({ joinGraph, cubeEvaluator, compiler }, query);
    q.buildSqlAndParams();
    return q.preAggregations?.preAggregationsDescription();
  }

  it('bounded trailing window narrows to the requested range expanded by the window', async () => {
    const desc: any = await preAggDescription({
      measures: ['rent.rolling_7d'],
      timeDimensions: [{
        dimension: 'rent.date',
        granularity: 'day',
        dateRange: ['2024-06-10', '2024-06-10'],
      }],
      timezone: 'UTC',
    });

    expect(desc).toHaveLength(1);
    expect(desc[0].preAggregationId).toEqual('rent.rolling_7d_pa');
    // The requested day is 2024-06-10; a 7-day trailing window needs data back
    // to 2024-06-03, so the matched range must start no later than 2024-06-03
    // and end at the requested day — NOT be undefined (which builds everything).
    expect(desc[0].matchedTimeDimensionDateRange).toEqual([
      '2024-06-03T00:00:00.000',
      '2024-06-10T23:59:59.999',
    ]);
  });

  it('unbounded trailing window still builds the whole range (unchanged)', async () => {
    const desc: any = await preAggDescription({
      measures: ['rent.rolling_unbounded'],
      timeDimensions: [{
        dimension: 'rent.date',
        granularity: 'day',
        dateRange: ['2024-06-10', '2024-06-10'],
      }],
      timezone: 'UTC',
    });

    expect(desc).toHaveLength(1);
    expect(desc[0].preAggregationId).toEqual('rent.rolling_unbounded_pa');
    // Unbounded trailing genuinely needs all history => no narrowing.
    expect(desc[0].matchedTimeDimensionDateRange).toBeUndefined();
  });

  // Both measure orders, so the assertion pins the `seconds > maxTrailingSeconds`
  // comparison rather than a positional accident: with only one order, taking the
  // first window or taking the last would each pass for one of them.
  it.each([
    ['7d first', ['rent.rolling_7d', 'rent.rolling_1m']],
    ['1m first', ['rent.rolling_1m', 'rent.rolling_7d']],
  ])('shifts the start back by the largest trailing window when measures disagree (%s)', async (_label, measures) => {
    const desc: any = await preAggDescription({
      measures,
      timeDimensions: [{
        dimension: 'rent.date',
        granularity: 'day',
        dateRange: ['2024-06-10', '2024-06-10'],
      }],
      timezone: 'UTC',
    });

    // Assert the id list first: a missing mixed rollup then reads as a match failure
    // rather than a TypeError on the range lookup below.
    const ids = desc.map(d => d.preAggregationId);
    expect(ids).toContain('rent.rolling_mixed_pa');
    // 1 month is the wider of the two windows, so the lookback must reach back a
    // month — shifting by the 7-day window would leave the 1-month measure short.
    const mixed = desc.find(d => d.preAggregationId === 'rent.rolling_mixed_pa');
    expect(mixed.matchedTimeDimensionDateRange).toEqual([
      '2024-05-10T00:00:00.000',
      '2024-06-10T23:59:59.999',
    ]);
  });

  // The description also accepts its range from an `inDateRange` filter when the time
  // dimension carries no dateRange. That fallback is unreachable for a partitioned
  // rollup: expressing the range as a filter stops the rollup matching at all, for a
  // plain count as much as for a rolling window — so nothing reaches the range code.
  // Pinned here so a change that starts matching this shape surfaces as a failure
  // rather than silently exercising an untested lookback path.
  it.each([
    ['plain count', 'rent.count'],
    ['rolling window', 'rent.rolling_7d'],
  ])('does not match a partitioned rollup when the range is only an inDateRange filter (%s)', async (_label, measure) => {
    const desc: any = await preAggDescription({
      measures: [measure],
      timeDimensions: [{
        dimension: 'rent.date',
        granularity: 'day',
      }],
      filters: [{
        member: 'rent.date',
        operator: 'inDateRange',
        values: ['2024-06-10', '2024-06-10'],
      }],
      timezone: 'UTC',
    });

    expect(desc).toEqual([]);
  });

  it('expanded range keeps the query timestamp precision (microseconds)', async () => {
    // BigQuery uses microsecond precision. The expanded start/end must be emitted
    // at that same precision — mixing 3-digit and 6-digit strings produced
    // malformed ranges (`...,...999999`) that the partition loader rejected.
    const desc: any = await preAggDescription({
      measures: ['rent.rolling_7d'],
      timeDimensions: [{
        dimension: 'rent.date',
        granularity: 'day',
        dateRange: ['2024-06-10', '2024-06-10'],
      }],
      timezone: 'UTC',
    }, BigqueryQuery);

    expect(desc[0].matchedTimeDimensionDateRange).toEqual([
      '2024-06-03T00:00:00.000000',
      '2024-06-10T23:59:59.999999',
    ]);
  });
});
