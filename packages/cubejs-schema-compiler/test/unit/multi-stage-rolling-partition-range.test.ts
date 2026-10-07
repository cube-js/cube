import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from './PrepareCompiler';

// A multi-stage rolling or shifted measure over the base measure is matched at
// its leaf; the partitions loaded must cover the band that leaf reads.
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

      - name: amount_running_total_ms
        multi_stage: true
        type: sum
        sql: "{amount}"
        rolling_window:
          trailing: unbounded

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
      - name: daily
        measures:
          - amount
        dimensions:
          - status
        time_dimension: created_at
        granularity: day
        partition_granularity: day
`;

  // The query asks for June 2024 only, at month granularity, unless told otherwise.
  const descriptionFor = async (
    measures: string[],
    timeDimension: Record<string, any> = { granularity: 'month', dateRange: ['2024-06-01', '2024-06-30'] },
    preAggregationId = 'orders.monthly',
  ) => {
    const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(model);
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures,
      dimensions: ['orders.status'],
      timeDimensions: [{ dimension: 'orders.created_at', ...timeDimension }],
      timezone: 'UTC',
      preAggregationsSchema: '',
      useNativeSqlPlanner: true,
    });

    const [sql] = query.buildSqlAndParams();
    const descriptions: any[] = query.preAggregations?.preAggregationsDescription() || [];
    const matched = descriptions.filter(d => d.preAggregationId === preAggregationId);
    expect(matched.length).toEqual(1);

    return { ...matched[0], sql };
  };

  const partitionRangeFor = async (measures: string[]) => (await descriptionFor(measures)).matchedTimeDimensionDateRange;

  it('a rolling measure with its own sql is not bounded', async () => {
    expect(await partitionRangeFor(['orders.amount_r3'])).toBeUndefined();
  });

  it('a multi-stage rolling measure loads the whole window', async () => {
    expect(await partitionRangeFor(['orders.amount_r3_ms']))
      .toEqual(['2024-03-01T00:00:00.000', '2024-07-29T23:59:59.999']);
  });

  it('a single usage reads the table under its plain name', async () => {
    const description = await descriptionFor(['orders.amount_r3_ms']);
    expect(Object.keys(description.usageMapping)).toEqual(['']);
    expect(description.sql).toContain(`${description.tableName} `);
    expect(description.sql).not.toContain('__usage_');
  });

  it('a multi-stage to_date measure loads the period from its start', async () => {
    expect(await partitionRangeFor(['orders.amount_ytd_ms']))
      .toEqual(['2024-01-01T00:00:00.000', '2024-07-29T23:59:59.999']);
  });

  it('a multi-stage time-shifted measure loads the shifted period', async () => {
    expect(await partitionRangeFor(['orders.amount_prev_month_ms']))
      .toEqual(['2024-05-01T00:00:00.000', '2024-05-31T23:59:59.999']);
  });

  it('a month shift ending on a day the earlier month also has keeps that day', async () => {
    // Jul 30 shifted back a month is Jun 30, which an exclusive end (Jul 31) would clamp past.
    const description = await descriptionFor(
      ['orders.amount_prev_month_ms'],
      { granularity: 'day', dateRange: ['2024-07-01', '2024-07-30'] },
      'orders.daily',
    );
    expect(description.matchedTimeDimensionDateRange)
      .toEqual(['2024-06-01T00:00:00.000', '2024-06-30T23:59:59.999']);
  });

  it('an unbounded multi-stage rolling measure is not bounded', async () => {
    expect(await partitionRangeFor(['orders.amount_running_total_ms'])).toBeUndefined();
  });

  it('an unbounded usage lifts the bound of the usages it is grouped with', async () => {
    const description = await descriptionFor(['orders.amount_running_total_ms', 'orders.amount_prev_month_ms']);
    expect(Object.keys(description.usageMapping)).toHaveLength(2);
    expect(description.matchedTimeDimensionDateRange).toBeUndefined();
  });
});

// Only filters on the partition time dimension bound the partitions.
describe('Pre-aggregation partition range with another time dimension', () => {
  const model = (preAggregation: string) => `
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
      - name: updated_at
        sql: updated_at
        type: time
    measures:
      - name: amount
        type: sum
        sql: amount
    pre_aggregations:
      - name: monthly
        measures:
          - amount
        partition_granularity: month
${preAggregation}
`;

  const partitionRangeFor = async (preAggregation: string, query: Record<string, any>) => {
    const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(model(preAggregation));
    await compiler.compile();

    const pgQuery = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['orders.amount'],
      dimensions: ['orders.status'],
      ...query,
      timezone: 'UTC',
      preAggregationsSchema: '',
      useNativeSqlPlanner: true,
    });

    pgQuery.buildSqlAndParams();
    const [description]: any[] = pgQuery.preAggregations?.preAggregationsDescription() || [];
    expect(description.preAggregationId).toEqual('orders.monthly');

    return description.matchedTimeDimensionDateRange;
  };

  const createdInJune = {
    dimension: 'orders.created_at',
    granularity: 'month',
    dateRange: ['2024-06-01', '2024-06-30'],
  };
  const june = ['2024-06-01T00:00:00.000', '2024-06-30T23:59:59.999'];

  it('ignores a date filter on a stored time dimension', async () => {
    expect(await partitionRangeFor(`        dimensions:
          - status
          - updated_at
        time_dimension: created_at
        granularity: month`, {
      filters: [{ member: 'orders.updated_at', operator: 'inDateRange', values: ['2020-01-01', '2020-12-31'] }],
      timeDimensions: [createdInJune],
    })).toEqual(june);
  });

  it('ignores a date range on a second time dimension', async () => {
    expect(await partitionRangeFor(`        dimensions:
          - status
        time_dimensions:
          - dimension: created_at
            granularity: month
          - dimension: updated_at
            granularity: day`, {
      timeDimensions: [
        { dimension: 'orders.updated_at', granularity: 'day', dateRange: ['2020-01-01', '2020-12-31'] },
        createdInJune,
      ],
    })).toEqual(june);
  });
});
