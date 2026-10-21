import { BaseQuery } from '../../src/adapter/BaseQuery';
import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from './PrepareCompiler';

const model = `
cubes:
  - name: orders
    sql: SELECT 1 AS id, 1 AS user_id, '2026-01-01'::timestamp AS created_at
    joins:
      - name: users
        sql: "{CUBE.user_id} = {users.id}"
        relationship: many_to_one
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: user_id
        sql: user_id
        type: number
      - name: created_at
        sql: created_at
        type: time
    measures:
      - name: count
        type: count
    pre_aggregations:
      - name: orders_rollup
        type: rollup
        measures:
          - count
        dimensions:
          - user_id
        time_dimension: created_at
        granularity: day
        partition_granularity: month

  - name: users
    sql: SELECT 1 AS id, 'a' AS name
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: name
        sql: name
        type: string
    pre_aggregations:
      - name: users_rollup
        type: rollup
        dimensions:
          - id
          - name
      - name: orders_with_users
        type: rollup_join
        rollups:
          - orders.orders_rollup
          - users.users_rollup
        measures:
          - orders.count
        dimensions:
          - name
        time_dimension: orders.created_at
        granularity: day
`;

describe('Pre-aggregation SQL evaluation query', () => {
  const compilers = prepareYamlCompiler(model);

  beforeAll(() => compilers.compile());

  afterEach(() => jest.restoreAllMocks());

  const newQuery = () => compilers.createQuery(PostgresQuery, {
    measures: ['orders.count'],
    dimensions: ['users.name'],
    timeDimensions: [{
      dimension: 'orders.created_at',
      granularity: 'day',
      dateRange: ['2026-01-01', '2026-02-15'],
    }],
    timezone: 'UTC',
  });

  const ordersRollup = () => compilers.cubeEvaluator.preAggregationsForCube('orders').orders_rollup;

  it('is its own SQL evaluation query', () => {
    const aggregation = ordersRollup();
    const query = newQuery().preAggregationQueryForSqlEvaluation('orders', aggregation);
    const newSubQueryForCube = jest.spyOn(BaseQuery.prototype, 'newSubQueryForCube');

    expect(query.preAggregationQueryForSqlEvaluation('orders', aggregation)).toBe(query);
    expect(query.preAggregationQueryForSqlEvaluation('orders', { ...aggregation })).not.toBe(query);
    newSubQueryForCube.mockClear();

    query.preAggregationLoadSql('orders', aggregation, 'orders_rollup');
    expect(newSubQueryForCube).not.toHaveBeenCalled();
  });

  it('builds the SQL evaluation query once per description', () => {
    const query = newQuery();
    query.preAggregations.findPreAggregationForQuery();
    const newSubQueryForCube = jest.spyOn(BaseQuery.prototype, 'newSubQueryForCube');

    const descriptions = query.preAggregations.preAggregationsDescription();

    expect(descriptions.map(d => d.preAggregationId)).toEqual([
      'orders.orders_rollup',
      'users.users_rollup',
    ]);
    // Per rollup: the rollup query and the dialect query it asks about ungrouping,
    // plus the build range query of the partitioned one
    expect(newSubQueryForCube.mock.calls.map(([cube]) => cube)).toEqual(['orders', 'orders', 'orders', 'users', 'users']);
  });
});
