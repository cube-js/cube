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
        output_column_types:
          - member: count
            type: integer
          - member: user_id
            type: integer

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

  it('renders the rollup from its SQL evaluation query without building another one', () => {
    const aggregation = ordersRollup();
    const query = newQuery().preAggregationQueryForSqlEvaluation('orders', aggregation);
    const newSubQueryForCube = jest.spyOn(BaseQuery.prototype, 'newSubQueryForCube');

    const [sql, params] = query.preAggregationSql('orders', aggregation);
    // The user query joins users, the rollup doesn't
    expect(sql).not.toContain('users');
    expect(query.preAggregationLoadSql('orders', aggregation, 'orders_rollup')).toEqual([
      `CREATE TABLE orders_rollup AS ${sql}`,
      params,
    ]);
    expect(query.preAggregationOutputColumnTypes('orders', aggregation)).toEqual([
      { name: 'orders__user_id', type: 'integer' },
      { name: 'orders__created_at_day', type: 'TIMESTAMP' },
      { name: 'orders__count', type: 'integer' },
    ]);
    expect(newSubQueryForCube).not.toHaveBeenCalled();
  });

  it('refuses to render the rollup from the user query', () => {
    const aggregation = ordersRollup();
    const query = newQuery();

    expect(() => query.preAggregationSql('orders', aggregation))
      .toThrow('preAggregationSql() for \'orders\' must be called on its SQL evaluation query');
    expect(() => query.preAggregationOutputColumnTypes('orders', aggregation))
      .toThrow('preAggregationOutputColumnTypes() for \'orders\' must be called on its SQL evaluation query');
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
