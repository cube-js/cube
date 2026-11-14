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
      - name: orders_lambda
        type: rollup_lambda
        rollups:
          - orders_rollup

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

describe('Pre-aggregation references memoization', () => {
  const compilers = prepareYamlCompiler(model);

  beforeAll(() => compilers.compile());

  const rollupByName = (cube: string, name: string): any => compilers.createQuery(PostgresQuery, {
    measures: ['orders.count'],
    dimensions: ['users.name'],
    timezone: 'UTC',
  }).preAggregations.getRollupPreAggregationByName(cube, name);

  it('rollup_join keeps its references and rollup join cache key across queries', () => {
    const first = rollupByName('users', 'orders_with_users');

    for (let i = 0; i < 3; i++) {
      const next = rollupByName('users', 'orders_with_users');
      expect(next.references.rollupsReferences).toHaveLength(2);
      expect(next.resolveRollupJoin()).toBe(first.resolveRollupJoin());
    }
  });

  it('rollup_lambda keeps its references across queries', () => {
    for (let i = 0; i < 3; i++) {
      expect(rollupByName('orders', 'orders_lambda').references.rollupsReferences).toHaveLength(1);
    }
  });
});
