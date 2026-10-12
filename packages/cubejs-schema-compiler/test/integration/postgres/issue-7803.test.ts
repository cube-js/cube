import { getEnv } from '@cubejs-backend/shared';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// GH #7803: ordering a multi-fact query (measures from two one_to_many joined
// cubes) by a measure that is NOT in the selection fails with
// `missing FROM-clause entry for table "dev_applications"`.
//
// Tesseract plans the query as two fact CTEs joined on the aggregate keys, but
// renders the non-selected order-by measure as its raw expression
// (`count("dev_applications".application_id)`) in the outer ORDER BY, where the
// cube alias is not in scope. The legacy planner silently drops such order items.
describe('Order by a non-selected measure in a multi-fact query (GH #7803)', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
cubes:
  - name: dev_applications
    sql: >
      select 1 as application_id, true as is_accepted, 1 as user_id
      union all
      select 2 as application_id, false as is_accepted, 2 as user_id
    dimensions:
      - name: application_id
        sql: application_id
        type: number
        primary_key: true
      - name: user_id
        sql: user_id
        type: number
    measures:
      - name: total_count
        type: count
        sql: application_id
      - name: accepted_count
        type: count
        sql: is_accepted
        filters:
          - sql: "{CUBE}.is_accepted"

  - name: dev_purchases
    sql: >
      select 1 as purchase_id, 1 as user_id
      union all
      select 2 as purchase_id, 1 as user_id
    dimensions:
      - name: purchase_id
        sql: purchase_id
        type: number
        primary_key: true
      - name: user_id
        sql: user_id
        type: number
    measures:
      - name: total_count
        type: count
        sql: purchase_id

  - name: dev_users
    sql: >
      select 1 as user_id
      union all
      select 2 as user_id
    dimensions:
      - name: user_id
        sql: user_id
        type: number
        primary_key: true
    joins:
      - name: dev_applications
        relationship: one_to_many
        sql: "{CUBE}.user_id = {dev_applications.user_id}"
      - name: dev_purchases
        relationship: one_to_many
        sql: "{CUBE}.user_id = {dev_purchases.user_id}"

views:
  - name: view_users
    cubes:
      - join_path: dev_users
        includes:
          - user_id
      - join_path: dev_users.dev_applications
        alias: applications
        prefix: true
        includes:
          - total_count
          - accepted_count
      - join_path: dev_users.dev_purchases
        alias: purchases
        prefix: true
        includes:
          - total_count
`);

  // Control: the same measures without the extra order item work.
  it('multi-fact measures ordered by a selected dimension', async () => dbRunner.runQueryTest({
    measures: ['view_users.purchases_total_count', 'view_users.applications_accepted_count'],
    dimensions: ['view_users.user_id'],
    order: [{ id: 'view_users.user_id' }],
  }, [
    { view_users__user_id: 1, view_users__purchases_total_count: '2', view_users__applications_accepted_count: '1' },
    { view_users__user_id: 2, view_users__purchases_total_count: '0', view_users__applications_accepted_count: '0' },
  ], { joinGraph, cubeEvaluator, compiler }));

  // Repro: exact query from the issue (+ a tie-breaker for deterministic output).
  it('view: multi-fact measures ordered by a non-selected measure', async () => dbRunner.runQueryTest({
    measures: ['view_users.purchases_total_count', 'view_users.applications_accepted_count'],
    dimensions: ['view_users.user_id'],
    order: [{ id: 'view_users.applications_total_count', desc: true }, { id: 'view_users.user_id' }],
  }, [
    { view_users__user_id: 1, view_users__purchases_total_count: '2', view_users__applications_accepted_count: '1' },
    { view_users__user_id: 2, view_users__purchases_total_count: '0', view_users__applications_accepted_count: '0' },
  ], { joinGraph, cubeEvaluator, compiler }));

  // Same failure without views: order by a measure of the other fact cube.
  // Tesseract-only: the legacy planner drops the order item and roots the join
  // at dev_purchases, so it never returns user 2.
  (getEnv('nativeSqlPlanner') ? it : it.skip)('cubes: order by a non-selected measure of another fact cube', async () => dbRunner.runQueryTest({
    measures: ['dev_purchases.total_count'],
    dimensions: ['dev_users.user_id'],
    order: [{ id: 'dev_applications.total_count', desc: true }, { id: 'dev_users.user_id' }],
  }, [
    { dev_users__user_id: 1, dev_purchases__total_count: '2' },
    { dev_users__user_id: 2, dev_purchases__total_count: '0' },
  ], { joinGraph, cubeEvaluator, compiler }));
});
