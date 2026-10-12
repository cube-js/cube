import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// https://github.com/cube-js/cube/issues/7803
// Measures from two one_to_many-joined fact cubes (a multi-fact query), ordered by
// a measure that is not selected. Tesseract renders the non-selected order-by
// measure as its raw expression (`count("dev_applications".application_id)`) in
// the outer ORDER BY, where that cube alias is not in scope:
// `missing FROM-clause entry for table "dev_applications"`.
// Combining the filtered measure with the other fact's measure (the original
// title) now works; the unselected order-by measure is what still breaks.
describe('Issue #7803: multi-fact query ordered by a non-selected measure', () => {
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

  const run = async (order: { id: string, desc?: boolean }[]) => {
    await compiler.compile();
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['view_users.purchases_total_count', 'view_users.applications_accepted_count'],
      dimensions: ['view_users.user_id'],
      order,
      limit: 5000,
      timezone: 'UTC',
    });
    return dbRunner.testQuery(query.buildSqlAndParams());
  };

  const expected = [
    { view_users__user_id: 1, view_users__purchases_total_count: '2', view_users__applications_accepted_count: '1' },
    { view_users__user_id: 2, view_users__purchases_total_count: '0', view_users__applications_accepted_count: '0' },
  ];

  // Control: the same measures (filtered measure + other fact's measure) work.
  it('ordered by a selected dimension', async () => {
    expect(await run([{ id: 'view_users.user_id' }])).toEqual(expected);
  });

  // The issue's query, plus a tie-breaker for a deterministic order.
  it('ordered by a non-selected measure', async () => {
    expect(await run([
      { id: 'view_users.applications_total_count', desc: true },
      { id: 'view_users.user_id' },
    ])).toEqual(expected);
  });
});
