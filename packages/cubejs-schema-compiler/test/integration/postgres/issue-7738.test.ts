import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// https://github.com/cube-js/cube/issues/7738
//
// teams -> user_teams (hasMany) -> users (belongsTo) -> phone_calls (hasMany)
// makes `teams.name` a fan-out dimension for `phone_calls` measures, so the
// planner deduplicates them through the keys subquery. A calculated measure
// (`type: number` over `{interested_count}` / `{total_count}`) whose component
// reaches another cube (`phone_call_dispositions`, via a filter) cannot be
// evaluated in that mode:
// - legacy planner: renders invalid SQL (the aggregates around the components
//   are dropped, `phone_call_id` ends up in arithmetic -> "cannot cast type
//   uuid to numeric" / "uuid = integer" as in the issue);
// - Tesseract: refuses with "has no aggregate of its own, so it cannot be
//   re-aggregated over the deduplicated rows this query needs", even though
//   both components are regular `count` measures that can be queried together
//   under the same dimension just fine.
// Reproduced end to end on Cube v1.7.49 (Tesseract and legacy planner, Postgres 16).
describe('Issue 7738: calculated measure reaching a joined cube under a many-to-many fan-out', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
cubes:
  - name: users
    sql: >
      SELECT 'u1' AS user_key, 'a@x' AS email UNION ALL
      SELECT 'u2' AS user_key, 'b@x' AS email UNION ALL
      SELECT 'u3' AS user_key, 'c@x' AS email
    dimensions:
      - name: user_key
        sql: user_key
        type: string
        primary_key: true
      - name: email
        sql: email
        type: string
    joins:
      - name: phone_calls
        relationship: one_to_many
        sql: "{CUBE}.user_key = {phone_calls.user_key}"

  - name: user_teams
    sql: >
      SELECT 1 AS user_team_id, 'u1' AS user_key, 't1' AS team_key UNION ALL
      SELECT 2 AS user_team_id, 'u2' AS user_key, 't1' AS team_key UNION ALL
      SELECT 3 AS user_team_id, 'u2' AS user_key, 't2' AS team_key
    dimensions:
      - name: user_team_id
        sql: user_team_id
        type: number
        primary_key: true
      - name: team_key
        sql: team_key
        type: string
    joins:
      - name: users
        relationship: many_to_one
        sql: "{CUBE}.user_key = {users.user_key}"

  - name: teams
    sql: >
      SELECT 't1' AS team_key, 'red' AS name UNION ALL
      SELECT 't2' AS team_key, 'blue' AS name UNION ALL
      SELECT 't3' AS team_key, 'empty' AS name
    dimensions:
      - name: team_key
        sql: team_key
        type: string
        primary_key: true
      - name: name
        sql: name
        type: string
    joins:
      - name: user_teams
        relationship: one_to_many
        sql: "{CUBE}.team_key = {user_teams.team_key}"

  - name: phone_calls
    sql: >
      SELECT '00000000-0000-0000-0000-000000000001'::uuid AS phone_call_id, 'u1' AS user_key, 'd1' AS disposition_key UNION ALL
      SELECT '00000000-0000-0000-0000-000000000002'::uuid AS phone_call_id, 'u1' AS user_key, 'd2' AS disposition_key UNION ALL
      SELECT '00000000-0000-0000-0000-000000000003'::uuid AS phone_call_id, 'u2' AS user_key, 'd1' AS disposition_key UNION ALL
      SELECT '00000000-0000-0000-0000-000000000004'::uuid AS phone_call_id, 'u3' AS user_key, 'd2' AS disposition_key
    dimensions:
      - name: phone_call_id
        sql: phone_call_id
        type: string
        primary_key: true
      - name: user_key
        sql: user_key
        type: string
    measures:
      - name: total_count
        type: count
        sql: phone_call_id
      - name: interested_count
        type: count
        filters:
          - sql: "{phone_call_dispositions.label} = 'Interested'"
      - name: interested_rate
        type: number
        sql: "ROUND({interested_count}::decimal / NULLIF({total_count}, 0) * 100, 2)"
    joins:
      - name: phone_call_dispositions
        relationship: many_to_one
        sql: "{CUBE}.disposition_key = {phone_call_dispositions.disposition_key}"

  - name: phone_call_dispositions
    sql: >
      SELECT 'd1' AS disposition_key, 'Interested' AS label UNION ALL
      SELECT 'd2' AS disposition_key, 'No' AS label
    dimensions:
      - name: disposition_key
        sql: disposition_key
        type: string
        primary_key: true
      - name: label
        sql: label
        type: string
`);

  async function runQuery(q) {
    await compiler.compile();
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, q);
    return dbRunner.testQuery(query.buildSqlAndParams());
  }

  it('components of the calculated measure, grouped by the many-to-many dimension', async () => {
    expect(await runQuery({
      measures: ['phone_calls.total_count', 'phone_calls.interested_count'],
      dimensions: ['teams.name'],
      order: [{ id: 'teams.name' }],
    })).toEqual([
      { teams__name: 'blue', phone_calls__total_count: '1', phone_calls__interested_count: '1' },
      { teams__name: 'empty', phone_calls__total_count: '0', phone_calls__interested_count: '0' },
      { teams__name: 'red', phone_calls__total_count: '3', phone_calls__interested_count: '2' },
    ]);
  });

  it('calculated measure reaching a joined cube, grouped by the many-to-many dimension', async () => {
    expect(await runQuery({
      measures: ['phone_calls.total_count', 'phone_calls.interested_rate'],
      dimensions: ['teams.name'],
      order: [{ id: 'teams.name' }],
    })).toEqual([
      { teams__name: 'blue', phone_calls__total_count: '1', phone_calls__interested_rate: '100.00' },
      { teams__name: 'empty', phone_calls__total_count: '0', phone_calls__interested_rate: null },
      { teams__name: 'red', phone_calls__total_count: '3', phone_calls__interested_rate: '66.67' },
    ]);
  });

  it('calculated measure reaching a joined cube, without a fan-out dimension', async () => {
    expect(await runQuery({
      measures: ['phone_calls.interested_rate'],
      dimensions: ['users.email'],
      order: [{ id: 'users.email' }],
    })).toEqual([
      { users__email: 'a@x', phone_calls__interested_rate: '50.00' },
      { users__email: 'b@x', phone_calls__interested_rate: '100.00' },
      { users__email: 'c@x', phone_calls__interested_rate: '0.00' },
    ]);
  });
});
