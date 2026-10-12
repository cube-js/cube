import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// https://github.com/cube-js/cube/issues/7738
// teams -< user_teams >- users -< phone_calls >- phone_call_dispositions.
// Grouping phone call measures by `teams.name` fans out phone_calls, so the
// measures go through the keys (deduplication) subquery. A calculated measure
// (`type: number`) whose component reaches the joined `phone_call_dispositions`
// cube is then rejected by Tesseract ("has no aggregate of its own, so it cannot
// be re-aggregated...") even though every aggregation lives in a `type: count`
// measure, as the error message itself recommends. Its components work alone in
// the same query. The legacy planner renders invalid SQL for the same query.
describe('Issue #7738: calculated measure across a many-to-many join', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
cubes:
  - name: users
    sql: >
      SELECT * FROM (VALUES (1, 'a@x'), (2, 'b@x')) AS t(user_key, email)
    dimensions:
      - name: user_key
        sql: user_key
        type: number
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
      SELECT * FROM (VALUES (1, 1, 't1'), (2, 2, 't1'), (3, 1, 't2')) AS t(user_team_id, user_key, team_key)
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
      SELECT * FROM (VALUES ('t1', 'Team1'), ('t2', 'Team2')) AS t(team_key, name)
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
      SELECT * FROM (VALUES (1, 1, 'd1'), (2, 1, 'd2'), (3, 2, 'd1')) AS t(phone_call_id, user_key, phone_call_disposition_key)
    dimensions:
      - name: phone_call_id
        sql: phone_call_id
        type: number
        primary_key: true
      - name: user_key
        sql: user_key
        type: number
    measures:
      - name: total_count
        type: count
      - name: interested_count
        type: count
        filters:
          - sql: "{phone_call_dispositions.label} = 'Interested'"
      - name: interested_rate
        type: number
        sql: "100.0 * {interested_count} / NULLIF({total_count}, 0)"
    joins:
      - name: phone_call_dispositions
        relationship: many_to_one
        sql: "{CUBE}.phone_call_disposition_key = {phone_call_dispositions.phone_call_disposition_key}"

  - name: phone_call_dispositions
    sql: >
      SELECT * FROM (VALUES ('d1', 'Interested'), ('d2', 'No')) AS t(phone_call_disposition_key, label)
    dimensions:
      - name: phone_call_disposition_key
        sql: phone_call_disposition_key
        type: string
        primary_key: true
      - name: label
        sql: label
        type: string
`);

  const run = async (measures: string[]) => {
    await compiler.compile();
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures,
      dimensions: ['teams.name'],
      order: [{ id: 'teams.name' }],
      timezone: 'UTC',
    });
    return dbRunner.testQuery(query.buildSqlAndParams());
  };

  // Control: the component measures work by the fan-out dimension.
  it('component measures by teams.name', async () => {
    expect(await run(['phone_calls.total_count', 'phone_calls.interested_count'])).toEqual([
      { teams__name: 'Team1', phone_calls__total_count: '3', phone_calls__interested_count: '2' },
      { teams__name: 'Team2', phone_calls__total_count: '2', phone_calls__interested_count: '1' },
    ]);
  });

  it('calculated measure over those components by teams.name', async () => {
    const res = await run(['phone_calls.interested_rate']);
    expect(res.map((r: any) => [r.teams__name, Number(r.phone_calls__interested_rate).toFixed(2)])).toEqual([
      ['Team1', '66.67'],
      ['Team2', '50.00'],
    ]);
  });
});
