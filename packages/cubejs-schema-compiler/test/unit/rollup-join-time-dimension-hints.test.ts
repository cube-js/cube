import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from './PrepareCompiler';

// A `rollup_join` whose only reference to one of its two cubes is that cube's
// `time_dimension`. The native planner resolves the join itself, so rendering
// the query must not resolve one of its own alongside it.
const model = `
cubes:
  - name: locations
    sql: >
      SELECT 1 AS id, 'A' AS board_id, '2026-01-01'::timestamp AS ts UNION ALL
      SELECT 2 AS id, 'A' AS board_id, '2026-01-02'::timestamp
    joins:
      - name: boards
        sql: "{CUBE.board_id} = {boards.board_id}"
        relationship: many_to_one
    dimensions:
      - name: id
        sql: "{CUBE}.id"
        type: number
        primary_key: true
      - name: board_id
        sql: "{CUBE}.board_id"
        type: string
      - name: ts
        sql: "{CUBE}.ts"
        type: time
    measures:
      - name: count
        type: count
    pre_aggregations:
      - name: locations_rollup
        type: rollup
        dimensions:
          - board_id
        time_dimension: ts
        granularity: day

  - name: boards
    sql: >
      SELECT 'A' AS board_id, 1 AS good
    dimensions:
      - name: board_id
        sql: "{CUBE}.board_id"
        type: string
        primary_key: true
    measures:
      - name: good_count
        sql: "{CUBE}.good"
        type: sum
    pre_aggregations:
      - name: boards_rollup
        type: rollup
        measures:
          - good_count
        dimensions:
          - board_id
      - name: joined
        type: rollup_join
        rollups:
          - locations.locations_rollup
          - boards.boards_rollup
        measures:
          - good_count
        time_dimension: locations.ts
        granularity: day
`;

describe('rollup_join reaching a cube only by its time dimension', () => {
  it('is served by both rollups', async () => {
    const compilers = prepareYamlCompiler(model);
    await compilers.compiler.compile();

    const [sql] = new PostgresQuery(compilers, {
      timezone: 'UTC',
      useNativeSqlPlanner: true,
      measures: ['boards.good_count'],
      timeDimensions: [{
        dimension: 'locations.ts',
        granularity: 'day',
        dateRange: ['2026-01-01', '2026-01-31'],
      }],
    }).buildSqlAndParams();

    const rollups = ['boards_boards_rollup', 'locations_locations_rollup'];

    expect(rollups.filter(t => sql.includes(t))).toEqual(rollups);
  });
});
