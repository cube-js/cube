import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from './PrepareCompiler';

// `rollup_join` builds its join tree from cube hints collected off the
// pre-aggregation's own references, so those hints have to cover every cube the
// join spans - including a cube whose single reference is its `time_dimension`.
const model = (joinedPreAggregation: string) => `
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
${joinedPreAggregation}
`;

// Reaches `locations` only through its time dimension.
const timeDimensionOnly = `
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

// Same, plus a dimension from `locations`.
const withAnchorDimension = `
      - name: joined
        type: rollup_join
        rollups:
          - locations.locations_rollup
          - boards.boards_rollup
        measures:
          - good_count
        dimensions:
          - locations.board_id
        time_dimension: locations.ts
        granularity: day
`;

// One description per leg rollup of the join, identified by table name.
const joinedRollupTables = async (useNativeSqlPlanner: boolean, joinedPreAggregation: string, query: any) => {
  const compilers = prepareYamlCompiler(model(joinedPreAggregation));
  await compilers.compiler.compile();

  const descriptions = new PostgresQuery(compilers, {
    timezone: 'UTC',
    useNativeSqlPlanner,
    ...query,
  }).preAggregations.preAggregationsDescription();

  return descriptions.map((d: any) => d.tableName).sort();
};

describe.each([
  ['legacy', false],
  ['tesseract', true],
])('rollup_join cube hints (%s planner)', (_name, useNativeSqlPlanner) => {
  const timeDimensions = [{
    dimension: 'locations.ts',
    granularity: 'day',
    dateRange: ['2026-01-01', '2026-01-31'],
  }];

  const bothRollups = [
    'stb_pre_aggregations.boards_boards_rollup',
    'stb_pre_aggregations.locations_locations_rollup',
  ];

  it('builds the join when a cube is referenced only by time_dimension', async () => {
    expect(await joinedRollupTables(useNativeSqlPlanner, timeDimensionOnly, {
      measures: ['boards.good_count'],
      timeDimensions,
    })).toEqual(bothRollups);
  });

  it('builds the join when a dimension of that cube is referenced too', async () => {
    expect(await joinedRollupTables(useNativeSqlPlanner, withAnchorDimension, {
      measures: ['boards.good_count'],
      dimensions: ['locations.board_id'],
      timeDimensions,
    })).toEqual(bothRollups);
  });
});

// The same hints also drive `existingJoins` - the joins a leg rollup already
// materialises, which are subtracted from the ones the join has to make. A leg
// storing a joined time dimension carries that one column, not the far cube's
// measures, so its join must not be treated as already made: the rollup_join
// below still has to reach `boards_rollup` for `good_count`.
const legWithJoinedTimeDimension = `
cubes:
  - name: locations
    sql: >
      SELECT 1 AS id, 'A' AS board_id
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
    pre_aggregations:
      - name: locations_rollup
        type: rollup
        dimensions:
          - board_id
        time_dimension: boards.bts
        granularity: day

  - name: boards
    sql: >
      SELECT 'A' AS board_id, 1 AS good, '2026-01-01'::timestamp AS bts
    dimensions:
      - name: board_id
        sql: "{CUBE}.board_id"
        type: string
        primary_key: true
      - name: bts
        sql: "{CUBE}.bts"
        type: time
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
        dimensions:
          - locations.board_id
        time_dimension: boards.bts
        granularity: day
`;

describe('rollup_join leg storing a joined time dimension', () => {
  // Only the legacy planner is pinned here: the native planner counts a leg's
  // time dimensions in `existingJoins` and rejects this shape.
  it('still joins to the rollup holding the measures', async () => {
    const compilers = prepareYamlCompiler(legWithJoinedTimeDimension);
    await compilers.compiler.compile();

    const tables = new PostgresQuery(compilers, {
      timezone: 'UTC',
      useNativeSqlPlanner: false,
      measures: ['boards.good_count'],
      dimensions: ['locations.board_id'],
      timeDimensions: [{
        dimension: 'boards.bts',
        granularity: 'day',
        dateRange: ['2026-01-01', '2026-01-31'],
      }],
    }).preAggregations.preAggregationsDescription().map((d: any) => d.tableName).sort();

    expect(tables).toEqual([
      'stb_pre_aggregations.boards_boards_rollup',
      'stb_pre_aggregations.locations_locations_rollup',
    ]);
  });
});
