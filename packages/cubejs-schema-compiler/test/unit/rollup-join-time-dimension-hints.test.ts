import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from './PrepareCompiler';

// `rollup_join` builds its join tree from cube hints collected off the
// pre-aggregation's own references. Those hints have to cover every cube the
// join is meant to span, including a cube whose single reference is its
// `time_dimension` - otherwise the join tree covers one cube, `targetJoins`
// comes back empty and the pre-aggregation is rejected with "Nothing to join in
// rollup join".
//
// The shape has no workaround: naming a dimension of that cube would put the
// hint in, but then that dimension has to be requested by every query for the
// rollup to match at all.
//
// Both planners build the pre-aggregation description here, so both are covered.
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

const preAggregationSql = async (useNativeSqlPlanner: boolean, joinedPreAggregation: string, query: any) => {
  const compilers = prepareYamlCompiler(model(joinedPreAggregation));
  await compilers.compiler.compile();

  return new PostgresQuery(compilers, {
    timezone: 'UTC',
    useNativeSqlPlanner,
    ...query,
  })
    .preAggregations.preAggregationsDescription()
    .map((d: any) => d.loadSql?.[0] ?? '')
    .join('\n');
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

  it('builds the join when a cube is referenced only by time_dimension', async () => {
    const sql = await preAggregationSql(useNativeSqlPlanner, timeDimensionOnly, {
      measures: ['boards.good_count'],
      timeDimensions,
    });

    expect(sql).toContain('locations_locations_rollup');
    expect(sql).toContain('boards_boards_rollup');
  });

  it('builds the join when a dimension of that cube is referenced too', async () => {
    const sql = await preAggregationSql(useNativeSqlPlanner, withAnchorDimension, {
      measures: ['boards.good_count'],
      dimensions: ['locations.board_id'],
      timeDimensions,
    });

    expect(sql).toContain('locations_locations_rollup');
    expect(sql).toContain('boards_boards_rollup');
  });
});
