import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from './PrepareCompiler';
import { PreAggregations } from '../../src/adapter/PreAggregations';

// A view listing two cubes that DO join to each other, but as two independent
// `join_path` roots rather than one dotted into the other. `locations` is the
// fan-out side - 2 rows per `boards` row, `many_to_one` into it - and `boards`
// carries a calculated ratio measure over two sums, plus a rollup covering
// them.
//
// What is pinned here is which root a query gets planned against. A query whose
// members all sit under the `boards` root must be planned as a single-cube query
// against `boards`: the leaf measures resolve to plain `boards.*` paths, nothing
// is multiplied, and the rollup on `boards` alone is usable. The fan-out cube is
// only pulled in when the query itself reaches for it, or when the view declares
// `boards` as reachable *through* `locations` instead of on its own root.
const cubes = `
cubes:
  - name: locations
    sql: >
      SELECT 1 AS id, 'A' AS board_id, 'x' AS pos UNION ALL
      SELECT 2 AS id, 'A' AS board_id, 'y' AS pos UNION ALL
      SELECT 3 AS id, 'B' AS board_id, 'x' AS pos UNION ALL
      SELECT 4 AS id, 'B' AS board_id, 'y' AS pos
    joins:
      - name: boards
        sql: "{CUBE}.board_id = {boards}.board_id"
        relationship: many_to_one
    dimensions:
      - name: id
        sql: "{CUBE}.id"
        type: number
        primary_key: true
      - name: board_id
        sql: "{CUBE}.board_id"
        type: string
      - name: pos
        sql: "{CUBE}.pos"
        type: string
    measures:
      - name: count
        type: count

  - name: boards
    sql: >
      SELECT 'A' AS board_id, 1 AS good, 2 AS total, '2026-01-01'::timestamp AS created_at UNION ALL
      SELECT 'B' AS board_id, 2 AS good, 2 AS total, '2026-01-02'::timestamp
    dimensions:
      - name: board_id
        sql: "{CUBE}.board_id"
        type: string
        primary_key: true
      - name: created_at
        sql: "{CUBE}.created_at"
        type: time
    measures:
      - name: good_count
        sql: "{CUBE}.good"
        type: sum
      - name: total_count
        sql: "{CUBE}.total"
        type: sum
      - name: yield_pct
        sql: "{good_count} / NULLIF({total_count}, 0) * 100"
        type: number
    pre_aggregations:
      - name: daily
        type: rollup
        measures:
          - good_count
          - total_count
        dimensions:
          - board_id
        time_dimension: created_at
        granularity: day

views:
  # The two roots are independent - \`boards\` is not dotted through
  # \`locations\`.
  - name: kpi
    cubes:
      - join_path: locations
        includes: [count, pos]
      - join_path: boards
        includes: [yield_pct, board_id, created_at]

  # Same members, but \`boards\` is declared as reachable through
  # \`locations\`, so the fan-out is part of what the view asks for.
  - name: kpi_nested
    cubes:
      - join_path: locations
        includes: [count, pos]
      - join_path: locations.boards
        includes: [yield_pct, board_id, created_at]

  # The second root on its own, as a baseline for "no other cube in the view".
  - name: kpi_boards_only
    cubes:
      - join_path: boards
        includes: [yield_pct, board_id, created_at]
`;

// The rollup on `boards`, as the matcher sees it.
const boardsDailyRefs: any = {
  dimensions: ['boards.board_id'],
  measures: ['boards.good_count', 'boards.total_count'],
  timeDimensions: [{ dimension: 'boards.created_at', granularity: 'day' }],
  rollups: [],
  rollupsReferences: [],
};

let compilers: any;

beforeAll(async () => {
  compilers = prepareYamlCompiler(cubes);
  await compilers.compiler.compile();
});

describe.each([
  ['tesseract', true],
  ['legacy', false],
])('Calculated measure under an independent view root (%s planner)', (_name, useNativeSqlPlanner) => {
  const plan = (query: any) => {
    const q = new PostgresQuery(compilers, {
      timezone: 'UTC',
      useNativeSqlPlanner,
      ...query,
    });
    const transformed: any = PreAggregations.transformQueryToCanUseForm(q as any);

    return {
      transformed,
      match: PreAggregations.canUsePreAggregationForTransformedQueryFn(transformed, boardsDailyRefs) as any,
      sql: q.buildSqlAndParams()[0],
    };
  };

  it('plans the ratio against the independent root alone', () => {
    const { transformed, match, sql } = plan({
      measures: ['kpi.yield_pct'],
      dimensions: ['kpi.board_id'],
    });

    // Not the nested `locations.boards.*` form - the view declares `boards` on
    // its own root, and the query never leaves it.
    expect(transformed.leafMeasuresFullPaths.slice().sort())
      .toEqual(['boards.good_count', 'boards.total_count']);
    expect(transformed.hasMultipliedMeasures).toBe(false);
    // So the rollup on `boards` alone matches, and no fan-out keys subquery is
    // needed to evaluate the ratio.
    expect(match.canUse).toBe(true);
    expect(sql).toContain('stb_pre_aggregations.boards_daily');
    expect(sql).not.toContain('locations');
    expect(sql).not.toContain('SELECT DISTINCT');
  });

  it('plans the same way with a time dimension from the independent root', () => {
    const { transformed, match, sql } = plan({
      measures: ['kpi.yield_pct'],
      dimensions: ['kpi.board_id'],
      timeDimensions: [{ dimension: 'kpi.created_at', granularity: 'day' }],
    });

    expect(transformed.hasMultipliedMeasures).toBe(false);
    expect(match.canUse).toBe(true);
    expect(sql).toContain('stb_pre_aggregations.boards_daily');
    expect(sql).not.toContain('locations');
  });

  it('plans the same way as a view holding that root on its own', () => {
    const throughBothRoots = plan({
      measures: ['kpi.yield_pct'],
      dimensions: ['kpi.board_id'],
    });
    const throughOneRoot = plan({
      measures: ['kpi_boards_only.yield_pct'],
      dimensions: ['kpi_boards_only.board_id'],
    });

    // Dropping the fan-out cube from the view changes nothing about how the
    // ratio is planned; only the emitted view aliases differ.
    expect(throughBothRoots.transformed.leafMeasuresFullPaths)
      .toEqual(throughOneRoot.transformed.leafMeasuresFullPaths);
    expect(throughBothRoots.transformed.hasMultipliedMeasures)
      .toEqual(throughOneRoot.transformed.hasMultipliedMeasures);
    expect(throughBothRoots.sql.replace(/kpi__/g, '__'))
      .toEqual(throughOneRoot.sql.replace(/kpi_boards_only__/g, '__'));
  });

  // The other side of the same coin, pinned so the two shapes stay
  // distinguishable: declaring `boards` through `locations` is what puts the
  // fan-out back into the plan.
  it('routes through the fan-out cube when the view nests the join path', () => {
    const { transformed, match } = plan({
      measures: ['kpi_nested.yield_pct'],
      dimensions: ['kpi_nested.board_id'],
    });

    expect(transformed.leafMeasuresFullPaths.slice().sort())
      .toEqual(['locations.boards.good_count', 'locations.boards.total_count']);
    expect(match.canUse).toBe(false);
  });

  // And so does asking for a member of the other root in the same query, which
  // genuinely spans both cubes.
  it('routes through the fan-out cube when the query reaches the other root', () => {
    const { transformed, match, sql } = plan({
      measures: ['kpi.yield_pct'],
      dimensions: ['kpi.board_id', 'kpi.pos'],
    });

    expect(transformed.leafMeasuresFullPaths.slice().sort())
      .toEqual(['locations.boards.good_count', 'locations.boards.total_count']);
    expect(transformed.hasMultipliedMeasures).toBe(true);
    expect(match.canUse).toBe(false);
    // `boards` is on the `many_to_one` side, so the join multiplies its rows
    // and the sums have to be taken over deduplicated keys.
    expect(sql).toContain('SELECT DISTINCT');
  });
});
