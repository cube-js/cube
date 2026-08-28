import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { PreAggregations } from '../../src/adapter/PreAggregations';
import { prepareYamlCompiler } from './PrepareCompiler';

// Reproduction for https://github.com/cube-js/cube/issues/11680
//
// `locations` fans out over `boards` (2 location rows per board), and `boards`
// carries a ratio measure plus a rollup that covers it. A query that touches
// only members declared under the view's independent `boards` root should be
// planned against `boards` alone - `locations` is never referenced by it.
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
        relationship: many_to_one
        sql: "{CUBE}.board_id = {boards}.board_id"
    measures:
      - name: count
        type: count
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

  - name: boards
    sql: >
      SELECT 'A' AS board_id, 'p1' AS product, 1 AS good, 2 AS total UNION ALL
      SELECT 'B' AS board_id, 'p2' AS product, 2 AS good, 2 AS total
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
    dimensions:
      - name: board_id
        sql: "{CUBE}.board_id"
        type: string
        primary_key: true
      - name: product
        sql: "{CUBE}.product"
        type: string
    pre_aggregations:
      # A rollup on \`boards\` alone. It carries one dimension more than the
      # query asks for, which only the additive matching path allows. Once the
      # query is considered to have multiplied measures, matching switches to
      # the strict path that requires exact dimension equality, and this rollup
      # stops being usable.
      - name: daily
        type: rollup
        measures:
          - good_count
          - total_count
        dimensions:
          - board_id
          - product
`;

// Two independent roots, no dotted paths - the schema shape from the issue.
const independentRootsOnly = `${cubes}
views:
  - name: kpi
    cubes:
      - join_path: locations
        includes:
          - count
          - pos
      - join_path: boards
        includes:
          - yield_pct
          - board_id
`;

// Same independent \`boards\` root, but the view additionally declares a dotted
// path that walks locations -> boards for an unrelated member.
const withDottedPath = `${cubes}
views:
  - name: kpi
    cubes:
      - join_path: locations
        includes:
          - count
          - pos
      - join_path: locations.boards
        includes:
          - name: product
            alias: loc_product
      - join_path: boards
        includes:
          - yield_pct
          - board_id
`;

const viewQuery = { measures: ['kpi.yield_pct'], dimensions: ['kpi.board_id'], timezone: 'UTC' };

async function compile(model: string, queryDef: any) {
  const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(model);
  await compiler.compile();
  const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, queryDef);
  return { query, transformed: PreAggregations.transformQueryToCanUseForm(query) as any };
}

describe('View with independent join_path roots', () => {
  // Baseline: querying the cube directly, without any view.
  it('matches the rollup when boards is queried directly', async () => {
    const { query, transformed } = await compile(independentRootsOnly, {
      measures: ['boards.yield_pct'],
      dimensions: ['boards.board_id'],
      timezone: 'UTC',
    });

    expect(transformed.leafMeasuresFullPaths).toEqual(['boards.good_count', 'boards.total_count']);
    expect(transformed.hasMultipliedMeasures).toBe(false);
    expect(query.buildSqlAndParams()[0]).toContain('boards_daily');
  });

  it('matches the rollup through a view with independent roots', async () => {
    const { query, transformed } = await compile(independentRootsOnly, viewQuery);

    expect(transformed.leafMeasuresFullPaths).toEqual(['boards.good_count', 'boards.total_count']);
    expect(transformed.hasMultipliedMeasures).toBe(false);
    expect(query.buildSqlAndParams()[0]).toContain('boards_daily');
  });

  // Adding a dotted `locations.boards` entry to the same view makes
  // `enrichHintsWithJoinMap` rewrite the join hint of the independent `boards`
  // root into `locations.boards`. The query is then planned as if it needed the
  // fan-out join: leaf measures resolve to the dotted form, the query is marked
  // as having multiplied measures, and the rollup on `boards` is rejected in
  // favour of a `SELECT DISTINCT ... FROM locations LEFT JOIN boards` plan -
  // even though the query never references `locations`.
  it('matches the rollup through a view that also declares a dotted path', async () => {
    const { query, transformed } = await compile(withDottedPath, viewQuery);

    expect(transformed.leafMeasuresFullPaths).toEqual(['boards.good_count', 'boards.total_count']);
    expect(transformed.hasMultipliedMeasures).toBe(false);
    expect(query.buildSqlAndParams()[0]).toContain('boards_daily');
  });
});
