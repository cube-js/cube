import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// A cube's own primary key as a query dimension, next to measures from two
// cubes. The measures split into per-cube subqueries, and the one on the
// `one` side of the join is multiplied by the fan-out, so it is read through
// the keys subquery and re-joined to its own cube by that same primary key.
// The key plays two roles at once - query dimension and re-join key - and has
// to be projected once: two columns under one alias make the re-join's
// reference to it ambiguous.
describe('Primary key dimension on the multi-fact path', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
cubes:
  - name: cube_a
    sql_alias: a
    sql: >
      SELECT 1 AS id, 100 AS value_a UNION ALL
      SELECT 2 AS id, 200 AS value_a
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
        public: true
    measures:
      - name: measure_a
        sql: value_a
        type: sum

  - name: cube_b
    sql_alias: b
    sql: >
      SELECT 10 AS id, 1 AS a_id, '2026-07-05'::timestamp AS date, 5 AS value_b UNION ALL
      SELECT 11 AS id, 1 AS a_id, '2026-07-10'::timestamp AS date, 7 AS value_b UNION ALL
      SELECT 12 AS id, 2 AS a_id, '2026-07-15'::timestamp AS date, 9 AS value_b
    joins:
      - name: cube_a
        relationship: many_to_one
        sql: "{CUBE.a_id} = {cube_a.id}"
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: a_id
        sql: a_id
        type: number
      - name: date
        sql: date
        type: time
    measures:
      - name: measure_b
        sql: value_b
        type: sum
    `);

  async function runQuery(q) {
    await compiler.compile();
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, q);
    return dbRunner.testQuery(query.buildSqlAndParams());
  }

  it('primary key dimension next to measures from two cubes', async () => {
    // measure_a must be counted once per cube_a row despite the two cube_b
    // rows that share a_id = 1.
    expect(await runQuery({
      measures: ['cube_b.measure_b', 'cube_a.measure_a'],
      dimensions: ['cube_a.id'],
      timeDimensions: [{
        dimension: 'cube_b.date',
        granularity: 'month',
        dateRange: ['2026-07-01', '2026-07-31'],
      }],
      order: [{ id: 'cube_a.id' }],
      timezone: 'UTC',
    })).toEqual([
      {
        a__id: 1,
        b__date_month: '2026-07-01T00:00:00.000Z',
        b__measure_b: '12',
        a__measure_a: '100',
      },
      {
        a__id: 2,
        b__date_month: '2026-07-01T00:00:00.000Z',
        b__measure_b: '9',
        a__measure_a: '200',
      },
    ]);
  });
});
