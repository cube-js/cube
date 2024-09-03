import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

describe('View Row Deduplication', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
cubes:
  - name: cube1
    sql: >
      SELECT 'a' AS date, 1 AS order_id UNION ALL
      SELECT 'a' AS date, 2 AS order_id UNION ALL
      SELECT 'b' AS date, 3 AS order_id UNION ALL
      SELECT 'b' AS date, 4 AS order_id

    dimensions:
      - name: date
        sql: date
        type: string
      - name: order_id
        sql: order_id
        type: number
        primary_key: true
    measures:
      - name: count_cube1
        type: count
      - name: sum_cube1
        type: sum
        sql: order_id

  - name: cube2
    sql: >
      SELECT 'a' AS date, 5 AS order_id UNION ALL
      SELECT 'b' AS date, 6 AS order_id

    dimensions:
      - name: date
        sql: date
        type: string
      - name: order_id
        sql: order_id
        type: number
        primary_key: true
    measures:
      - name: count_cube2
        type: count
      - name: sum_cube2
        type: sum
        sql: order_id

  - name: date_cube
    sql: >
      SELECT 'a' AS date UNION ALL
      SELECT 'b' AS date
    joins:
      - name: cube1
        sql: "{date_cube.date}={cube1.date}"
        relationship: one_to_many
      - name: cube2
        sql: "{date_cube.date}={cube2.date}"
        relationship: one_to_many
    dimensions:
      - name: date
        sql: date
        type: string
views:
  - name: test
    cubes:
      - join_path: date_cube
        includes:
          - date
      - join_path: date_cube.cube1
        includes:
          - sum_cube1
      - join_path: date_cube.cube2
        includes:
          - sum_cube2
    `);

  it('works if called in view', async () => dbRunner.runQueryTest({
    measures: ['test.sum_cube1', 'test.sum_cube2'],
    dimensions: [
      'test.date'
    ],
    order: [{ id: 'test.date' }]
  }, [{
    test__date: 'a',
    test__sum_cube1: '3',
    test__sum_cube2: '5',
  }, {
    test__date: 'b',
    test__sum_cube1: '7',
    test__sum_cube2: '6',
  }], { joinGraph, cubeEvaluator, compiler }));
});
