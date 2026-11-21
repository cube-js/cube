import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from './PrepareCompiler';

// The native planner reads a cube definition through its own properties only,
// so scalar parameters a cube inherits through `extends` must be own properties
// of the extending cube.
describe('Scalar cube parameters inherited through extends', () => {
  const model = `
cubes:
  - name: parent_cube
    sql_alias: pa
    sql: "SELECT 1 AS id, 5 AS amt"
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
    measures:
      - name: total
        sql: amt
        type: sum

  - name: child_cube
    extends: parent_cube

  - name: aliased_child_cube
    extends: parent_cube
    sql_alias: ac

  - name: parent_calendar
    calendar: true
    sql: "SELECT '2026-01-01'::date AS d"
    dimensions:
      - name: d
        sql: d
        type: time
        primary_key: true

  - name: child_calendar
    extends: parent_calendar
`;

  const compile = async () => {
    const compilers = prepareYamlCompiler(model);
    await compilers.compiler.compile();
    return compilers;
  };

  describe.each([
    ['native', true],
    ['legacy', false],
  ])('%s planner', (_name, useNativeSqlPlanner) => {
    const buildSql = async (measure: string) => {
      const compilers = await compile();
      const [sql] = new PostgresQuery(compilers, {
        timezone: 'UTC',
        useNativeSqlPlanner,
        measures: [measure],
      }).buildSqlAndParams();
      return sql;
    };

    it('aliases the extending cube with the inherited sql_alias', async () => {
      const sql = await buildSql('child_cube.total');
      expect(sql).toContain('AS "pa"');
      expect(sql).not.toContain('"child_cube"');
    });

    it('aliases the extending cube with its own sql_alias', async () => {
      const sql = await buildSql('aliased_child_cube.total');
      expect(sql).toContain('AS "ac"');
      expect(sql).not.toContain('"pa"');
    });
  });

  it('exposes inherited scalar parameters as own properties', async () => {
    const { cubeEvaluator } = await compile();

    const childCube: Record<string, unknown> = cubeEvaluator.cubeFromPath('child_cube');
    expect(Object.getOwnPropertyNames(childCube)).toContain('sqlAlias');
    expect(childCube.sqlAlias).toEqual('pa');

    const childCalendar: Record<string, unknown> = cubeEvaluator.cubeFromPath('child_calendar');
    expect(Object.getOwnPropertyNames(childCalendar)).toContain('calendar');
    expect(childCalendar.calendar).toEqual(true);
  });
});
