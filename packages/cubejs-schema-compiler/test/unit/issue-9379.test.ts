// https://github.com/cube-js/cube/issues/9379
// SQL API: `CASE ... THEN DATE_TRUNC('week', cube.time) ... END` together with
// MEASURE() fails on MySQL with "Can't detect Cube query", while the same query
// works on Postgres / ClickHouse. cubesql only pushes a scalar function down
// into the generated SQL when the data source dialect exposes a
// `functions/<NAME>` template (see rust/cubesql/cubesql/src/compile/rewrite/
// rules/wrapper/scalar_function.rs). MysqlQuery does not define
// `functions.DATETRUNC`, so any DATE_TRUNC nested in an expression cannot be
// planned for MySQL.
import { MysqlQuery } from '../../src/adapter/MysqlQuery';
import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from './PrepareCompiler';

describe('issue #9379: MySQL SQL templates must support DATE_TRUNC pushdown', () => {
  const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
cubes:
  - name: test_sales
    sql: "SELECT CAST('2025-01-01' AS DATETIME) AS order_date, 100 AS total_amount"
    dimensions:
      - name: order_date
        sql: order_date
        type: time
    measures:
      - name: total_sum
        sql: total_amount
        type: sum
`);

  const queryOptions = { measures: ['test_sales.total_sum'], timezone: 'UTC' };

  it('Postgres exposes a DATETRUNC function template (control)', async () => {
    await compiler.compile();
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, queryOptions);
    expect(query.sqlTemplates().functions.DATETRUNC).toBeDefined();
  });

  it('MySQL exposes a DATETRUNC function template', async () => {
    await compiler.compile();
    const query = new MysqlQuery({ joinGraph, cubeEvaluator, compiler }, queryOptions);
    const template = query.sqlTemplates().functions.DATETRUNC;
    expect(template).toBeDefined();
    // MySQL / MariaDB have no DATE_TRUNC function, so the Postgres-style
    // default must not be inherited as-is.
    expect(template).not.toMatch(/^DATE_TRUNC\(/);
  });
});
