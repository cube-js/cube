// https://github.com/cube-js/cube/issues/7392
// MSSQL: a non-zero `offset` without an explicit order renders `OFFSET n ROWS FETCH NEXT ...`
// with no ORDER BY, which T-SQL rejects ("Incorrect syntax near 'OFFSET'").
// With the Tesseract planner (default) the same query instead drops the offset silently
// (`SELECT TOP 10000 ...`), returning unskipped rows.
import { MssqlQuery } from '../../src/adapter/MssqlQuery';
import { prepareJsCompiler } from './PrepareCompiler';

describe('issue-7392: MSSQL offset without order', () => {
  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
    cube(\`transactions\`, {
      sql: \`select * from analytics_learn.transactions\`,
      measures: {
        count: { type: 'count', sql: 'transaction_id' }
      },
      dimensions: {
        id: { type: 'number', sql: 'transaction_id', primaryKey: true },
        status: { type: 'string', sql: 'status' }
      }
    });
  `);

  it('measures-only query with offset puts ORDER BY before OFFSET', async () => {
    await compiler.compile();
    // The API gateway always sends rowLimit (default 10000)
    const query = new MssqlQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['transactions.count'],
      rowLimit: 10000,
      offset: 1,
    });
    const [sql] = query.buildSqlAndParams();
    console.log(sql);
    expect(sql).toMatch(/OFFSET 1 ROWS/);
    // T-SQL: OFFSET/FETCH is only valid as part of an ORDER BY clause
    expect(sql).toMatch(/ORDER BY[\s\S]*OFFSET 1 ROWS/);
  });

  it('grouped query with explicit empty order and offset puts ORDER BY before OFFSET', async () => {
    await compiler.compile();
    const query = new MssqlQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['transactions.count'],
      dimensions: ['transactions.status'],
      order: [],
      rowLimit: 10000,
      offset: 1,
    });
    const [sql] = query.buildSqlAndParams();
    console.log(sql);
    expect(sql).toMatch(/OFFSET 1 ROWS/);
    expect(sql).toMatch(/ORDER BY[\s\S]*OFFSET 1 ROWS/);
  });
});
