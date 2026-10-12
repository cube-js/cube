// https://github.com/cube-js/cube/issues/3127
// Oracle: building a pre-aggregation inside the source database fails because the
// generated `CREATE TABLE <name> <select>` is missing the mandatory `AS` keyword.
// OracleQuery overrides `asSyntaxTable` to '' (Oracle forbids `AS` for table aliases),
// and BaseQuery#preAggregationLoadSql reuses that getter for the CTAS keyword.
import { OracleQuery } from '../../src/adapter/OracleQuery';
import { prepareJsCompiler } from './PrepareCompiler';

describe('issue-3127: Oracle pre-aggregation CTAS', () => {
  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
    cube(\`orders\`, {
      sql: \`select * from orders\`,
      measures: {
        count: { type: 'count' }
      },
      dimensions: {
        id: { type: 'number', sql: 'id', primaryKey: true },
        status: { type: 'string', sql: 'status' }
      },
      preAggregations: {
        main: {
          type: 'rollup',
          external: false,
          measures: [CUBE.count],
          dimensions: [CUBE.status]
        }
      }
    });
  `);

  it('pre-aggregation loadSql is a valid Oracle CREATE TABLE ... AS SELECT', async () => {
    await compiler.compile();
    const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['orders.count'],
      dimensions: ['orders.status'],
      preAggregationsSchema: 'dev_pre_aggregations',
    });
    const descriptions: any = query.preAggregations?.preAggregationsDescription();
    expect(descriptions.length).toBe(1);
    const loadSql = descriptions[0].loadSql[0] as string;
    console.log(loadSql);
    expect(loadSql).toMatch(/^CREATE TABLE\s+\S+\s+AS\s+SELECT/i);
  });
});
