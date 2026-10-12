// https://github.com/cube-js/cube/issues/1232
// MongoBI: filtering a boolean dimension sends the string 'true' instead of a boolean literal,
// which defeats the MongoDB index on that field.
import { format } from 'mysql2';
import { MongoBiQuery } from '../../src/adapter/MongoBiQuery';
import { prepareJsCompiler } from './PrepareCompiler';

describe('issue-1232: MongoBI boolean filter value', () => {
  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
    cube(\`items\`, {
      sql: \`select * from items\`,
      measures: {
        count: { type: 'count' }
      },
      dimensions: {
        id: { type: 'number', sql: 'id', primaryKey: true },
        myBoolean: { type: 'boolean', sql: 'my_boolean' }
      }
    });
  `);

  it.each(['notEquals', 'equals'])('%s filter on boolean dimension is not compared with a string', async (operator) => {
    await compiler.compile();
    const query = new MongoBiQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['items.count'],
      filters: [{ member: 'items.myBoolean', operator, values: ['true'] }],
    });
    const [sql, params] = query.buildSqlAndParams();
    // The MongoBI driver executes via mysql2 `query(sql, values)`, which formats params client side.
    const rendered = format(sql, params as any[]);
    console.log(rendered);
    expect(rendered).not.toMatch(/'true'/);
  });
});
