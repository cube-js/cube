import { MongoBiQuery } from '../../src/adapter/MongoBiQuery';
import { prepareJsCompiler } from './PrepareCompiler';

// https://github.com/cube-js/cube/issues/1232
// MongoDB BI Connector compares a boolean column with the string 'true' as false,
// so boolean filter values must be cast instead of being passed as a bare `?`.
describe('MongoBiQuery boolean filters (issue #1232)', () => {
  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
    cube(\`items\`, {
      sql: \`select * from items\`,

      measures: {
        count: {
          type: 'count'
        }
      },

      dimensions: {
        id: {
          type: 'number',
          sql: 'id',
          primaryKey: true
        },
        active: {
          type: 'boolean',
          sql: 'active'
        }
      }
    });
    `);

  beforeAll(async () => {
    await compiler.compile();
  });

  it.each(['equals', 'notEquals'])('casts the boolean value for %s', (operator) => {
    const query = new MongoBiQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['items.count'],
      filters: [{ member: 'items.active', operator, values: ['true'] }],
    });

    const [sql] = query.buildSqlAndParams();
    expect(sql).not.toMatch(/`items`\.active (=|<>) \?(?! = 'true')/);
  });
});
