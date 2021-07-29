import { DruidQuery } from '../src/DruidQuery';
import { prepareCompiler } from './PrepareCompiler';

describe('DruidQuery', () => {
  const { compiler, joinGraph, cubeEvaluator } = prepareCompiler(`
    cube(\`visitors\`, {
      sql: \`
      select * from visitors
      \`,

      measures: {
        count: {
          type: 'count'
        }
      },

      dimensions: {
        name: {
          type: 'string',
          sql: 'name'
        }
      }
    })
    `);

  it('druid query like test',
    () => compiler.compile().then(() => {
      const query = new DruidQuery(
        { joinGraph, cubeEvaluator, compiler },
        {
          measures: [],
          filters: [
            {
              "member": "visitors.name",
              "operator": "contains",
              "values": [
                "demo"
              ]
            }
          ]
        }
      );
      const queryAndParams = query.buildSqlAndParams();
      expect(queryAndParams[0]).toContain("LIKE CONCAT('%', ?, '%'))");
    }));
});
