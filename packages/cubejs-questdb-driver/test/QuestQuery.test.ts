import { QuestQuery } from '../src/QuestQuery';
import { prepareCompiler as originalPrepareCompiler } from '@cubejs-backend/schema-compiler';

const prepareCompiler = (content: string, options: any[]) => originalPrepareCompiler({
  localPath: () => __dirname,
  dataSchemaFiles: () => Promise.resolve([
    { fileName: 'main.js', content }
  ])
}, { adapter: 'postgres', ...options });

describe('QuestQuery', () => {
  const { compiler, joinGraph, cubeEvaluator } = prepareCompiler(`
    cube(\`visitors\`, {
      sql: \`
      select * from visitors
      \`,

      measures: {
        count: {
          type: 'count'
        },

        unboundedCount: {
          type: 'count',
          rollingWindow: {
            trailing: 'unbounded'
          }
        }
      },

      dimensions: {
        createdAt: {
          type: 'time',
          sql: 'created_at'
        },
        name: {
          type: 'string',
          sql: 'name'
        }
      }
    });
    `, []);

  it('test equal filters', async () => {
    await compiler.compile();

    const filterValuesVariants = [
      [[true], 'WHERE ("visitors".name = $1)'],
      [[false], 'WHERE ("visitors".name = $1)'],
      [[''], 'WHERE ("visitors".name = $1)'],
      [[null], 'WHERE ("visitors".name = NULL)'],
    ];

    for (const [values, expected] of filterValuesVariants) {
      const query = new QuestQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'visitors.count'
        ],
        timeDimensions: [],
        filters: [{
          member: 'visitors.name',
          operator: 'equals',
          values
        }],
        timezone: 'America/Los_Angeles'
      });

      const queryAndParams = query.buildSqlAndParams();

      expect(queryAndParams[0]).toContain(expected);
    }
  });

  it('test non-positional order by',
    () => compiler.compile().then(() => {
      const query = new QuestQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timeDimensions: [
          {
            dimension: 'visitors.createdAt',
            granularity: 'day',
            dateRange: ['2017-01-01', '2017-01-02'],
          },
        ],
        timezone: 'America/Los_Angeles',
        order: [
          {
            id: 'visitors.createdAt',
          },
        ],
      });

      const queryAndParams = query.buildSqlAndParams();

      expect(queryAndParams[0]).toContain('ORDER BY "visitors__created_at_day"');
    }));

  it('test non-positional group by',
    () => compiler.compile().then(() => {
      const query = new QuestQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timeDimensions: [
          {
            dimension: 'visitors.createdAt',
            granularity: 'day',
            dateRange: ['2017-01-01', '2017-01-02'],
          },
        ],
        timezone: 'America/Los_Angeles',
        order: [
          {
            id: 'visitors.createdAt',
          },
        ],
      });

      const queryAndParams = query.buildSqlAndParams();

      expect(queryAndParams[0]).toContain('GROUP BY "visitors__created_at_day"');
    }));
});
