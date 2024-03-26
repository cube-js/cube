/* globals describe, it, expect */
const { prepareCompiler } = require('@cubejs-backend/schema-compiler');
const VerticaQuery = require('../src/VerticaQuery.js');

const testCompiler = (content, options) => prepareCompiler({
  localPath: () => __dirname,
  dataSchemaFiles: () => Promise.resolve([
    { fileName: 'main.js', content },
  ]),
}, { adapter: 'vertica', ...options });

describe('VerticaQuery', () => {
  const { compiler, joinGraph, cubeEvaluator } = testCompiler(`
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
        },
        dnc_address: {
          type: 'boolean',
          sql: 'dnc_address',
          title: 'Unsubscribed (Address)'
        },
        createdAt: {
            sql: \`created_at\`,
            type: 'time',
        }
      }
      
    })
    `, {});

  it('vertica query like test', async () => {
    await compiler.compile();

    const query = new VerticaQuery(
      { joinGraph, cubeEvaluator, compiler },
      {
        measures: [],
        filters: [
          {
            member: 'visitors.name',
            operator: 'contains',
            values: [
              'demo',
            ],
          },
        ],
      },
    );

    const queryAndParams = query.buildSqlAndParams();
    expect(queryAndParams[0]).toContain('("visitors".name ILIKE \'%\' || ? || \'%\')');
  });

  it('vertica query boolean', async () => {
    await compiler.compile();

    const query = new VerticaQuery(
      { joinGraph, cubeEvaluator, compiler },
      {
        measures: [],
        filters: [
          {
            member: 'visitors.dnc_address',
            operator: 'equals',
            values: ['0', null],
          },
        ],
      },
    );

    const queryAndParams = query.buildSqlAndParams();
    expect(queryAndParams[0]).toContain('("visitors".dnc_address IN (CAST(? AS BOOLEAN)) OR "visitors".dnc_address IS NULL');
  });

  it('test equal filters', async () => {
    await compiler.compile();

    const filterValuesVariants = [
      [[true], 'WHERE ("visitors".name = ?)'],
      [[false], 'WHERE ("visitors".name = ?)'],
      [[''], 'WHERE ("visitors".name = ?)'],
      [[null], 'WHERE ("visitors".name IS NULL)'],
    ];

    for (const [values, expected] of filterValuesVariants) {
      const query = new VerticaQuery({ joinGraph, cubeEvaluator, compiler }, {
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
});
