import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { ClickHouseDbRunner } from './ClickHouseDbRunner';

describe('ClickHouse complex joins', () => {
  jest.setTimeout(20 * 1000);

  const dbRunner = new ClickHouseDbRunner();

  afterAll(async () => {
    await dbRunner.tearDown();
  });

  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
    cube(\`Acube\`, {
      sql: \`SELECT * FROM (SELECT 1 as id, 'Category A' as category, 10 as value UNION ALL
           SELECT 2, 'Category B', 20 UNION ALL
           SELECT 3, 'Category C', 30) as t\`,

      joins: {
        Bcube: {
          relationship: \`one_to_many\`,
          sql: \`\${Acube}.id = \${Bcube}.id\`
        },
        Ccube: {
          relationship: \`one_to_many\`,
          sql: \`\${Acube}.id = \${Ccube}.id\`
        },
      },

      measures: {
        AcubeCount: {
          type: \`count\`,
        },
        AcubeTotalValue: {
          type: \`sum\`,
          sql: \`value\`
        },
        BcubeTotalValue: {
          sql: \`\${Bcube.totalValue}\`,
          type: \`number\`
        },
        CcubeTotalAmount: {
          sql: \`\${Ccube.totalAmount}\`,
          type: \`number\`
        },
      },

      dimensions: {
        id: {
          sql: \`id\`,
          type: \`number\`,
          primaryKey: true,
          shown: true,
          title: \`id\`
        },
        category: {
          sql: \`category\`,
          type: \`string\`
        }
      }
    });

    cube(\`Bcube\`, {
      sql: \`SELECT 1 as id, 'Bgroup1' as groupName, 50 as value UNION ALL
           SELECT 2, 'Bgroup2', 60 UNION ALL
           SELECT 3, 'Bgroup3', 70\`,

      measures: {
        count: {
          type: \`count\`
        },
        totalValue: {
          type: \`sum\`,
          sql: \`value\`
        }
      },

      dimensions: {
        id: {
          sql: \`id\`,
          type: \`number\`,
          primaryKey: true
        },
        groupName: {
          sql: \`groupName\`,
          type: \`string\`
        }
      }
    });

    cube(\`Ccube\`, {
      sql: \`SELECT 1 as id, 'Ctype1' as type, 15 as amount UNION ALL
           SELECT 2, 'Ctype2', 25 UNION ALL
           SELECT 3, 'Ctype3', 35\`,

      measures: {
        count: {
          type: \`count\`
        },
        totalAmount: {
          type: \`sum\`,
          sql: \`amount\`
        }
      },

      dimensions: {
        id: {
          sql: \`id\`,
          type: \`number\`,
          primaryKey: true,
          shown: true
        },
        type: {
          sql: \`type\`,
          type: \`string\`
        }
      }
    });
  `);

  it('query with 1 cube join', async () => dbRunner.runQueryTest(
    {
      dimensions: [
        'Acube.category',
        'Acube.id'
      ],
      measures: [
        'Acube.AcubeCount',
        'Acube.AcubeTotalValue',
        'Acube.BcubeTotalValue'
      ],
      order: [{
        id: 'Acube.id',
        desc: false
      }],
      queryType: 'multi'
    },
    [
      {
        acube__category: 'Category A',
        acube__id: '1',
        acube___acube_count: '1',
        acube___acube_total_value: '10',
        acube___bcube_total_value: '50',
      },
      {
        acube__category: 'Category B',
        acube__id: '2',
        acube___acube_count: '1',
        acube___acube_total_value: '20',
        acube___bcube_total_value: '60',
      },
      {
        acube__category: 'Category C',
        acube__id: '3',
        acube___acube_count: '1',
        acube___acube_total_value: '30',
        acube___bcube_total_value: '70',
      }
    ],
    { joinGraph, cubeEvaluator, compiler }
  ));

  it('query with 2 cube joins', async () => dbRunner.runQueryTest(
    {
      dimensions: [
        'Acube.category',
        'Acube.id'
      ],
      measures: [
        'Acube.AcubeCount',
        'Acube.AcubeTotalValue',
        'Acube.BcubeTotalValue',
        'Acube.CcubeTotalAmount'
      ],
      order: [{
        id: 'Acube.id',
        desc: false
      }],
      queryType: 'multi'
    },
    [
      {
        acube__category: 'Category A',
        acube__id: '1',
        acube___acube_count: '1',
        acube___acube_total_value: '10',
        acube___bcube_total_value: '50',
        acube___ccube_total_amount: '15',
      },
      {
        acube__category: 'Category B',
        acube__id: '2',
        acube___acube_count: '1',
        acube___acube_total_value: '20',
        acube___bcube_total_value: '60',
        acube___ccube_total_amount: '25',
      },
      {
        acube__category: 'Category C',
        acube__id: '3',
        acube___acube_count: '1',
        acube___acube_total_value: '30',
        acube___bcube_total_value: '70',
        acube___ccube_total_amount: '35',
      }
    ],
    { joinGraph, cubeEvaluator, compiler }
  ));
});
