import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
// import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// TODO: move into utils
async function runQueryTest(prepareCompilerResult, { cubeQuery, expectedResult }) {
  await prepareCompilerResult.compiler.compile();
  const query = new PostgresQuery(prepareCompilerResult, cubeQuery);

  console.log(query.buildSqlAndParams());

  const res = await dbRunner.testQuery(query.buildSqlAndParams());
  console.log(JSON.stringify(res));

  expect(res).toEqual(
    expectedResult
  );
}

describe('Views in YAML', () => {
  jest.setTimeout(200000);
});

describe('Views in JS', () => {
  jest.setTimeout(200000);

  const prepareCompilerResult = prepareJsCompiler(`
    cube(\`orders\`, {
      sql: \`SELECT 1 as id, 1 as customer_id, '2022-01-01' as timestamp\`,

      joins: {
        customers: {
          relationship: \`many_to_one\`,
          sql: \`\${CUBE}.customer_id = \${customers}.id\`
        }
      },

      dimensions: {
        id: {
          sql: \`id\`,
          type: \`number\`,
          primary_key: true,
          public: true
        },

        time: {
          sql: \`timestamp\`,
          type: \`time\`
        }
      },

      measures: {
        count: {
          type: \`count\`
        }
      }
    });

    cube(\`customers\`, {
      sql: \`SELECT 1 as id, 'Foo' as name\`,

      dimensions: {
        id: {
          sql: \`id\`,
          type: \`number\`,
          primary_key: true,
          public: true
        },

        name: {
          sql: \`name\`,
          type: \`string\`
        }
      }
    });

    view(\`ecommerce\`, {
      cubes: [
        {
          join_path: orders,
          prefix: true,
          includes: \`*\`
        },
        {
          join_path: orders.customers,
          prefix: true,
          includes: [\`name\`]
        }
      ]
    });

  `);

  it('join_path', async () => runQueryTest(prepareCompilerResult, {
    cubeQuery: {
      measures: ['ecommerce.orders_count'],
      dimensions: ['ecommerce.customers_name']
    },
    expectedResult: [
      { ecommerce__customers_name: 'Foo', ecommerce__orders_count: '1' }
    ]
  }));
});
