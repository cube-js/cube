import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
// import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { prepareCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// TODO: move into utils
async function runQueryTest(prepareCompilerResult, { cubeQuery, expectedResult }) {
  await prepareCompilerResult.compiler.compile();
  const query = new PostgresQuery(prepareCompilerResult, cubeQuery);

  // console.log(query.buildSqlAndParams());

  const res = await dbRunner.testQuery(query.buildSqlAndParams());
  // console.log(JSON.stringify(res));

  expect(res).toEqual(
    expectedResult
  );
}

describe('Cube Views Failing', () => {
  jest.setTimeout(200000);

  const prepareCompilerResult = prepareCompiler(`
  cube(\`web_first_touch\`, {
    sql: \`
      SELECT 1 AS id, '2022-01-01' AS timestamp UNION ALL
      SELECT 2 AS id, '2022-01-02' AS timestamp
    \`,

    measures: {
      visitor_count: {
        type: \`count\`
      }
    },

    dimensions: {
      timestamp: {
        sql: \`timestamp\`,
        type: \`time\`
      }
    }
  });

  view(\`web_visitors_view\`, {
    includes: [
      web_first_touch.timestamp,
      web_first_touch.visitor_count
    ]
  });
`);

  it('test', async () => runQueryTest(prepareCompilerResult, {
    cubeQuery: {
      measures: ['web_visitors_view.visitor_count'],
      timeDimensions: [{
        dimension: 'web_visitors_view.timestamp',
        dateRange: ['2023-04-17', '2023-05-17']
      }],
      order: [{ id: 'OrdersView.categoryName' }]
    },
    expectedResult: [{
      web_visitors_view__timestamp: '2022-01-01T00:00:00.000Z',
      web_visitors_view__count: '1'
    }, {
      web_visitors_view__timestamp: '2022-01-02T00:00:00.000Z',
      web_visitors_view__count: '1'
    }]
  }));
});
