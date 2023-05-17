import { BaseQuery, PostgresQuery } from '../../../src/adapter';
import { prepareCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

describe('Cube Views Failing', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator, metaTransformer } = prepareCompiler(`
    cube(\`web_first_touch\`, {
      sql: \`
        SELECT 1 AS id, '2022-01-01T00:00:00.000Z'::TIMESTAMPTZ AS timestamp UNION ALL
        SELECT 2 AS id, '2022-01-02T00:00:00.000Z'::TIMESTAMPTZ AS timestamp
      \`,

      measures: {
        count: {
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
        web_first_touch.timestamp
      ]
    });
  `);

  async function runQueryTest(q: any, expectedResult: any, additionalTest?: (query: BaseQuery) => any) {
    await compiler.compile();
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, { ...q, timezone: 'UTC', preAggregationsSchema: '' });

    console.log(query.buildSqlAndParams());

    console.log(query.cacheKeyQueries());

    const res = await dbRunner.evaluateQueryWithPreAggregations(query);
    console.log(JSON.stringify(res));

    if (additionalTest) {
      additionalTest(query);
    }

    expect(res).toEqual(
      expectedResult
    );
  }

  it('simple view', async () => runQueryTest({
    measures: ['OrdersView.count'],
    dimensions: [
      'OrdersView.categoryName'
    ],
    order: [{ id: 'OrdersView.categoryName' }]
  }, [{
    web_visitors_view__timestamp: '2022-01-01T00:00:00.000Z',
    web_visitors_view__count: '1'
  }, {
    web_visitors_view__timestamp: '2022-01-02T00:00:00.000Z',
    web_visitors_view__count: '1'
  }]));
});
