import {
  getEnv,
} from '@cubejs-backend/shared';
import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

describe('Member Expression', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
cubes:
  - name: customers
    sql: >
      SELECT 9 as ID, 'state1' as STATE, 'New York' as CITY

    dimensions:
      - name: id
        sql: ID
        type: number
        primary_key: true

      - name: state
        sql: STATE
        type: string

      - name: city
        sql: CITY
        type: string


    measures:
      - name: count
        type: count

views:
  - name: customers_view

    cubes:
      - join_path: customers
        includes:
          - count

          - city

    `);

  async function runQueryTest(q, expectedResult) {
    /* if (!getEnv('nativeSqlPlanner')) {
      return;
    } */
    await compiler.compile();
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, q);

    console.log(query.buildSqlAndParams());

    const res = await dbRunner.testQuery(query.buildSqlAndParams());
    console.log(JSON.stringify(res));

    expect(res).toEqual(
      expectedResult
    );
  }

  it('member expression over views', async () => runQueryTest({
    measures: [
      {
        // eslint-disable-next-line no-new-func
        expression: new Function(
          'customers_view',
          // eslint-disable-next-line no-template-curly-in-string
          'return `${customers_view.count}`'
        ),
        // eslint-disable-next-line no-template-curly-in-string
        definition: '${customers_view.count}',
        expressionName: 'count',
        cubeName: 'customers_view',
      },
      {
        // eslint-disable-next-line no-new-func
        expression: new Function(
          'customers_view',
          // eslint-disable-next-line no-template-curly-in-string
          'return `${customers_view.city}`'
        ),
        // eslint-disable-next-line no-template-curly-in-string
        definition: '${customers_view.city}',
        expressionName: 'city',
        cubeName: 'customers_view',
      },
      {
        // eslint-disable-next-line no-new-func
        expression: new Function(
          // eslint-disable-next-line no-template-curly-in-string
          'return `\'NULL\'`'
        ),
        // eslint-disable-next-line no-template-curly-in-string
        definition: 'CAST(NULL AS STRING)',
        expressionName: 'cubejoinfield',
        cubeName: 'customers_view',
      },
    ],
    allowUngroupedWithoutPrimaryKey: true,
    ungrouped: true,
  }, [

    { orders__date_year: '2023-01-01T00:00:00.000Z',
      orders__revenue: '15',
      orders__revenue_1_y_ago: '5',
      orders__cagr_1_y: '2.0000000000000000' },
    { orders__date_year: '2024-01-01T00:00:00.000Z', orders__revenue: '30', orders__revenue_1_y_ago: '15', orders__cagr_1_y: '1.0000000000000000' },
    { orders__date_year: '2025-01-01T00:00:00.000Z', orders__revenue: '5', orders__revenue_1_y_ago: '30', orders__cagr_1_y: '-0.83333333333333333333' }]));
});
