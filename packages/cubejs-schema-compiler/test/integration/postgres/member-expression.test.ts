import {
  getEnv,
} from '@cubejs-backend/shared';
import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

describe('Member Expression Multistage', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
cubes:
  - name: customers
    sql: >
      SELECT 9 as ID, 'state1' as STATE, 'New York' as CITY
      UNION ALL
      SELECT 10 as ID, 'state2' as STATE, 'New York' as CITY
      UNION ALL
      SELECT 11 as ID, 'state3' as STATE, 'LA' as CITY

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

  - name: orders
    sql: >
      select 10 AS ID, 'complited' AS STATUS, '2021-01-05 00:00:00'::timestamp AS CREATED_AT, 100 AS CUSTOMER_ID, 50.0 as revenue
      UNION ALL
      select 11 AS ID, 'complited' AS STATUS, '2021-05-01 00:00:00'::timestamp AS CREATED_AT, 100 AS CUSTOMER_ID, 150.0 as revenue
      UNION ALL
      select 12 AS ID, 'complited' AS STATUS, '2021-06-01 00:00:00'::timestamp AS CREATED_AT, 100 AS CUSTOMER_ID, 200.0 as revenue
      UNION ALL
      select 13 AS ID, 'complited' AS STATUS, '2022-01-04 00:00:00'::timestamp AS CREATED_AT, 100 AS CUSTOMER_ID, 10.0 as revenue
      UNION ALL
      select 14 AS ID, 'complited' AS STATUS, '2022-05-04 00:00:00'::timestamp AS CREATED_AT, 100 AS CUSTOMER_ID, 30.0 as revenue
    public: false

    joins:
      - name: line_items
        sql: "{CUBE}.ID = {line_items}.order_id"
        relationship: many_to_one

      - name: customers
        sql: "{CUBE}.CUSTOMER_ID = {customers}.ID"
        relationship: many_to_one

    dimensions:
      - name: id
        sql: ID
        type: number
        primary_key: true

      - name: status
        sql: STATUS
        type: string

      - name: date
        sql: CREATED_AT
        type: time

      - name: amount
        sql: '{line_items.total_amount}'
        type: number
        sub_query: true

    measures:
      - name: count
        type: count

      - name: completed_count
        type: count
        filters:
          - sql: "{CUBE}.STATUS = 'completed'"

      - name: returned_count
        type: count
        filters:
          - sql: "{CUBE}.STATUS = 'returned'"

      - name: return_rate
        type: number
        sql: "({returned_count} / NULLIF({completed_count}, 0)) * 100.0"
        description: "Percentage of returned orders out of completed, exclude just placed orders."
        format: percent

      - name: total_amount
        sql: '{CUBE.amount}'
        type: sum

      - name: revenue
        sql: "revenue"
        type: sum
        format: currency

      - name: average_order_value
        sql: '{CUBE.amount}'
        type: avg

      - name: revenue_1_y_ago
        sql: "{revenue}"
        multi_stage: true
        type: number
        format: currency
        time_shift:
          - time_dimension: date
            interval: 1 year
            type: prior
          - time_dimension: orders_view.date
            interval: 1 year
            type: prior

      - name: cagr_1_y
        sql: "(({revenue} / {revenue_1_y_ago}) - 1)"
        type: number
        format: percent
        description: "Annual CAGR, year over year growth in revenue"

  - name: line_items
    sql: >
      SELECT 10 AS ID, 10 AS PRODUCT_ID, '2021-01-01 00:00:00'::timestamp AS CREATED_AT, 10 as order_id
      UNION ALL
      SELECT 11 AS ID, 10 AS PRODUCT_ID, '2021-01-01 00:00:00'::timestamp AS CREATED_AT, 11 as order_id
      UNION ALL
      SELECT 12 AS ID, 10 AS PRODUCT_ID, '2021-01-01 00:00:00'::timestamp AS CREATED_AT, 11 as order_id
      UNION ALL
      SELECT 13 AS ID, 10 AS PRODUCT_ID, '2021-01-01 00:00:00'::timestamp AS CREATED_AT, 12 as order_id
    public: false

    joins:
      - name: products
        sql: "{CUBE}.PRODUCT_ID = {products}.ID"
        relationship: many_to_one

    dimensions:
      - name: id
        sql: ID
        type: number
        primary_key: true

      - name: created_at
        sql: CREATED_AT
        type: time

      - name: price
        sql: "{products.price}"
        type: number

    measures:
      - name: count
        type: count

      - name: total_amount
        sql: "{price}"
        type: sum
  - name: products
    sql: >
      SELECT 10 AS ID, 'Clothes' AS PRODUCT_CATEGORY, 'Shirt' AS NAME, 10 AS PRICE
      UNION ALL
      SELECT 11 AS ID, 'Clothes' AS PRODUCT_CATEGORY, 'Shirt' AS NAME, 20 AS PRICE
    public: false
    description: > 
      Products and categories in our e-commerce store.

    dimensions:
      - name: id
        sql: ID
        type: number
        primary_key: true

      - name: product_category
        sql: PRODUCT_CATEGORY
        type: string

      - name: name
        sql: NAME
        type: string

      - name: price
        sql: PRICE
        type: number

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

  - name: orders_view
    cubes:
      - join_path: orders
        includes:
          - count
          - date
          - revenue
          - cagr_1_y
          - return_rate

      - join_path: line_items.products
        prefix: true
        includes:
          - product_category
      
      - join_path: orders.customers
        prefix: true
        includes:
          - city
          - count
          - id


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
    segments: [
      {
        // eslint-disable-next-line no-new-func
        expression: new Function(
          'customers_view',
          // eslint-disable-next-line no-template-curly-in-string
          'return `(${customers_view.city} = \'New York\')`'
        ),
        // eslint-disable-next-line no-template-curly-in-string
        definition: '(${customers_view.city} = \'New York\')',
        expressionName: 'castomers_view_c',
        cubeName: 'customers_view',
      },

    ],
    allowUngroupedWithoutPrimaryKey: true,
    ungrouped: true,
  },

  [{ count: 1, city: 'New York', cubejoinfield: 'NULL' }, { count: 1, city: 'New York', cubejoinfield: 'NULL' }]));
  if (getEnv('nativeSqlPlanner')) {
    it('member expression multi stage', async () => runQueryTest({
      measures: [
        {
        // eslint-disable-next-line no-new-func
          expression: new Function(
            'orders',
            // eslint-disable-next-line no-template-curly-in-string
            'return `${orders.cagr_1_y}`'
          ),
          // eslint-disable-next-line no-template-curly-in-string
          definition: '${orders.cagr_1_y}',
          expressionName: 'orders__cagr_2023',
          cubeName: 'orders',
        },
      ],
      timeDimensions: [
        {
          dimension: 'orders.date',
          dateRange: ['2022-01-01', '2022-10-31'],
        },
      ],
      timezone: 'America/Los_Angeles'
    },

    [{ orders__cagr_2023: '-0.90000000000000000000' }]));
  } else {
    it.skip('member expression multi stage', () => {
      // Skipping because it works only in Tesseract
    });
  }

  if (getEnv('nativeSqlPlanner')) {
    it('member expression multi stage with time dimension segment', async () => runQueryTest({
      measures: [
        {
        // eslint-disable-next-line no-new-func
          expression: new Function(
            'orders',
            // eslint-disable-next-line no-template-curly-in-string
            'return `${orders.cagr_1_y}`'
          ),
          // eslint-disable-next-line no-template-curly-in-string
          definition: '${orders.cagr_1_y}',
          expressionName: 'orders__cagr_2023',
          cubeName: 'orders',
        },
      ],
      segments: [
        {
          cubeName: 'orders',
          name: 'orders_date____c',
          expressionName: 'orders_date____c',
          // eslint-disable-next-line no-new-func
          expression: new Function(
            'orders',
            // eslint-disable-next-line no-template-curly-in-string
            'return `((${orders.date} >= CAST(\'2022-01-01\' AS TIMESTAMP)) AND (${orders.date} < CAST(\'2022-10-31\' AS TIMESTAMP)))`'
          ),
          // eslint-disable-next-line no-template-curly-in-string
          definition: '{"cube_name":"orders","alias":"orders_date____c","cube_params":["orders"],"expr":"((${orders.date} >= CAST($0$ AS TIMESTAMP)) AND (${orders.date} < CAST($1$ AS TIMESTAMP)))","grouping_set":null}',
        }
      ],

      timezone: 'America/Los_Angeles'
    },

    [{ orders__cagr_2023: '-0.90000000000000000000' }]));
  } else {
    it.skip('member expression multi stage with time dimension segment', () => {
      // Skipping because it works only in Tesseract
    });
  }

  if (getEnv('nativeSqlPlanner')) {
    it('multi stage duplicated time shift over view and origin cube', async () => runQueryTest({
      measures: [
        'orders_view.cagr_1_y'
      ],
      timeDimensions: [
        {
          dimension: 'orders_view.date',
          dateRange: ['2022-01-01', '2022-10-31'],
        },
      ],

      timezone: 'America/Los_Angeles'
    },

    [{ orders_view__cagr_1_y: '-0.90000000000000000000' }]));
  } else {
    it.skip('member expression multi stage with time dimension segment', () => {
      // Skipping because it works only in Tesseract
    });
  }
});
