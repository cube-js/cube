import {
  getEnv,
} from '@cubejs-backend/shared';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

describe('Calc-Groups', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
cubes:
  - name: orders
    sql: >
      SELECT 9 as ID, 'completed' as STATUS, '2022-01-12T20:00:00.000Z'::timestamptz as CREATED_AT
      union all
      SELECT 10 as ID, 'completed' as STATUS, '2023-01-12T20:00:00.000Z'::timestamptz as CREATED_AT
      union all
      SELECT 11 as ID, 'completed' as STATUS, '2024-01-14T20:00:00.000Z'::timestamptz as CREATED_AT
      union all
      SELECT 12 as ID, 'completed' as STATUS, '2024-02-14T20:00:00.000Z'::timestamptz as CREATED_AT
      union all
      SELECT 13 as ID, 'completed' as STATUS, '2025-03-14T20:00:00.000Z'::timestamptz as CREATED_AT
    joins:
      - name: line_items
        sql: "{CUBE}.ID = {line_items}.order_id"
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

      - name: currency
        type: switch
        values:
          - USD
          - EUR
          - GBP

      - name: test
        type: string
        case:
          when:
            - sql: "{CUBE.currency} = 'USD'"
              label: '111'
            - sql: "{CUBE.currency} = 'EUR'"
              label: '333'
          else:
            label: 'def'

      - name: strategy
        type: switch
        values:
          - A
          - B

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
        sql: "CASE WHEN {CUBE}.status = 'completed' THEN {CUBE.amount} END"
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
      SELECT 9 as ID, '2024-01-12T20:00:00.000Z'::timestamptz as CREATED_AT, 9 as ORDER_ID, 11 as PRODUCT_ID
      union all
      SELECT 10 as ID, '2024-01-12T20:00:00.000Z'::timestamptz as CREATED_AT, 10 as ORDER_ID, 10 as PRODUCT_ID
      union all
      SELECT 11 as ID, '2024-01-12T20:00:00.000Z'::timestamptz as CREATED_AT, 10 as ORDER_ID, 11 as PRODUCT_ID
      union all
      SELECT 12 as ID, '2024-01-12T20:00:00.000Z'::timestamptz as CREATED_AT, 11 as ORDER_ID, 10 as PRODUCT_ID
      union all
      SELECT 13 as ID, '2024-01-12T20:00:00.000Z'::timestamptz as CREATED_AT, 11 as ORDER_ID, 10 as PRODUCT_ID
      union all
      SELECT 14 as ID, '2024-01-12T20:00:00.000Z'::timestamptz as CREATED_AT, 12 as ORDER_ID, 10 as PRODUCT_ID
      union all
      SELECT 15 as ID, '2024-01-12T20:00:00.000Z'::timestamptz as CREATED_AT, 13 as ORDER_ID, 11 as PRODUCT_ID
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
      SELECT 10 as ID, 'some category' as PRODUCT_CATEGORY, 'some name' as NAME, 10 as PRICE
      union all
      SELECT 11 as ID, 'some category' as PRODUCT_CATEGORY, 'some name' as NAME, 5 as PRICE
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
  - name: orders_view

    cubes:
      - join_path: orders
        includes:
          - date
          - revenue
          - cagr_1_y
          - return_rate

      - join_path: line_items.products
        prefix: true
        includes:
          - product_category

    `);

  if (getEnv('nativeSqlPlanner')) {
    it('basic cross join', async () => dbRunner.runQueryTest({
      dimensions: ['orders.currency'],
      timeDimensions: [
        {
          dimension: 'orders.date',
          granularity: 'year'
        }
      ],
      timezone: 'UTC'
    }, [
      {
        orders__currency: 'EUR',
        orders__date_year: '2022-01-01T00:00:00.000Z'
      },
      {
        orders__currency: 'GBP',
        orders__date_year: '2022-01-01T00:00:00.000Z'
      },
      {
        orders__currency: 'USD',
        orders__date_year: '2022-01-01T00:00:00.000Z'
      },
      {
        orders__currency: 'EUR',
        orders__date_year: '2023-01-01T00:00:00.000Z'
      },
      {
        orders__currency: 'GBP',
        orders__date_year: '2023-01-01T00:00:00.000Z'
      },
      {
        orders__currency: 'USD',
        orders__date_year: '2023-01-01T00:00:00.000Z'
      },
      {
        orders__currency: 'EUR',
        orders__date_year: '2024-01-01T00:00:00.000Z'
      },
      {
        orders__currency: 'GBP',
        orders__date_year: '2024-01-01T00:00:00.000Z'
      },
      {
        orders__currency: 'USD',
        orders__date_year: '2024-01-01T00:00:00.000Z'
      },
      {
        orders__currency: 'EUR',
        orders__date_year: '2025-01-01T00:00:00.000Z'
      },
      {
        orders__currency: 'GBP',
        orders__date_year: '2025-01-01T00:00:00.000Z'
      },
      {
        orders__currency: 'USD',
        orders__date_year: '2025-01-01T00:00:00.000Z'
      }
    ],
    { joinGraph, cubeEvaluator, compiler }));

    it('basic double cross join', async () => dbRunner.runQueryTest({
      dimensions: ['orders.currency', 'orders.strategy'],
      timeDimensions: [
        {
          dimension: 'orders.date',
          granularity: 'year',
          dateRange: ['2024-01-01', '2026-01-01']
        }
      ],
      timezone: 'UTC',
      order: [{
        id: 'orders.date'
      }, {
        id: 'orders.currency'
      }, {
        id: 'orders.strategy'
      },
      ],
    }, [
      {
        orders__currency: 'EUR',
        orders__strategy: 'A',
        orders__date_year: '2024-01-01T00:00:00.000Z'
      },
      {
        orders__currency: 'EUR',
        orders__strategy: 'B',
        orders__date_year: '2024-01-01T00:00:00.000Z'
      },
      {
        orders__currency: 'GBP',
        orders__strategy: 'A',
        orders__date_year: '2024-01-01T00:00:00.000Z'
      },
      {
        orders__currency: 'GBP',
        orders__strategy: 'B',
        orders__date_year: '2024-01-01T00:00:00.000Z'
      },
      {
        orders__currency: 'USD',
        orders__strategy: 'A',
        orders__date_year: '2024-01-01T00:00:00.000Z'
      },
      {
        orders__currency: 'USD',
        orders__strategy: 'B',
        orders__date_year: '2024-01-01T00:00:00.000Z'
      },
      {
        orders__currency: 'EUR',
        orders__strategy: 'A',
        orders__date_year: '2025-01-01T00:00:00.000Z'
      },
      {
        orders__currency: 'EUR',
        orders__strategy: 'B',
        orders__date_year: '2025-01-01T00:00:00.000Z'
      },
      {
        orders__currency: 'GBP',
        orders__strategy: 'A',
        orders__date_year: '2025-01-01T00:00:00.000Z'
      },
      {
        orders__currency: 'GBP',
        orders__strategy: 'B',
        orders__date_year: '2025-01-01T00:00:00.000Z'
      },
      {
        orders__currency: 'USD',
        orders__strategy: 'A',
        orders__date_year: '2025-01-01T00:00:00.000Z'
      },
      {
        orders__currency: 'USD',
        orders__strategy: 'B',
        orders__date_year: '2025-01-01T00:00:00.000Z'
      }
    ],
    { joinGraph, cubeEvaluator, compiler }));

    it('basic cross join with measure', async () => dbRunner.runQueryTest({
      dimensions: ['orders.strategy'],
      measures: ['orders.revenue'],
      timeDimensions: [
        {
          dimension: 'orders.date',
          granularity: 'year'
        }
      ],
      timezone: 'UTC',
      order: [{
        id: 'orders.date'
      }, {
        id: 'orders.currency'
      },
      ],
    }, [
      {
        orders__date_year: '2022-01-01T00:00:00.000Z',
        orders__strategy: 'A',
        orders__revenue: '5',
      },
      {
        orders__date_year: '2022-01-01T00:00:00.000Z',
        orders__strategy: 'B',
        orders__revenue: '5',
      },
      {
        orders__date_year: '2023-01-01T00:00:00.000Z',
        orders__strategy: 'A',
        orders__revenue: '15',
      },
      {
        orders__date_year: '2023-01-01T00:00:00.000Z',
        orders__strategy: 'B',
        orders__revenue: '15',
      },
      {
        orders__date_year: '2024-01-01T00:00:00.000Z',
        orders__strategy: 'A',
        orders__revenue: '30',
      },
      {
        orders__date_year: '2024-01-01T00:00:00.000Z',
        orders__strategy: 'B',
        orders__revenue: '30',
      },
      {
        orders__date_year: '2025-01-01T00:00:00.000Z',
        orders__strategy: 'A',
        orders__revenue: '5',
      },
      {
        orders__date_year: '2025-01-01T00:00:00.000Z',
        orders__strategy: 'B',
        orders__revenue: '5',
      }
    ],
    { joinGraph, cubeEvaluator, compiler }));

    it('basic cross join with measure with filters', async () => dbRunner.runQueryTest({
      dimensions: ['orders.strategy'],
      measures: ['orders.revenue'],
      timeDimensions: [
        {
          dimension: 'orders.date',
          granularity: 'year'
        }
      ],
      filters: [
        { dimension: 'orders.strategy', operator: 'equals', values: ['B'] }
      ],
      timezone: 'UTC',
      order: [{
        id: 'orders.date'
      }, {
        id: 'orders.currency'
      },
      ],
    }, [
      {
        orders__date_year: '2022-01-01T00:00:00.000Z',
        orders__strategy: 'B',
        orders__revenue: '5',
      },
      {
        orders__date_year: '2023-01-01T00:00:00.000Z',
        orders__strategy: 'B',
        orders__revenue: '15',
      },
      {
        orders__date_year: '2024-01-01T00:00:00.000Z',
        orders__strategy: 'B',
        orders__revenue: '30',
      },
      {
        orders__date_year: '2025-01-01T00:00:00.000Z',
        orders__strategy: 'B',
        orders__revenue: '5',
      }
    ],
    { joinGraph, cubeEvaluator, compiler }));

    it('measure switch', async () => dbRunner.runQueryTest({
      dimensions: ['orders.currency', 'orders.test'],
      measures: ['orders.revenue'],
      timeDimensions: [
        {
          dimension: 'orders.date',
          granularity: 'year'
        }
      ],
      timezone: 'UTC',
      order: [{
        id: 'orders.date'
      }, {
        id: 'orders.currency'
      },
      ],
    }, [
      {
        orders__date_year: '2022-01-01T00:00:00.000Z',
        orders__strategy: 'B',
        orders__revenue: '5',
      },
      {
        orders__date_year: '2023-01-01T00:00:00.000Z',
        orders__strategy: 'B',
        orders__revenue: '15',
      },
      {
        orders__date_year: '2024-01-01T00:00:00.000Z',
        orders__strategy: 'B',
        orders__revenue: '30',
      },
      {
        orders__date_year: '2025-01-01T00:00:00.000Z',
        orders__strategy: 'B',
        orders__revenue: '5',
      }
    ],
    { joinGraph, cubeEvaluator, compiler }));
  } else {
    // This test is working only in tesseract
    test.skip('calc groups testst', () => { expect(1).toBe(1); });
  }
});
