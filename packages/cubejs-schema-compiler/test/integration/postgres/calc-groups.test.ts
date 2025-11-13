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
      SELECT 9 as ID, 'completed' as STATUS, 100.0 as amount_usd, 97.4 as amount_eur, 80.6 as amount_gbp, '2022-01-12T20:00:00.000Z'::timestamptz as CREATED_AT
      union all
      SELECT 10 as ID, 'completed' as STATUS, 10.0 as amount_usd, 9.74 as amount_eur, 8.06 as amount_gbp, '2023-01-12T20:00:00.000Z'::timestamptz as CREATED_AT
      union all
      SELECT 11 as ID, 'completed' as STATUS, 1000.0 as amount_usd, 974 as amount_eur, 806 as amount_gbp,'2024-01-14T20:00:00.000Z'::timestamptz as CREATED_AT
      union all
      SELECT 12 as ID, 'completed' as STATUS, 30.0 as amount_usd, 28 as amount_eur, 22 as amount_gbp,'2024-02-14T20:00:00.000Z'::timestamptz as CREATED_AT
      union all
      SELECT 13 as ID, 'completed' as STATUS, 40.0 as amount_usd, 38 as amount_eur, 33 as amount_gbp, '2025-03-14T20:00:00.000Z'::timestamptz as CREATED_AT
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

      - name: currency_ref
        type: string
        sql: "{currency}"


      - name: strategy
        type: switch
        values:
          - A
          - B

      - name: strategy_ref
        type: string
        sql: "{strategy}"

      - name: currency_and_stategy
        type: string
        sql: "CONCAT({currency}, '-', {strategy})"

      - name: currency_and_strategy_ref
        type: string
        sql: "{currency_and_stategy}"

      - name: currency_full_name
        type: string
        case:
          switch: "{CUBE.currency}"
          when:
            - value: USD
              sql: "'dollars'"
            - value: EUR
              sql: "'euros'"
          else:
            sql: "'unknown'"

    measures:
      - name: count
        type: count

      - name: completed_count
        type: count
        filters:
          - sql: "{CUBE}.STATUS = 'completed'"

      - name: amount_usd
        type: sum
        sql: amount_usd

      - name: amount_eur
        type: sum
        sql: amount_eur

      - name: amount_gbp
        type: sum
        sql: amount_gbp

      - name: amount_in_currency
        type: number
        multi_stage: true
        case:
          switch: "{CUBE.currency}"
          when:
            - value: USD
              sql: "{CUBE.amount_usd}"
            - value: EUR
              sql: "{CUBE.amount_eur}"
          else:
            sql: "{CUBE.amount_gbp}"

      - name: amount_in_currency_ref
        type: number
        sql: "{CUBE.amount_in_currency}"

      - name: returned_count
        type: count
        filters:
          - sql: "{CUBE}.STATUS = 'returned'"

      - name: amount_in_currency_percent_of_usd
        type: number
        sql: "FLOOR({CUBE.amount_in_currency_ref} / {CUBE.amount_usd} * 100)"


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

  - name: source_a
    sql: >
      SELECT 10 as ID, 'some category' as PRODUCT_CATEGORY, 'some name' as NAME, 100 as PRICE_USD, 0 as PRICE_EUR, '2022-01-12T20:00:00.000Z'::timestamptz as CREATED_AT
      union all
      SELECT 11 as ID, 'some category' as PRODUCT_CATEGORY, 'some name' as NAME, 500 as PRICE_USD, 0 as PRICE_EUR, '2022-01-14T20:00:00.000Z'::timestamptz as CREATED_AT
      union all
      SELECT 12 as ID, 'some category A' as PRODUCT_CATEGORY, 'some name' as NAME, 200 as PRICE_USD, 0 as PRICE_EUR, '2022-02-12T20:00:00.000Z'::timestamptz as CREATED_AT
      union all
      SELECT 13 as ID, 'some category A' as PRODUCT_CATEGORY, 'some name' as NAME, 300 as PRICE_USD, 0 as PRICE_EUR, '2022-03-14T20:00:00.000Z'::timestamptz as CREATED_AT
    public: false

    dimensions:
      - name: pk
        type: number
        sql: ID
        primary_key: true

      - name: product_category
        sql: PRODUCT_CATEGORY
        type: string

      - name: created_at
        sql: CREATED_AT
        type: time

    measures:
      - name: count
        type: 'count'

      - name: price_usd
        type: 'sum'
        sql: PRICE_USD

      - name: price_eur
        type: 'sum'
        sql: PRICE_EUR


  - name: source_b
    sql: >
      SELECT 10 as ID, 'some category' as PRODUCT_CATEGORY, 'some name' as NAME, 0 as PRICE_USD, 100 as PRICE_EUR, '2022-01-12T20:00:00.000Z'::timestamptz as CREATED_AT
      union all
      SELECT 11 as ID, 'some category' as PRODUCT_CATEGORY, 'some name' as NAME, 0 as PRICE_USD, 500 as PRICE_EUR, '2022-02-12T20:00:00.000Z'::timestamptz as CREATED_AT
      union all
      SELECT 12 as ID, 'some category B' as PRODUCT_CATEGORY, 'some name' as NAME, 0 as PRICE_USD, 200 as PRICE_EUR, '2022-02-15T20:00:00.000Z'::timestamptz as CREATED_AT
      union all
      SELECT 13 as ID, 'some category B' as PRODUCT_CATEGORY, 'some name' as NAME, 0 as PRICE_USD, 300 as PRICE_EUR, '2022-03-12T20:00:00.000Z'::timestamptz as CREATED_AT
      union all
      SELECT 14 as ID, 'some category B' as PRODUCT_CATEGORY, 'some name' as NAME, 0 as PRICE_USD, 300 as PRICE_EUR, '2022-04-12T20:00:00.000Z'::timestamptz as CREATED_AT
    public: false

    dimensions:
      - name: pk
        type: number
        sql: ID
        primary_key: true

      - name: product_category
        sql: PRODUCT_CATEGORY
        type: string

      - name: created_at
        sql: CREATED_AT
        type: time

    measures:
      - name: count
        type: 'count'

      - name: price_usd
        type: 'sum'
        sql: PRICE_USD

      - name: price_eur
        type: 'sum'
        sql: PRICE_EUR

views:

  - name: source
    dimensions:
      - name: source
        type: switch
        values: ["A", "B"]

      - name: currency
        type: switch
        values: ["USD", "EUR"]

      - name: product_category
        type: string
        multi_stage: true
        case:
          switch: "{CUBE.source}"
          when:
            - value: A
              sql: "{source_a.product_category}"
            - value: B
              sql: "{source_b.product_category}"
          else:
            sql: "{source_a.product_category}"

      - name: product_category_ext
        type: string
        multi_stage: true
        case:
          switch: "{CUBE.currency}"
          when:
            - value: USD
              sql: "CONCAT({source.product_category}, '-', 'USD', '-', {source.currency})"
            - value: EUR
              sql: "CONCAT({source.product_category}, '-', 'EUR', '-', {source.currency})"
          else:
            sql: ""

      - name: created_at
        type: time
        multi_stage: true
        case:
          switch: "{CUBE.source}"
          when:
            - value: A
              sql: "{source_a.created_at}"
            - value: B
              sql: "{source_b.created_at}"
          else:
            sql: "{source_a.created_at}"


    measures:
      - name: count
        type: sum
        multi_stage: true
        case:
          switch: "{CUBE.source}"
          when:
            - value: A
              sql: "{source_a.count}"
            - value: B
              sql: "{source_b.count}"
          else:
            sql: "{source_a.count}"

      - name: price_eur
        type: sum
        multi_stage: true
        case:
          switch: "{CUBE.source}"
          when:
            - value: A
              sql: "{source_a.price_eur}"
            - value: B
              sql: "{source_b.price_eur}"
          else:
            sql: "{source_a.price_eur}"

      - name: price_usd
        type: sum
        multi_stage: true
        case:
          switch: "{CUBE.source}"
          when:
            - value: A
              sql: "{source_a.price_usd}"
            - value: B
              sql: "{source_b.price_usd}"
          else:
            sql: "{source_a.price_usd}"

      - name: price
        type: sum
        multi_stage: true
        case:
          switch: "{CUBE.currency}"
          when:
            - value: USD
              sql: "{CUBE.price_usd}"
            - value: EUR
              sql: "{CUBE.price_eur}"
          else:
            sql: "{CUBE.price_usd}"


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

    it('basic cross join by proxy dim', async () => dbRunner.runQueryTest({
      dimensions: ['orders.currency_ref'],
      timeDimensions: [
        {
          dimension: 'orders.date',
          granularity: 'year'
        }
      ],
      timezone: 'UTC'
    }, [
      {
        orders__currency_ref: 'EUR',
        orders__date_year: '2022-01-01T00:00:00.000Z'
      },
      {
        orders__currency_ref: 'GBP',
        orders__date_year: '2022-01-01T00:00:00.000Z'
      },
      {
        orders__currency_ref: 'USD',
        orders__date_year: '2022-01-01T00:00:00.000Z'
      },
      {
        orders__currency_ref: 'EUR',
        orders__date_year: '2023-01-01T00:00:00.000Z'
      },
      {
        orders__currency_ref: 'GBP',
        orders__date_year: '2023-01-01T00:00:00.000Z'
      },
      {
        orders__currency_ref: 'USD',
        orders__date_year: '2023-01-01T00:00:00.000Z'
      },
      {
        orders__currency_ref: 'EUR',
        orders__date_year: '2024-01-01T00:00:00.000Z'
      },
      {
        orders__currency_ref: 'GBP',
        orders__date_year: '2024-01-01T00:00:00.000Z'
      },
      {
        orders__currency_ref: 'USD',
        orders__date_year: '2024-01-01T00:00:00.000Z'
      },
      {
        orders__currency_ref: 'EUR',
        orders__date_year: '2025-01-01T00:00:00.000Z'
      },
      {
        orders__currency_ref: 'GBP',
        orders__date_year: '2025-01-01T00:00:00.000Z'
      },
      {
        orders__currency_ref: 'USD',
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

    it('basic double cross join by proxy', async () => dbRunner.runQueryTest({
      dimensions: ['orders.currency_and_strategy_ref'],
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
        orders__currency_and_strategy_ref: 'EUR-A',
        orders__date_year: '2024-01-01T00:00:00.000Z'
      },
      {
        orders__currency_and_strategy_ref: 'EUR-B',
        orders__date_year: '2024-01-01T00:00:00.000Z'
      },
      {
        orders__currency_and_strategy_ref: 'GBP-A',
        orders__date_year: '2024-01-01T00:00:00.000Z'
      },
      {
        orders__currency_and_strategy_ref: 'GBP-B',
        orders__date_year: '2024-01-01T00:00:00.000Z'
      },
      {
        orders__currency_and_strategy_ref: 'USD-A',
        orders__date_year: '2024-01-01T00:00:00.000Z'
      },
      {
        orders__currency_and_strategy_ref: 'USD-B',
        orders__date_year: '2024-01-01T00:00:00.000Z'
      },
      {
        orders__currency_and_strategy_ref: 'EUR-A',
        orders__date_year: '2025-01-01T00:00:00.000Z'
      },
      {
        orders__currency_and_strategy_ref: 'EUR-B',
        orders__date_year: '2025-01-01T00:00:00.000Z'
      },
      {
        orders__currency_and_strategy_ref: 'GBP-A',
        orders__date_year: '2025-01-01T00:00:00.000Z'
      },
      {
        orders__currency_and_strategy_ref: 'GBP-B',
        orders__date_year: '2025-01-01T00:00:00.000Z'
      },
      {
        orders__currency_and_strategy_ref: 'USD-A',
        orders__date_year: '2025-01-01T00:00:00.000Z'
      },
      {
        orders__currency_and_strategy_ref: 'USD-B',
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

    it('basic cross join with filters', async () => {
      const sqlAndParams = await dbRunner.runQueryTest({
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
      { joinGraph, cubeEvaluator, compiler });

      expect(sqlAndParams[0]).not.toMatch(/CROSS.+JOIN/);
    });

    it('basic cross join with filters proxy dim', async () => {
      const sqlAndParams = await dbRunner.runQueryTest({
        dimensions: ['orders.strategy_ref'],
        measures: ['orders.revenue'],
        timeDimensions: [
          {
            dimension: 'orders.date',
            granularity: 'year'
          }
        ],
        filters: [
          { dimension: 'orders.strategy_ref', operator: 'equals', values: ['B'] }
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
          orders__strategy_ref: 'B',
          orders__revenue: '5',
        },
        {
          orders__date_year: '2023-01-01T00:00:00.000Z',
          orders__strategy_ref: 'B',
          orders__revenue: '15',
        },
        {
          orders__date_year: '2024-01-01T00:00:00.000Z',
          orders__strategy_ref: 'B',
          orders__revenue: '30',
        },
        {
          orders__date_year: '2025-01-01T00:00:00.000Z',
          orders__strategy_ref: 'B',
          orders__revenue: '5',
        }
      ],
      { joinGraph, cubeEvaluator, compiler });

      expect(sqlAndParams[0]).not.toMatch(/CROSS.+JOIN/);
    });

    it('dimension switch expression simple', async () => dbRunner.runQueryTest({
      dimensions: ['orders.currency', 'orders.currency_full_name'],
      measures: ['orders.revenue'],
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
      },
      ],
    }, [
      {
        orders__currency: 'EUR',
        orders__currency_full_name: 'euros',
        orders__date_year: '2024-01-01T00:00:00.000Z',
        orders__revenue: '30'
      },
      {
        orders__currency: 'GBP',
        orders__currency_full_name: 'unknown',
        orders__date_year: '2024-01-01T00:00:00.000Z',
        orders__revenue: '30'
      },
      {
        orders__currency: 'USD',
        orders__currency_full_name: 'dollars',
        orders__date_year: '2024-01-01T00:00:00.000Z',
        orders__revenue: '30'
      },
      {
        orders__currency: 'EUR',
        orders__currency_full_name: 'euros',
        orders__date_year: '2025-01-01T00:00:00.000Z',
        orders__revenue: '5'
      },
      {
        orders__currency: 'GBP',
        orders__currency_full_name: 'unknown',
        orders__date_year: '2025-01-01T00:00:00.000Z',
        orders__revenue: '5'
      },
      {
        orders__currency: 'USD',
        orders__currency_full_name: 'dollars',
        orders__date_year: '2025-01-01T00:00:00.000Z',
        orders__revenue: '5'
      }
    ],
    { joinGraph, cubeEvaluator, compiler }));

    it('measure switch cross join', async () => dbRunner.runQueryTest({
      dimensions: ['orders.currency'],
      measures: ['orders.amount_usd', 'orders.amount_in_currency'],
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
      },
      ],
    }, [
      {
        orders__currency: 'EUR',
        orders__date_year: '2024-01-01T00:00:00.000Z',
        orders__amount_usd: '1030.0',
        orders__amount_in_currency: '1002'
      },
      {
        orders__currency: 'GBP',
        orders__date_year: '2024-01-01T00:00:00.000Z',
        orders__amount_usd: '1030.0',
        orders__amount_in_currency: '828'
      },
      {
        orders__currency: 'USD',
        orders__date_year: '2024-01-01T00:00:00.000Z',
        orders__amount_usd: '1030.0',
        orders__amount_in_currency: '1030.0'
      },
      {
        orders__currency: 'EUR',
        orders__date_year: '2025-01-01T00:00:00.000Z',
        orders__amount_usd: '40.0',
        orders__amount_in_currency: '38'
      },
      {
        orders__currency: 'GBP',
        orders__date_year: '2025-01-01T00:00:00.000Z',
        orders__amount_usd: '40.0',
        orders__amount_in_currency: '33'
      },
      {
        orders__currency: 'USD',
        orders__date_year: '2025-01-01T00:00:00.000Z',
        orders__amount_usd: '40.0',
        orders__amount_in_currency: '40.0'
      }
    ],
    { joinGraph, cubeEvaluator, compiler }));

    it('complex measure switch cross join', async () => dbRunner.runQueryTest({
      dimensions: ['orders.currency'],
      measures: ['orders.amount_in_currency_percent_of_usd'],
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
      },
      ],
    }, [
      {
        orders__currency: 'EUR',
        orders__date_year: '2024-01-01T00:00:00.000Z',
        orders__amount_in_currency_percent_of_usd: '97'
      },
      {
        orders__currency: 'GBP',
        orders__date_year: '2024-01-01T00:00:00.000Z',
        orders__amount_in_currency_percent_of_usd: '80'
      },
      {
        orders__currency: 'USD',
        orders__date_year: '2024-01-01T00:00:00.000Z',
        orders__amount_in_currency_percent_of_usd: '100'
      },
      {
        orders__currency: 'EUR',
        orders__date_year: '2025-01-01T00:00:00.000Z',
        orders__amount_in_currency_percent_of_usd: '95'
      },
      {
        orders__currency: 'GBP',
        orders__date_year: '2025-01-01T00:00:00.000Z',
        orders__amount_in_currency_percent_of_usd: '82'
      },
      {
        orders__currency: 'USD',
        orders__date_year: '2025-01-01T00:00:00.000Z',
        orders__amount_in_currency_percent_of_usd: '100'
      }
    ],
    { joinGraph, cubeEvaluator, compiler }));

    it('measure switch with filter', async () => {
      const sqlAndParams = await dbRunner.runQueryTest({
        dimensions: ['orders.currency'],
        measures: ['orders.amount_usd', 'orders.amount_in_currency'],
        timeDimensions: [
          {
            dimension: 'orders.date',
            granularity: 'year',
            dateRange: ['2024-01-01', '2026-01-01']
          }
        ],
        filters: [
          { dimension: 'orders.currency', operator: 'equals', values: ['EUR'] }
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
          orders__currency: 'EUR',
          orders__date_year: '2024-01-01T00:00:00.000Z',
          orders__amount_usd: '1030.0',
          orders__amount_in_currency: '1002'
        },
        {
          orders__currency: 'EUR',
          orders__date_year: '2025-01-01T00:00:00.000Z',
          orders__amount_usd: '40.0',
          orders__amount_in_currency: '38'
        },
      ],
      { joinGraph, cubeEvaluator, compiler });

      expect(sqlAndParams[0]).not.toMatch(/CASE/);
      expect(sqlAndParams[0]).not.toMatch(/CROSS.+JOIN/);
    });

    it('complex measure switch with filter', async () => {
      const sqlAndParams = await dbRunner.runQueryTest({
        dimensions: ['orders.currency'],
        measures: ['orders.amount_in_currency_percent_of_usd'],
        timeDimensions: [
          {
            dimension: 'orders.date',
            granularity: 'year',
            dateRange: ['2024-01-01', '2026-01-01']
          }
        ],
        filters: [
          { dimension: 'orders.currency', operator: 'equals', values: ['EUR'] }
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
          orders__currency: 'EUR',
          orders__date_year: '2024-01-01T00:00:00.000Z',
          orders__amount_in_currency_percent_of_usd: '97'
        },
        {
          orders__currency: 'EUR',
          orders__date_year: '2025-01-01T00:00:00.000Z',
          orders__amount_in_currency_percent_of_usd: '95'
        },
      ],
      { joinGraph, cubeEvaluator, compiler });

      expect(sqlAndParams[0]).not.toMatch(/CASE/);
      expect(sqlAndParams[0]).not.toMatch(/CROSS.+JOIN/);
    });
    it('source switch cross join', async () => {
      await dbRunner.runQueryTest({
        dimensions: ['source.source'],
        measures: ['source.count'],
        order: [{
          id: 'source.source'
        }
        ],
      }, [
        { source__source: 'A', source__count: '4' },
        { source__source: 'B', source__count: '5' }
      ],
      { joinGraph, cubeEvaluator, compiler });
    });
    it('source product_category cross join', async () => {
      await dbRunner.runQueryTest({
        dimensions: ['source.product_category'],
        order: [{
          id: 'source.product_category'
        }
        ],
      }, [
        { source__product_category: 'some category' },
        { source__product_category: 'some category A' },
        { source__product_category: 'some category B' }
      ],
      { joinGraph, cubeEvaluator, compiler });
    });
    it('source product_category and created_at cross join', async () => {
      await dbRunner.runQueryTest({
        dimensions: ['source.product_category'],
        timeDimensions: [
          {
            dimension: 'source.created_at',
            granularity: 'month'
          }
        ],
        timezone: 'UTC',
        order: [
          {
            id: 'source.created_at'
          },
          {
            id: 'source.product_category'
          }
        ],
      }, [
        {
          source__product_category: 'some category',
          source__created_at_month: '2022-01-01T00:00:00.000Z'
        },
        {
          source__product_category: 'some category',
          source__created_at_month: '2022-02-01T00:00:00.000Z'
        },
        {
          source__product_category: 'some category A',
          source__created_at_month: '2022-02-01T00:00:00.000Z'
        },
        {
          source__product_category: 'some category B',
          source__created_at_month: '2022-02-01T00:00:00.000Z'
        },
        {
          source__product_category: 'some category A',
          source__created_at_month: '2022-03-01T00:00:00.000Z'
        },
        {
          source__product_category: 'some category B',
          source__created_at_month: '2022-03-01T00:00:00.000Z'
        },
        {
          source__product_category: 'some category B',
          source__created_at_month: '2022-04-01T00:00:00.000Z'
        }
      ],
      { joinGraph, cubeEvaluator, compiler });
    });

    it('source product_category_ext filter', async () => {
      await dbRunner.runQueryTest({
        dimensions: ['source.product_category'],
        measures: ['source.price'],
        filters: [
          { dimension: 'source.product_category_ext', operator: 'equals', values: ['some category B-EUR-EUR'] }
        ],
        timezone: 'UTC',
        order: [
          {
            id: 'source.created_at'
          },
          {
            id: 'source.product_category_ext'
          }
        ],
      }, [
        {
          source__product_category: 'some category B',
          source__price: '800'
        },
      ],
      { joinGraph, cubeEvaluator, compiler });
    });

    it('source switch cross join without dimension', async () => {
      await dbRunner.runQueryTest({
        dimensions: ['source.product_category'],
        measures: ['source.count'],
        order: [{
          id: 'source.product_category'
        }
        ],
      }, [
        { source__product_category: 'some category', source__count: '4' },
        { source__product_category: 'some category A', source__count: '2' },
        { source__product_category: 'some category B', source__count: '3' }
      ],
      { joinGraph, cubeEvaluator, compiler });
    });
    it('source full switch', async () => {
      await dbRunner.runQueryTest({
        dimensions: ['source.currency', 'source.product_category'],
        measures: ['source.price'],
        order: [{
          id: 'source.product_category'
        },
        {
          id: 'source.currency'
        }
        ],
      }, [
        {
          source__currency: 'EUR',
          source__product_category: 'some category',
          source__price: '600'
        },
        {
          source__currency: 'USD',
          source__product_category: 'some category',
          source__price: '600'
        },
        {
          source__currency: 'EUR',
          source__product_category: 'some category A',
          source__price: '0'
        },
        {
          source__currency: 'USD',
          source__product_category: 'some category A',
          source__price: '500'
        },
        {
          source__currency: 'EUR',
          source__product_category: 'some category B',
          source__price: '800'
        },
        {
          source__currency: 'USD',
          source__product_category: 'some category B',
          source__price: '0'
        }
      ],
      { joinGraph, cubeEvaluator, compiler });
    });
    it('source full switch - td day', async () => {
      await dbRunner.runQueryTest({
        dimensions: ['source.currency', 'source.product_category'],
        timeDimensions: [
          {
            dimension: 'source.created_at',
            granularity: 'month',
          }
        ],
        timezone: 'UTC',
        order: [{
          id: 'source.created_at'
        },
        {
          id: 'source.product_category'
        },
        {
          id: 'source.currency'
        }
        ],
      }, [
        {
          source__product_category: 'some category',
          source__created_at_month: '2022-01-01T00:00:00.000Z',
          source__currency: 'EUR',
        },
        {
          source__product_category: 'some category',
          source__created_at_month: '2022-01-01T00:00:00.000Z',
          source__currency: 'USD',
        },
        {
          source__product_category: 'some category',
          source__created_at_month: '2022-02-01T00:00:00.000Z',
          source__currency: 'EUR',
        },
        {
          source__product_category: 'some category',
          source__created_at_month: '2022-02-01T00:00:00.000Z',
          source__currency: 'USD',
        },
        {
          source__product_category: 'some category A',
          source__created_at_month: '2022-02-01T00:00:00.000Z',
          source__currency: 'EUR',
        },
        {
          source__product_category: 'some category A',
          source__created_at_month: '2022-02-01T00:00:00.000Z',
          source__currency: 'USD',
        },
        {
          source__product_category: 'some category B',
          source__created_at_month: '2022-02-01T00:00:00.000Z',
          source__currency: 'EUR',
        },
        {
          source__product_category: 'some category B',
          source__created_at_month: '2022-02-01T00:00:00.000Z',
          source__currency: 'USD',
        },
        {
          source__product_category: 'some category A',
          source__created_at_month: '2022-03-01T00:00:00.000Z',
          source__currency: 'EUR',
        },
        {
          source__product_category: 'some category A',
          source__created_at_month: '2022-03-01T00:00:00.000Z',
          source__currency: 'USD',
        },
        {
          source__product_category: 'some category B',
          source__created_at_month: '2022-03-01T00:00:00.000Z',
          source__currency: 'EUR',
        },
        {
          source__product_category: 'some category B',
          source__created_at_month: '2022-03-01T00:00:00.000Z',
          source__currency: 'USD',
        },
        {
          source__product_category: 'some category B',
          source__created_at_month: '2022-04-01T00:00:00.000Z',
          source__currency: 'EUR',
        },
        {
          source__product_category: 'some category B',
          source__created_at_month: '2022-04-01T00:00:00.000Z',
          source__currency: 'USD',
        }
      ],
      { joinGraph, cubeEvaluator, compiler });
    });
    it('source full switch - price - td day and date range', async () => {
      await dbRunner.runQueryTest({
        dimensions: ['source.currency', 'source.product_category'],
        measures: ['source.price'],
        timeDimensions: [
          {
            dimension: 'source.created_at',
            granularity: 'month',
            dateRange: ['2022-02-01', '2022-04-01']
          }
        ],
        timezone: 'UTC',
        order: [{
          id: 'source.created_at'
        },
        {
          id: 'source.product_category'
        },
        {
          id: 'source.currency'
        }
        ],
      }, [
        {
          source__product_category: 'some category',
          source__created_at_month: '2022-02-01T00:00:00.000Z',
          source__price: '500',
          source__currency: 'EUR',
        },
        {
          source__product_category: 'some category',
          source__created_at_month: '2022-02-01T00:00:00.000Z',
          source__price: '0',
          source__currency: 'USD',
        },
        {
          source__product_category: 'some category A',
          source__created_at_month: '2022-02-01T00:00:00.000Z',
          source__price: '0',
          source__currency: 'EUR',
        },
        {
          source__product_category: 'some category A',
          source__created_at_month: '2022-02-01T00:00:00.000Z',
          source__price: '200',
          source__currency: 'USD',
        },
        {
          source__product_category: 'some category B',
          source__created_at_month: '2022-02-01T00:00:00.000Z',
          source__price: '200',
          source__currency: 'EUR',
        },
        {
          source__product_category: 'some category B',
          source__created_at_month: '2022-02-01T00:00:00.000Z',
          source__price: '0',
          source__currency: 'USD',
        },
        {
          source__product_category: 'some category A',
          source__created_at_month: '2022-03-01T00:00:00.000Z',
          source__price: '0',
          source__currency: 'EUR',
        },
        {
          source__product_category: 'some category A',
          source__created_at_month: '2022-03-01T00:00:00.000Z',
          source__price: '300',
          source__currency: 'USD',
        },
        {
          source__product_category: 'some category B',
          source__created_at_month: '2022-03-01T00:00:00.000Z',
          source__price: '300',
          source__currency: 'EUR',
        },
        {
          source__product_category: 'some category B',
          source__created_at_month: '2022-03-01T00:00:00.000Z',
          source__price: '0',
          source__currency: 'USD',
        },
      ],
      { joinGraph, cubeEvaluator, compiler });
    });
    it('source full switch - price - td day and date range', async () => {
      await dbRunner.runQueryTest({
        dimensions: ['source.currency', 'source.product_category'],
        measures: ['source.price'],
        timeDimensions: [
          {
            dimension: 'source.created_at',
            granularity: 'month',
            dateRange: ['2022-02-01', '2022-04-01']
          }
        ],
        timezone: 'UTC',
        order: [{
          id: 'source.created_at'
        },
        {
          id: 'source.product_category'
        },
        {
          id: 'source.currency'
        }
        ],
      }, [
        {
          source__product_category: 'some category',
          source__created_at_month: '2022-02-01T00:00:00.000Z',
          source__price: '500',
          source__currency: 'EUR',
        },
        {
          source__product_category: 'some category',
          source__created_at_month: '2022-02-01T00:00:00.000Z',
          source__price: '0',
          source__currency: 'USD',
        },
        {
          source__product_category: 'some category A',
          source__created_at_month: '2022-02-01T00:00:00.000Z',
          source__price: '0',
          source__currency: 'EUR',
        },
        {
          source__product_category: 'some category A',
          source__created_at_month: '2022-02-01T00:00:00.000Z',
          source__price: '200',
          source__currency: 'USD',
        },
        {
          source__product_category: 'some category B',
          source__created_at_month: '2022-02-01T00:00:00.000Z',
          source__price: '200',
          source__currency: 'EUR',
        },
        {
          source__product_category: 'some category B',
          source__created_at_month: '2022-02-01T00:00:00.000Z',
          source__price: '0',
          source__currency: 'USD',
        },
        {
          source__product_category: 'some category A',
          source__created_at_month: '2022-03-01T00:00:00.000Z',
          source__price: '0',
          source__currency: 'EUR',
        },
        {
          source__product_category: 'some category A',
          source__created_at_month: '2022-03-01T00:00:00.000Z',
          source__price: '300',
          source__currency: 'USD',
        },
        {
          source__product_category: 'some category B',
          source__created_at_month: '2022-03-01T00:00:00.000Z',
          source__price: '300',
          source__currency: 'EUR',
        },
        {
          source__product_category: 'some category B',
          source__created_at_month: '2022-03-01T00:00:00.000Z',
          source__price: '0',
          source__currency: 'USD',
        },
      ],
      { joinGraph, cubeEvaluator, compiler });
    });
    it('source switch - source_a + usd', async () => {
      await dbRunner.runQueryTest({
        dimensions: ['source.currency', 'source.product_category'],
        measures: ['source.price'],
        order: [{
          id: 'source.product_category'
        },
        ],
        filters: [
          { dimension: 'source.currency', operator: 'equals', values: ['USD'] },
          { dimension: 'source.source', operator: 'equals', values: ['A'] }
        ],
      }, [
        {
          source__currency: 'USD',
          source__product_category: 'some category',
          source__price: '600'
        },
        {
          source__currency: 'USD',
          source__product_category: 'some category A',
          source__price: '500'
        },
      ],
      { joinGraph, cubeEvaluator, compiler });
    });

    it('source switch - source_a + usd + filter by category', async () => {
      await dbRunner.runQueryTest({
        dimensions: ['source.currency', 'source.product_category'],
        measures: ['source.price'],
        order: [{
          id: 'source.product_category'
        },
        ],
        filters: [
          { dimension: 'source.currency', operator: 'equals', values: ['USD'] },
          { dimension: 'source.source', operator: 'equals', values: ['A'] },
          { dimension: 'source.product_category', operator: 'equals', values: ['some category'] },
        ],
      }, [
        {
          source__currency: 'USD',
          source__product_category: 'some category',
          source__price: '600'
        },
      ],
      { joinGraph, cubeEvaluator, compiler });
    });

    it('source switch - source_b + eur', async () => {
      await dbRunner.runQueryTest({
        dimensions: ['source.currency', 'source.product_category'],
        measures: ['source.price'],
        order: [{
          id: 'source.product_category'
        },
        ],
        filters: [
          { dimension: 'source.currency', operator: 'equals', values: ['EUR'] },
          { dimension: 'source.source', operator: 'equals', values: ['B'] }
        ],
      }, [
        {
          source__currency: 'EUR',
          source__product_category: 'some category',
          source__price: '600'
        },
        {
          source__currency: 'EUR',
          source__product_category: 'some category B',
          source__price: '800'
        },
      ],
      { joinGraph, cubeEvaluator, compiler });
    });
  } else {
    // This test is working only in tesseract
    test.skip('calc groups tests', () => { expect(1).toBe(1); });
  }
});
