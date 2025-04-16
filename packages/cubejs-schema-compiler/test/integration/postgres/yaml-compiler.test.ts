import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareCompiler, prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

describe('YAMLCompiler', () => {
  jest.setTimeout(200000);

  it('simple', async () => {
    const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
cubes:
  - name: ActiveUsers
    sql: "SELECT 1 as user_id, '2022-01-01' as timestamp"

    measures:
      - name: weeklyActive
        sql: "{CUBE}.user_id"
        type: count_distinct
        rollingWindow:
          trailing: 7 day
          offset: start

    dimensions:
      - name: time
        sql: "{CUBE}.timestamp"
        type: time
    `);
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['ActiveUsers.weeklyActive'],
      timeDimensions: [{
        dimension: 'ActiveUsers.time',
        granularity: 'day',
        dateRange: ['2022-01-01', '2022-01-03']
      }],
      timezone: 'UTC'
    });

    console.log(query.buildSqlAndParams());

    const res = await dbRunner.testQuery(query.buildSqlAndParams());
    console.log(JSON.stringify(res));

    expect(res).toEqual(
      [{
        active_users__time_day: '2022-01-01T00:00:00.000Z',
        active_users__weekly_active: '0',
      },
      {
        active_users__time_day: '2022-01-02T00:00:00.000Z',
        active_users__weekly_active: '1',
      },
      {
        active_users__time_day: '2022-01-03T00:00:00.000Z',
        active_users__weekly_active: '1',
      }]
    );
  });

  it('simple with json/curly in sql', async () => {
    const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
cubes:
  - name: ActiveUsers
    sql: SELECT 1 as user_id, '2022-01-01'::TIMESTAMP as timestamp, CAST('\\{"key":"value"\\}'::JSON AS TEXT) AS json_col

    dimensions:
      - name: time
        sql: "{CUBE}.timestamp"
        type: time
      - name: json_col
        sql: json_col
        type: string
    `);
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      dimensions: ['ActiveUsers.time', 'ActiveUsers.json_col'],
      timezone: 'UTC'
    });

    console.log(query.buildSqlAndParams());

    const res = await dbRunner.testQuery(query.buildSqlAndParams());
    console.log(JSON.stringify(res));

    expect(res).toEqual(
      [{
        active_users__time: '2022-01-01T00:00:00.000Z',
        active_users__json_col: '{"key":"value"}',
      }]
    );
  });

  it('missed sql', async () => {
    const { compiler } = prepareYamlCompiler(`
cubes:
  - name: ActiveUsers
    sql: "SELECT 1 as user_id, '2022-01-01' as timestamp"

    measures:
      - name: weeklyActive
        sql: "{CUBE}.user_id"
        type: count_distinct
        rollingWindow:
          trailing: 7 day
          offset: start

    dimensions:
      - name: time
        type: time
    `);
    expect(() => compiler.compile()).rejects.toThrow(/sql.*is required/);
  });

  it('with filter', async () => {
    const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
cubes:
  - name: ActiveUsers
    sql: "SELECT 1 as user_id, '2022-01-01'::timestamptz as timestamp"

    measures:
      - name: withFilter
        sql: "{CUBE}.user_id"
        type: count_distinct
        filters:
          - sql: "{CUBE}.user_id > 10"

    dimensions:
      - name: time
        sql: "{CUBE}.timestamp"
        type: time
    `);
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['ActiveUsers.withFilter'],
      timeDimensions: [{
        dimension: 'ActiveUsers.time',
        granularity: 'day',
        dateRange: ['2022-01-01', '2022-01-03']
      }],
      timezone: 'UTC'
    });

    console.log(query.buildSqlAndParams());

    const res = await dbRunner.testQuery(query.buildSqlAndParams());
    console.log(JSON.stringify(res));

    expect(res).toEqual(
      [{
        active_users__time_day: '2022-01-01T00:00:00.000Z',
        active_users__with_filter: '0',
      }]
    );
  });

  it('member reference', async () => {
    const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
cubes:
  - name: ActiveUsers
    sql: "SELECT 1 as user_id, '2022-01-01' as timestamp"

    measures:
      - name: weeklyActive
        sql: "{user_id}"
        type: count_distinct
        rollingWindow:
          trailing: 7 day
          offset: start

    dimensions:
      - name: user_id
        sql: "{CUBE}.user_id"
        type: number
      - name: time
        sql: "{CUBE}.timestamp"
        type: time
    `);
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['ActiveUsers.weeklyActive'],
      timeDimensions: [{
        dimension: 'ActiveUsers.time',
        granularity: 'day',
        dateRange: ['2022-01-01', '2022-01-03']
      }],
      timezone: 'UTC'
    });

    console.log(query.buildSqlAndParams());

    const res = await dbRunner.testQuery(query.buildSqlAndParams());
    console.log(JSON.stringify(res));

    expect(res).toEqual(
      [{
        active_users__time_day: '2022-01-01T00:00:00.000Z',
        active_users__weekly_active: '0',
      },
      {
        active_users__time_day: '2022-01-02T00:00:00.000Z',
        active_users__weekly_active: '1',
      },
      {
        active_users__time_day: '2022-01-03T00:00:00.000Z',
        active_users__weekly_active: '1',
      }]
    );
  });

  it('pre-aggregations', async () => {
    const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
cubes:
  - name: ActiveUsers
    sql: "SELECT 1 as user_id, '2022-01-01' as timestamp"

    measures:
      - name: weeklyActive
        sql: "{CUBE.user_id}"
        type: sum
        rollingWindow:
          trailing: 7 day
          offset: start

    dimensions:
      - name: user_id
        sql: "{CUBE}.user_id"
        type: number
      - name: time
        sql: "{CUBE}.timestamp"
        type: time

    preAggregations:
      - name: main
        measures:
          - weeklyActive
        timeDimension: time
        indexes:
          - name: weeklyActive
            columns:
              - weeklyActive
        granularity: day
    `);
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['ActiveUsers.weeklyActive'],
      timeDimensions: [{
        dimension: 'ActiveUsers.time',
        granularity: 'day',
        dateRange: ['2022-01-01', '2022-01-03']
      }],
      timezone: 'UTC',
      preAggregationsSchema: ''
    });

    console.log(query.buildSqlAndParams());

    const res = await dbRunner.evaluateQueryWithPreAggregations(query);
    console.log(JSON.stringify(res));

    expect(res).toEqual(
      [{
        active_users__time_day: '2022-01-01T00:00:00.000Z',
        active_users__weekly_active: null,
      },
      {
        active_users__time_day: '2022-01-02T00:00:00.000Z',
        active_users__weekly_active: '1',
      },
      {
        active_users__time_day: '2022-01-03T00:00:00.000Z',
        active_users__weekly_active: '1',
      }]
    );
  });

  it('filter params', async () => {
    const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
cubes:
  - name: active_users
    sql: "SELECT * FROM (SELECT 1 as user_id, '2022-01-01'::timestamptz as \\"timestamp\\") t WHERE {FILTER_PARAMS.active_users.time.filter(\\"timestamp\\")} AND {FILTER_PARAMS.active_users.time.filter(lambda a,b : f'timestamp >= {a}::timestamptz AND timestamp <= {b}::timestamptz')}"

    measures:
      - name: weekly_active
        sql: "{CUBE.user_id}"
        type: sum
        rollingWindow:
          trailing: 7 day
          offset: start

    dimensions:
      - name: user_id
        sql: "{CUBE}.user_id"
        type: number
      - name: time
        sql: "{CUBE}.timestamp"
        type: time
    `);
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['active_users.weekly_active'],
      timeDimensions: [{
        dimension: 'active_users.time',
        granularity: 'day',
        dateRange: ['2022-01-01', '2022-01-03']
      }],
      timezone: 'UTC',
      preAggregationsSchema: ''
    });

    console.log(query.buildSqlAndParams());

    const res = await dbRunner.evaluateQueryWithPreAggregations(query);
    console.log(JSON.stringify(res));

    expect(res).toEqual(
      [{
        active_users__time_day: '2022-01-01T00:00:00.000Z',
        active_users__weekly_active: null,
      },
      {
        active_users__time_day: '2022-01-02T00:00:00.000Z',
        active_users__weekly_active: '1',
      },
      {
        active_users__time_day: '2022-01-03T00:00:00.000Z',
        active_users__weekly_active: '1',
      }]
    );
  });

  it('joins', async () => {
    const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
cubes:
  - name: orders
    sql: "SELECT *
          FROM (
            SELECT
              1 as id,
              1 as customer_id,
              TO_TIMESTAMP('2022-01-01', 'YYYY-MM-DD') as timestamp
          ) sq
          WHERE {FILTER_PARAMS.orders.time.filter(\\"timestamp\\")}"

    joins:
      - name: customers
        sql: "{CUBE}.customer_id = {customers}.id"
        relationship: many_to_one

    measures:
      - name: count
        type: count

    dimensions:
      - name: id
        sql: "{CUBE}.id"
        type: string
        primary_key: true

      - name: time
        sql: "{CUBE}.timestamp"
        type: time

    preAggregations:
      - name: main
        measures: [orders.count]
        dimensions: [customers.name]
        time_dimension: orders.time
        granularity: day

  - name: line_items
    sql: "SELECT 1 as id, 1 as order_id, 100 as price"

    joins:
      - name: orders
        sql: "{CUBE.order_id} = {orders.id}"
        relationship: many_to_one

    dimensions:
      - name: id
        sql: "{CUBE}.id"
        type: string
        primary_key: true

      - name: order_id
        sql: "{CUBE}.order_id"
        type: number

      - name: price
        sql: "{CUBE}.price"
        type: number

    measures:
      - name: count
        type: count


  - name: customers
    sql: "SELECT 1 as id, 'Foo' as name"

    measures:
      - name: count
        type: count

    dimensions:
      - name: id
        sql: id
        type: string
        primary_key: true

      - name: name
        sql: "{CUBE}.name"
        type: string

views:
  - name: line_items_view

    cubes:
      - join_path: line_items
        includes: "*"

      - join_path: line_items.orders
        prefix: true
        includes: "*"
        excludes:
          - count

      - join_path: line_items.orders.customers
        alias: aliased_customers
        prefix: true
        includes:
          - name: name
            alias: full_name
    `);
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['line_items_view.count'],
      dimensions: ['line_items_view.aliased_customers_full_name'],
      timeDimensions: [{
        dimension: 'line_items_view.orders_time',
        granularity: 'day',
        dateRange: ['2022-01-01', '2022-01-03']
      }],
      timezone: 'UTC',
      preAggregationsSchema: ''
    });

    const res = await dbRunner.evaluateQueryWithPreAggregations(query);

    expect(res).toEqual(
      [{
        line_items_view__aliased_customers_full_name: 'Foo',
        line_items_view__count: '1',
        line_items_view__orders_time_day: '2022-01-01T00:00:00.000Z',
      }]
    );
  });

  it('extends', async () => {
    const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
cubes:
  - name: BaseUsers
    sql: "SELECT 1"

    dimensions:
      - name: time
        sql: "{CUBE}.timestamp"
        type: time

  - name: ActiveUsers
    sql: "SELECT 1 as user_id, '2022-01-01' as timestamp"
    extends: BaseUsers
    measures:
      - name: weeklyActive
        sql: "{CUBE}.user_id"
        type: count_distinct
        rollingWindow:
          trailing: 7 day
          offset: start
    `);
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['ActiveUsers.weeklyActive'],
      timeDimensions: [{
        dimension: 'ActiveUsers.time',
        granularity: 'day',
        dateRange: ['2022-01-01', '2022-01-03']
      }],
      timezone: 'UTC'
    });

    console.log(query.buildSqlAndParams());

    const res = await dbRunner.testQuery(query.buildSqlAndParams());
    console.log(JSON.stringify(res));

    expect(res).toEqual(
      [{
        active_users__time_day: '2022-01-01T00:00:00.000Z',
        active_users__weekly_active: '0',
      },
      {
        active_users__time_day: '2022-01-02T00:00:00.000Z',
        active_users__weekly_active: '1',
      },
      {
        active_users__time_day: '2022-01-03T00:00:00.000Z',
        active_users__weekly_active: '1',
      }]
    );
  });

  it('COMPILE_CONTEXT', async () => {
    const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
    cubes:
      - name: orders
        sql: "SELECT 1 as id, 'completed' as status"
        public: COMPILE_CONTEXT.security_context.can_see_orders

        measures:
          - name: count
            type: count
    `,
    {
      compileContext: {
        authInfo: null,
        securityContext: { can_see_orders: true },
        requestId: 'XXX'
      }
    });

    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'orders.count'
      ],
      timeDimensions: [],
      timezone: 'America/Los_Angeles'
    });

    return dbRunner.testQuery(query.buildSqlAndParams()).then(res => {
      expect(res).toEqual(
        [{ orders__count: '1' }]
      );
    });
  });

  it('view join ambiguity', async () => {
    const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
cubes:
  - name: W
    sql: |
      SELECT 1 as w_id, 1 as z_id

    measures: []

    dimensions:

      - name: w_id
        type: string
        sql: w_id
        primary_key: true

    joins:

      - name: Z
        sql: "{CUBE}.z_id = {Z}.z_id"
        relationship: many_to_one

  - name: M
    sql: |
      SELECT 1 as m_id, 1 as v_id, 1 as w_id

    measures:

      - name: count
        type: countDistinct
        sql: "{CUBE}.m_id"

    dimensions:

      - name: m_id
        type: string
        sql: m_id
        primary_key: true

    joins:

      - name: V
        sql: "{CUBE}.v_id = {V}.v_id"
        relationship: many_to_one

      - name: W
        sql: "{CUBE}.w_id = {W}.w_id"
        relationship: many_to_one

  - name: Z
    sql: >
      SELECT 1 as z_id, 'US' as COUNTRY

    dimensions:
      - name: country
        sql: "{CUBE}.COUNTRY"
        type: string

      - name: z_id
        sql: "{CUBE}.z_id"
        type: string
        primaryKey: true

  - name: V
    sql: |
      SELECT 1 as v_id, 1 as z_id

    dimensions:

      - name: v_id
        sql: "{CUBE}.v_id"
        type: string
        primary_key: true

    joins:

      - name: Z
        sql: "{CUBE}.z_id = {Z}.z_id"
        relationship: many_to_one


views:
  - name: m_view

    cubes:

      - join_path: M
        includes: "*"
        prefix: false

      - join_path: M.V
        includes: "*"
        prefix: true
        alias: v

      - join_path: M.W
        includes: "*"
        prefix: true
        alias: w

      - join_path: M.W.Z
        includes: "*"
        prefix: true
        alias: w_z
    `);
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['m_view.count'],
      dimensions: ['m_view.w_z_country'],
      timezone: 'UTC',
      preAggregationsSchema: ''
    });

    const res = await dbRunner.evaluateQueryWithPreAggregations(query);

    expect(res).toEqual(
      [{
        m_view__count: '1',
        m_view__w_z_country: 'US',
      }]
    );
  });

  it('calling cube\'s sql() (yaml-yaml)', async () => {
    const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(
      `cubes:
  - name: simple_orders
    sql: >
      SELECT 1 AS id, 100 AS amount, 'new' status, '2025-04-15'::TIMESTAMP AS created_at
      UNION ALL
      SELECT 2 AS id, 200 AS amount, 'new' status, '2025-04-16'::TIMESTAMP AS created_at
      UNION ALL
      SELECT 3 AS id, 300 AS amount, 'processed' status, '2025-04-17'::TIMESTAMP AS created_at
      UNION ALL
      SELECT 4 AS id, 500 AS amount, 'processed' status, '2025-04-18'::TIMESTAMP AS created_at
      UNION ALL
      SELECT 5 AS id, 600 AS amount, 'shipped' status, '2025-04-19'::TIMESTAMP AS created_at

    measures:
      - name: count
        type: count
      - name: total_amount
        sql: amount
        type: sum

    dimensions:
      - name: status
        sql: status
        type: string

  - name: simple_orders_sql_ext

    sql: >
      SELECT * FROM {simple_orders.sql()} as parent
      WHERE status = 'new'

    measures:
      - name: count
        type: count

      - name: total_amount
        sql: amount
        type: sum

    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

      - name: created_at
        sql: created_at
        type: time
    `
    );

    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['simple_orders_sql_ext.count'],
      timeDimensions: [{
        dimension: 'simple_orders_sql_ext.created_at',
        granularity: 'day',
        dateRange: ['2025-04-01', '2025-05-01']
      }],
      timezone: 'UTC',
      preAggregationsSchema: ''
    });

    const res = await dbRunner.evaluateQueryWithPreAggregations(query);

    expect(res).toEqual(
      [
        {
          simple_orders_sql_ext__count: '1',
          simple_orders_sql_ext__created_at_day: '2025-04-15T00:00:00.000Z',
        },
        {
          simple_orders_sql_ext__count: '1',
          simple_orders_sql_ext__created_at_day: '2025-04-16T00:00:00.000Z',
        }
      ]
    );
  });

  it('calling cube\'s sql() (yaml-js)', async () => {
    const { compiler, joinGraph, cubeEvaluator } = prepareCompiler([
      {
        content: `
cube('simple_orders', {
  sql: \`
      SELECT 1 AS id, 100 AS amount, 'new' status, '2025-04-15'::TIMESTAMP AS created_at
      UNION ALL
      SELECT 2 AS id, 200 AS amount, 'new' status, '2025-04-16'::TIMESTAMP AS created_at
      UNION ALL
      SELECT 3 AS id, 300 AS amount, 'processed' status, '2025-04-17'::TIMESTAMP AS created_at
      UNION ALL
      SELECT 4 AS id, 500 AS amount, 'processed' status, '2025-04-18'::TIMESTAMP AS created_at
      UNION ALL
      SELECT 5 AS id, 600 AS amount, 'shipped' status, '2025-04-19'::TIMESTAMP AS created_at
  \`,

  dimensions: {
    status: {
      sql: 'status',
      type: 'string',
    },
  },

  measures: {
    count: {
      type: 'count',
    },
    total_amount: {
      type: 'sum',
      sql: 'total_amount',
    },
  },
});
    `,
        fileName: 'cube.js',
      },
      {
        content: `cubes:
  - name: simple_orders_sql_ext

    sql: >
      SELECT * FROM {simple_orders.sql()} as parent
      WHERE status = 'new'

    measures:
      - name: count
        type: count

      - name: total_amount
        sql: amount
        type: sum

    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

      - name: created_at
        sql: created_at
        type: time
    `,
        fileName: 'cube.yml',
      },
    ]);

    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['simple_orders_sql_ext.count'],
      timeDimensions: [{
        dimension: 'simple_orders_sql_ext.created_at',
        granularity: 'day',
        dateRange: ['2025-04-01', '2025-05-01']
      }],
      timezone: 'UTC',
      preAggregationsSchema: ''
    });

    const res = await dbRunner.evaluateQueryWithPreAggregations(query);

    expect(res).toEqual(
      [
        {
          simple_orders_sql_ext__count: '1',
          simple_orders_sql_ext__created_at_day: '2025-04-15T00:00:00.000Z',
        },
        {
          simple_orders_sql_ext__count: '1',
          simple_orders_sql_ext__created_at_day: '2025-04-16T00:00:00.000Z',
        }
      ]
    );
  });
});
