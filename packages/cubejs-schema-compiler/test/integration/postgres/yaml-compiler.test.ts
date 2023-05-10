import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
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
    `, { yamlExtension: true });
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
    sql: "SELECT 1 as id, 1 as customer_id, TO_TIMESTAMP('2022-01-01', 'YYYY-MM-DD') as timestamp WHERE {FILTER_PARAMS.orders.time.filter(\\"timestamp\\")}"
    
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
    `, { yamlExtension: true });
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
    {},
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
});
