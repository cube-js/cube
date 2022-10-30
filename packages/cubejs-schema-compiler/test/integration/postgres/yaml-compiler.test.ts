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
        type: countDistinct
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

  it('member reference', async () => {
    const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
cubes:
  - name: ActiveUsers
    sql: "SELECT 1 as user_id, '2022-01-01' as timestamp"
    
    measures:
      - name: weeklyActive
        sql: "{user_id}"
        type: countDistinct
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
    sql: "SELECT 1 as id, 1 as customer_id, '2022-01-01' as \\"timestamp\\" WHERE {FILTER_PARAMS.orders.time.filter(\\"timestamp\\")}"
    
    joins:
      - name: customers
        sql: "{orders}.customer_id = {customers}.id"
        relationship: belongsTo
    
    measures:
      - name: count
        type: count

    dimensions:
      - name: id
        sql: id
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
  - name: orders_view
    includes: 
      - orders.count
      - orders.time
      - customers.name
    `);
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['orders_view.count'],
      dimensions: ['orders_view.name'],
      timeDimensions: [{
        dimension: 'orders_view.time',
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
        orders_view__count: '1',
        orders_view__name: 'Foo',
        orders_view__time_day: '2022-01-01T00:00:00.000Z',
      }]
    );
  });
});
