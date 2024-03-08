import { prepareCompiler, prepareYamlCompiler } from './PrepareCompiler';
import { createECommerceSchema, createSchemaYaml } from './utils';
import { PostgresQuery } from '../../src';

describe('pre-aggregations', () => {
  it('rollupJoin scheduledRefresh', async () => {
    process.env.CUBEJS_SCHEDULED_REFRESH_DEFAULT = 'true';
    const { compiler, cubeEvaluator } = prepareCompiler(
      `
        cube(\`Users\`, {
          sql: \`SELECT * FROM public.users\`,
        
          preAggregations: {
            usersRollup: {
              dimensions: [CUBE.id],
            },
          },
        
          measures: {
            count: {
              type: \`count\`,
            },
          },
        
          dimensions: {
            id: {
              sql: \`id\`,
              type: \`string\`,
              primaryKey: true,
            },
            
            name: {
              sql: \`name\`,
              type: \`string\`,
            },
          },
        });
        
        cube('Orders', {
          sql: \`SELECT * FROM orders\`,
        
          preAggregations: {
            ordersRollup: {
              measures: [CUBE.count],
              dimensions: [CUBE.status],
            },
            // Here we add a new pre-aggregation of type \`rollupJoin\`
            ordersRollupJoin: {
              type: \`rollupJoin\`,
              measures: [CUBE.count],
              dimensions: [Users.name],
              rollups: [Users.usersRollup, CUBE.ordersRollup],
            },
          },
        
          joins: {
            Users: {
              relationship: \`belongsTo\`,
              sql: \`\${CUBE.userId} = \${Users.id}\`,
            },
          },
        
          measures: {
            count: {
              type: \`count\`,
            },
          },
        
          dimensions: {
            id: {
              sql: \`id\`,
              type: \`number\`,
              primaryKey: true,
            },
            userId: {
              sql: \`user_id\`,
              type: \`number\`,
            },
            status: {
              sql: \`status\`,
              type: \`string\`,
            },
          },
        });
      `
    );

    await compiler.compile();

    expect(cubeEvaluator.cubeFromPath('Users').preAggregations.usersRollup.scheduledRefresh).toEqual(true);
    expect(cubeEvaluator.cubeFromPath('Orders').preAggregations.ordersRollup.scheduledRefresh).toEqual(true);
    expect(cubeEvaluator.cubeFromPath('Orders').preAggregations.ordersRollupJoin.scheduledRefresh).toEqual(undefined);
  });

  // @link https://github.com/cube-js/cube/issues/6623
  it('view and pre-aggregation granularity', async () => {
    const { compiler, cubeEvaluator, joinGraph } = prepareYamlCompiler(
      createSchemaYaml(createECommerceSchema())
    );

    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'orders_view.count'
      ],
      timeDimensions: [{
        dimension: 'orders_view.created_at',
        granularity: 'day',
        dateRange: ['2023-01-01', '2023-01-10']
      }],
      timezone: 'America/Los_Angeles'
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);

    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    console.log(JSON.stringify(preAggregationsDescription, null, 2));

    expect(preAggregationsDescription[0].preAggregationId).toEqual('orders.orders_by_day_with_day');
    expect(preAggregationsDescription[0].matchedTimeDimensionDateRange).toEqual([
      '2023-01-01T00:00:00.000',
      '2023-01-10T23:59:59.999'
    ]);
  });

  it('view and pre-aggregation granularity two level', async () => {
    const { compiler, cubeEvaluator, joinGraph } = prepareYamlCompiler(
      createSchemaYaml(createECommerceSchema())
    );

    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'orders_view.count'
      ],
      timeDimensions: [{
        dimension: 'orders_view.updated_at',
        granularity: 'day',
        dateRange: ['2023-01-01', '2023-01-10']
      }],
      timezone: 'America/Los_Angeles'
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);

    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    console.log(JSON.stringify(preAggregationsDescription, null, 2));

    expect(preAggregationsDescription[0].preAggregationId).toEqual('orders.orders_by_day_with_day');
    expect(preAggregationsDescription[0].matchedTimeDimensionDateRange).toEqual([
      '2023-01-01T00:00:00.000',
      '2023-01-10T23:59:59.999'
    ]);
  });

  it('view and pre-aggregation granularity with additional filters test', async () => {
    const { compiler, cubeEvaluator, joinGraph } = prepareYamlCompiler(
      createSchemaYaml(createECommerceSchema())
    );

    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'orders_view.count'
      ],
      timeDimensions: [{
        dimension: 'orders_view.created_at',
        granularity: 'day',
        dateRange: ['2023-01-01', '2023-01-10']
      }],
      filters: [{
        or: [
          {
            member: 'orders_view.status',
            operator: 'equals',
            values: [
              'finished'
            ]
          },
          {
            member: 'orders_view.status',
            operator: 'equals',
            values: [
              'pending'
            ]
          },
        ]
      }],
      timezone: 'America/Los_Angeles'
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);

    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    console.log(JSON.stringify(preAggregationsDescription, null, 2));

    expect(preAggregationsDescription[0].preAggregationId).toEqual('orders.orders_by_day_with_day_by_status');
    expect(preAggregationsDescription[0].matchedTimeDimensionDateRange).toEqual([
      '2023-01-01T00:00:00.000',
      '2023-01-10T23:59:59.999'
    ]);
  });
});
