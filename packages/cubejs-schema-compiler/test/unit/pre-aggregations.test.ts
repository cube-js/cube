import { prepareCompiler, prepareYamlCompiler } from './PrepareCompiler';
import { createECommerceSchema, createSchemaYaml } from './utils';
import { PostgresQuery, queryClass, QueryFactory } from '../../src';

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

  it('query rollupLambda', async () => {
    const { compiler, cubeEvaluator, joinGraph } = prepareCompiler(
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
            ordersRollupLambda: {
              type: \`rollupLambda\`,
              rollups: [simple1, simple2],
            },
            simple1: {
              measures: [CUBE.count],
              dimensions: [CUBE.status, Users.name],
              timeDimension: CUBE.created_at,
              granularity: 'day',
              partitionGranularity: 'day',
              buildRangeStart: {
                sql: \`SELECT NOW() - INTERVAL '1000 day'\`,
              },
              buildRangeEnd: {
                sql: \`SELECT NOW()\`
              },
            },
            simple2: {
              measures: [CUBE.count],
              dimensions: [CUBE.status, Users.name],
              timeDimension: CUBE.created_at,
              granularity: 'day',
              partitionGranularity: 'day',
              buildRangeStart: {
                sql: \`SELECT NOW() - INTERVAL '1000 day'\`,
              },
              buildRangeEnd: {
                sql: \`SELECT NOW()\`
              },
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
            created_at: {
              sql: \`created_at\`,
              type: \`time\`,
            },
          },
        });
      `
    );

    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'Orders.count'
      ],
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    expect(queryAndParams[0].includes('undefined')).toBeFalsy();
    expect(queryAndParams[0].includes('"orders__status" "orders__status"')).toBeTruthy();
    expect(queryAndParams[0].includes('"users__name" "users__name"')).toBeTruthy();
    expect(queryAndParams[0].includes('"orders__created_at_day" "orders__created_at_day"')).toBeTruthy();
    expect(queryAndParams[0].includes('"orders__count" "orders__count"')).toBeTruthy();
    expect(queryAndParams[0].includes('UNION ALL')).toBeTruthy();

    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    console.log(JSON.stringify(preAggregationsDescription, null, 2));

    expect(preAggregationsDescription.length).toEqual(2);
    expect(preAggregationsDescription[0].preAggregationId).toEqual('Orders.simple1');
    expect(preAggregationsDescription[1].preAggregationId).toEqual('Orders.simple2');
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

  it('pre-aggregation with indexes descriptions', async () => {
    const { compiler, cubeEvaluator, joinGraph } = prepareYamlCompiler(
      createSchemaYaml(createECommerceSchema())
    );

    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'orders_indexes.count'
      ],
      timeDimensions: [{
        dimension: 'orders_indexes.created_at',
        granularity: 'day',
        dateRange: ['2023-01-01', '2023-01-10']
      }],
      dimensions: ['orders_indexes.status']
    });

    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    const { indexesSql } = preAggregationsDescription[0];
    expect(indexesSql.length).toEqual(2);
    expect(indexesSql[0].indexName).toEqual('orders_indexes_orders_by_day_with_day_by_status_regular_index');
    expect(indexesSql[1].indexName).toEqual('orders_indexes_orders_by_day_with_day_by_status_agg_index');
  });

  it('pre-aggregation with FILTER_PARAMS', async () => {
    const { compiler, cubeEvaluator, joinGraph } = prepareYamlCompiler(
      createSchemaYaml({
        cubes: [
          {
            name: 'orders',
            sql_table: 'orders',
            measures: [{
              name: 'count',
              type: 'count',
            }],
            dimensions: [
              {
                name: 'created_at',
                sql: 'created_at',
                type: 'time',
              },
              {
                name: 'updated_at',
                sql: '{created_at}',
                type: 'time',
              },
              {
                name: 'status',
                sql: 'status',
                type: 'string',
              }
            ],
            preAggregations: [
              {
                name: 'orders_by_day_with_day',
                measures: ['count'],
                dimensions: ['status'],
                timeDimension: 'CUBE.created_at',
                granularity: 'day',
                partition_granularity: 'month',
                build_range_start: {
                  sql: 'SELECT \'2022-01-01\'::timestamp',
                },
                build_range_end: {
                  sql: 'SELECT \'2024-01-01\'::timestamp'
                },
                refresh_key: {
                  every: '4 hours',
                  sql: `
                    SELECT max(created_at) as max_created_at
                    FROM orders
                    WHERE {FILTER_PARAMS.orders.created_at.filter('date(created_at)')}`,
                },
              },
            ]
          }
        ]
      })
    );

    await compiler.compile();

    // It's important to provide a queryFactory, as it triggers flow
    // with paramAllocator reset in BaseQuery->newSubQueryForCube()
    const queryFactory = new QueryFactory(
      {
        orders: PostgresQuery
      }
    );

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'orders.count'
      ],
      timeDimensions: [{
        dimension: 'orders.created_at',
        granularity: 'day',
        dateRange: ['2023-01-01', '2023-01-10']
      }],
      dimensions: ['orders.status'],
      queryFactory
    });

    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    expect(preAggregationsDescription[0].loadSql[0].includes('WHERE ("orders".created_at >= $1::timestamptz AND "orders".created_at <= $2::timestamptz)')).toBeTruthy();
    expect(preAggregationsDescription[0].loadSql[1]).toEqual(['__FROM_PARTITION_RANGE', '__TO_PARTITION_RANGE']);
    expect(preAggregationsDescription[0].invalidateKeyQueries[0][0].includes('WHERE ((date(created_at) >= $1::timestamptz AND date(created_at) <= $2::timestamptz))')).toBeTruthy();
    expect(preAggregationsDescription[0].invalidateKeyQueries[0][1]).toEqual(['__FROM_PARTITION_RANGE', '__TO_PARTITION_RANGE']);
  });
});
