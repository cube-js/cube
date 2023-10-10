import { prepareCompiler } from './PrepareCompiler';

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
});
