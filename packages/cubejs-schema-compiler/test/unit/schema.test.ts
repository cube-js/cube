import { prepareCompiler } from './PrepareCompiler';

describe('Schema Testing', () => {
  const schemaCompile = async () => {
    const { compiler, cubeEvaluator } = prepareCompiler(` 
      cube('CubeA', {
        sql: \`select * from test\`,
   
        measures: {
          count: {
            type: 'count'
          }
        },
  
        dimensions: {
          id: {
            type: 'number',
            sql: 'id',
            primaryKey: true
          },
          createdAt: {
            type: 'time',
            sql: 'created_at'
          },
        },

        preAggregations: {
            // Pre-aggregation without type, rollup is used by default
            countCreatedAt: {
                measureReferences: [count],
                timeDimensionReference: createdAt,
                granularity: \`day\`,
                partitionGranularity: \`month\`
            }
        },
      }) 
    `);
    await compiler.compile();

    return { compiler, cubeEvaluator };
  };

  it('valid schemas', async () => {
    const { cubeEvaluator } = await schemaCompile();

    expect(cubeEvaluator.preAggregationsForCube('CubeA')).toEqual({
      countCreatedAt: {
        external: false,
        scheduledRefresh: false,
        granularity: 'day',
        measureReferences: expect.any(Function),
        timeDimensionReference: expect.any(Function),
        partitionGranularity: 'month',
        type: 'rollup',
      }
    });
  });

  it('valid schemas (preview flags)', async () => {
    process.env.CUBEJS_EXTERNAL_DEFAULT = 'true';
    process.env.CUBEJS_SCHEDULED_REFRESH_DEFAULT = 'true';

    const { cubeEvaluator } = await schemaCompile();

    delete process.env.CUBEJS_EXTERNAL_DEFAULT;
    delete process.env.CUBEJS_SCHEDULED_REFRESH_DEFAULT;

    expect(cubeEvaluator.preAggregationsForCube('CubeA')).toEqual({
      countCreatedAt: {
        // because preview
        external: true,
        scheduledRefresh: true,
        granularity: 'day',
        measureReferences: expect.any(Function),
        timeDimensionReference: expect.any(Function),
        partitionGranularity: 'month',
        type: 'rollup',
      }
    });
  });
});
