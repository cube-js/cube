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
          type: {
            type: 'string',
            sql: 'type',
          },
          createdAt: {
            type: 'time',
            sql: 'created_at'
          },
        },
        
        segments: {
          sfUsers: {
            sql: \`\${CUBE}.location = 'San Francisco'\`
          }
        },

        preAggregations: {
            main: {
                type: 'originalSql',
                timeDimension: createdAt,
                partitionGranularity: \`month\`,
            },
            // Pre-aggregation without type, rollup is used by default
            countCreatedAt: {
                measureReferences: [count],
                timeDimensionReference: createdAt,
                granularity: \`day\`,
                partitionGranularity: \`month\`
            },
            countCreatedAtWithoutReferences: {
                dimensions: [type],
                measures: [count],
                timeDimension: [createdAt],
                segments: [sfUsers],
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
      main: {
        external: false,
        scheduledRefresh: false,
        timeDimensionReference: expect.any(Function),
        partitionGranularity: 'month',
        type: 'originalSql',
      },
      countCreatedAt: {
        external: false,
        scheduledRefresh: false,
        granularity: 'day',
        measureReferences: expect.any(Function),
        timeDimensionReference: expect.any(Function),
        partitionGranularity: 'month',
        type: 'rollup',
      },
      countCreatedAtWithoutReferences: {
        // because preview
        external: false,
        scheduledRefresh: false,
        granularity: 'day',
        measureReferences: expect.any(Function),
        timeDimensionReference: expect.any(Function),
        segmentReferences: expect.any(Function),
        dimensionReferences: expect.any(Function),
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
      main: {
        external: false,
        scheduledRefresh: true,
        timeDimensionReference: expect.any(Function),
        partitionGranularity: 'month',
        type: 'originalSql',
      },
      countCreatedAt: {
        // because preview
        external: true,
        scheduledRefresh: true,
        granularity: 'day',
        measureReferences: expect.any(Function),
        timeDimensionReference: expect.any(Function),
        partitionGranularity: 'month',
        type: 'rollup',
      },
      countCreatedAtWithoutReferences: {
        // because preview
        external: true,
        scheduledRefresh: true,
        granularity: 'day',
        measureReferences: expect.any(Function),
        segmentReferences: expect.any(Function),
        dimensionReferences: expect.any(Function),
        timeDimensionReference: expect.any(Function),
        partitionGranularity: 'month',
        type: 'rollup',
      }
    });
  });
});
