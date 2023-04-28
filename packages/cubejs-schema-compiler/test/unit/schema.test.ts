import { prepareCompiler } from './PrepareCompiler';
import { createCubeSchema } from './utils';

describe('Schema Testing', () => {
  const schemaCompile = async () => {
    const { compiler, cubeEvaluator } = prepareCompiler(
      createCubeSchema({
        name: 'CubeA',
        preAggregations: `
          main: {
                type: 'originalSql',
                timeDimension: createdAt,
                partitionGranularity: \`month\`,
                refreshRangeStart: {
                  sql: 'SELECT NOW()',
                },
                refreshRangeEnd: {
                  sql: 'SELECT NOW()',
                }
            },
            // Pre-aggregation without type, rollup is used by default
            countCreatedAt: {
                measureReferences: [count],
                timeDimensionReference: createdAt,
                granularity: \`day\`,
                partitionGranularity: \`month\`,
                buildRangeStart: {
                  sql: 'SELECT NOW()',
                },
                buildRangeEnd: {
                  sql: 'SELECT NOW()',
                }
            },
            countCreatedAtWithoutReferences: {
                dimensions: [type],
                measures: [count],
                timeDimension: [createdAt],
                segments: [sfUsers],
                granularity: \`day\`,
                partitionGranularity: \`month\`,
                buildRangeStart: {
                  sql: 'SELECT NOW()',
                },
                buildRangeEnd: {
                  sql: 'SELECT NOW()',
                }
            }
        `
      })
    );
    await compiler.compile();

    return { compiler, cubeEvaluator };
  };

  it('valid schemas', async () => {
    const { cubeEvaluator } = await schemaCompile();

    expect(cubeEvaluator.preAggregationsForCube('CubeA')).toEqual({
      main: {
        external: false,
        scheduledRefresh: true,
        timeDimensionReference: expect.any(Function),
        partitionGranularity: 'month',
        type: 'originalSql',
        refreshRangeStart: {
          sql: expect.any(Function),
        },
        refreshRangeEnd: {
          sql: expect.any(Function),
        },
      },
      countCreatedAt: {
        external: true,
        scheduledRefresh: true,
        granularity: 'day',
        measureReferences: expect.any(Function),
        timeDimensionReference: expect.any(Function),
        partitionGranularity: 'month',
        type: 'rollup',
        refreshRangeStart: {
          sql: expect.any(Function),
        },
        refreshRangeEnd: {
          sql: expect.any(Function),
        },
      },
      countCreatedAtWithoutReferences: {
        // because preview
        external: true,
        scheduledRefresh: true,
        granularity: 'day',
        measureReferences: expect.any(Function),
        timeDimensionReference: expect.any(Function),
        segmentReferences: expect.any(Function),
        dimensionReferences: expect.any(Function),
        partitionGranularity: 'month',
        type: 'rollup',
        refreshRangeStart: {
          sql: expect.any(Function),
        },
        refreshRangeEnd: {
          sql: expect.any(Function),
        },
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
        refreshRangeStart: {
          sql: expect.any(Function),
        },
        refreshRangeEnd: {
          sql: expect.any(Function),
        },
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
        refreshRangeStart: {
          sql: expect.any(Function),
        },
        refreshRangeEnd: {
          sql: expect.any(Function),
        },
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
        refreshRangeStart: {
          sql: expect.any(Function),
        },
        refreshRangeEnd: {
          sql: expect.any(Function),
        },
      }
    });
  });

  it('invalid schema', async () => {
    const logger = jest.fn();

    const { compiler } = prepareCompiler(
      createCubeSchema({
        name: 'CubeA',
        preAggregations: `
            main: {
                type: 'originalSql',
                timeDimension: createdAt,
                partitionGranularity: \`month\`,
                refreshRangeStart: {
                  sql: 'SELECT NOW()',
                },
                buildRangeStart: {
                  sql: 'SELECT NOW()',
                },
                refreshRangeEnd: {
                  sql: 'SELECT NOW()',
                },
                buildRangeEnd: {
                  sql: 'SELECT NOW()',
                }
            },
          `
      }),
      {
        omitErrors: true,
        errorReport: {
          logger,
        }
      }
    );

    await compiler.compile();
    compiler.throwIfAnyErrors();

    expect(logger.mock.calls.length).toEqual(2);
    expect(logger.mock.calls[0]).toEqual([
      'You specified both buildRangeStart and refreshRangeStart, buildRangeStart will be used.'
    ]);
    expect(logger.mock.calls[1]).toEqual([
      'You specified both buildRangeEnd and refreshRangeEnd, buildRangeEnd will be used.'
    ]);
  });

  it('visibility modifier', async () => {
    const { compiler, metaTransformer } = prepareCompiler([
      createCubeSchema({
        name: 'CubeA',
        publicly: false
      }),
      createCubeSchema({
        name: 'CubeB',
        publicly: true
      }),
      createCubeSchema({
        name: 'CubeC',
        shown: false
      })
    ]);
    await compiler.compile();

    expect(metaTransformer.cubes[0]).toMatchObject({
      isVisible: false,
      config: {
        name: 'CubeA',
      }
    });
    expect(metaTransformer.cubes[1]).toMatchObject({
      isVisible: true,
      config: {
        name: 'CubeB',
      }
    });
    expect(metaTransformer.cubes[2]).toMatchObject({
      isVisible: false,
      config: {
        name: 'CubeC',
      }
    });
  });

  it('join types', async () => {
    const { compiler, cubeEvaluator } = prepareCompiler([
      createCubeSchema({
        name: 'CubeA',
        joins: `{
          CubeB: {
            sql: \`SQL ON clause\`,
            relationship: 'one_to_one'
          },
          CubeC: {
            sql: \`SQL ON clause\`,
            relationship: 'one_to_many'
          },
          CubeD: {
            sql: \`SQL ON clause\`,
            relationship: 'many_to_one'
          },
        }`
      }),
      createCubeSchema({
        name: 'CubeB',
      }),
      createCubeSchema({
        name: 'CubeC',
      }),
      createCubeSchema({
        name: 'CubeD',
      }),
    ]);
    await compiler.compile();

    expect(cubeEvaluator.cubeFromPath('CubeA').joins).toMatchObject({
      CubeB: { relationship: 'hasOne' },
      CubeC: { relationship: 'hasMany' },
      CubeD: { relationship: 'belongsTo' }
    });
  });
});
