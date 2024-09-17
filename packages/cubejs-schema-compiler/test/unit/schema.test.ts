import { prepareCompiler } from './PrepareCompiler';
import { createCubeSchema, createCubeSchemaWithCustomGranularities } from './utils';

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
      config: {
        isVisible: false,
        name: 'CubeA',
      }
    });
    expect(metaTransformer.cubes[1]).toMatchObject({
      config: {
        isVisible: true,
        name: 'CubeB',
      }
    });
    expect(metaTransformer.cubes[2]).toMatchObject({
      config: {
        isVisible: false,
        name: 'CubeC',
      }
    });
  });

  it('dimensions', async () => {
    const { compiler, metaTransformer } = prepareCompiler([
      createCubeSchema({
        name: 'CubeA',
        publicly: false,
      }),
    ]);
    await compiler.compile();

    const { dimensions } = metaTransformer.cubes[0].config;

    expect(dimensions).toBeDefined();
    expect(dimensions.length).toBeGreaterThan(0);
    expect(dimensions.every((dimension) => dimension.primaryKey)).toBeDefined();
    expect(dimensions.every((dimension) => typeof dimension.primaryKey === 'boolean')).toBe(true);
    expect(dimensions.find((dimension) => dimension.name === 'CubeA.id').primaryKey).toBe(true);
    expect(dimensions.find((dimension) => dimension.name === 'CubeA.type').primaryKey).toBe(false);
  });

  it('descriptions', async () => {
    const { compiler, metaTransformer } = prepareCompiler([
      createCubeSchema({
        name: 'CubeA',
        publicly: false,
      }),
    ]);
    await compiler.compile();

    const { description, dimensions, measures, segments } = metaTransformer.cubes[0].config;

    expect(description).toBe('test cube from createCubeSchema');

    expect(dimensions).toBeDefined();
    expect(dimensions.length).toBeGreaterThan(0);
    expect(dimensions.find((dimension) => dimension.name === 'CubeA.id').description).toBe('id dimension from createCubeSchema');

    expect(measures).toBeDefined();
    expect(measures.length).toBeGreaterThan(0);
    expect(measures.find((measure) => measure.name === 'CubeA.count').description).toBe('count measure from createCubeSchema');

    expect(segments).toBeDefined();
    expect(segments.length).toBeGreaterThan(0);
    expect(segments.find((segment) => segment.name === 'CubeA.sfUsers').description).toBe('SF users segment from createCubeSchema');
  });

  it('custom granularities in meta', async () => {
    const { compiler, metaTransformer } = prepareCompiler([
      createCubeSchemaWithCustomGranularities('orders')
    ]);
    await compiler.compile();

    const { dimensions } = metaTransformer.cubes[0].config;

    expect(dimensions).toBeDefined();
    expect(dimensions.length).toBeGreaterThan(0);

    const dg = dimensions.find((dimension) => dimension.name === 'orders.createdAt');
    expect(dg).toBeDefined();
    expect(dg.granularities).toBeDefined();
    expect(dg.granularities.length).toBeGreaterThan(0);

    // Granularity defined with title
    let gr = dg.granularities.find(g => g.name === 'half_year');
    expect(gr).toBeDefined();
    expect(gr.title).toBe('6 month intervals');

    // // Granularity defined without title -> titlize()
    gr = dg.granularities.find(g => g.name === 'half_year_by_1st_june');
    expect(gr).toBeDefined();
    expect(gr.title).toBe('Half Year By1 St June');
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
