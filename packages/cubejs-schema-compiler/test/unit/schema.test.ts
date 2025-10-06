import fs from 'fs';
import path from 'path';
import { prepareCompiler, prepareJsCompiler, prepareYamlCompiler } from './PrepareCompiler';
import { createCubeSchema, createCubeSchemaWithCustomGranularitiesAndTimeShift, createCubeSchemaWithAccessPolicy } from './utils';

const CUBE_COMPONENTS = ['dimensions', 'measures', 'segments', 'hierarchies', 'preAggregations', 'joins'];

describe('Schema Testing', () => {
  const schemaCompile = async () => {
    const { compiler, cubeEvaluator } = prepareJsCompiler(
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

  describe('Cubes validations', () => {
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
          allowNonStrictDateRangeMatch: true,
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
          allowNonStrictDateRangeMatch: true,
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
          allowNonStrictDateRangeMatch: true,
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
          allowNonStrictDateRangeMatch: true,
        }
      });
    });

    it('invalid schema', async () => {
      const logger = jest.fn();

      const { compiler } = prepareJsCompiler(
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

    it('throws an error on duplicate member names', async () => {
      const orders = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/orders_dup_members.js'),
        'utf8'
      );

      const { compiler } = prepareCompiler([
        {
          content: orders,
          fileName: 'orders.js',
        },
      ]);

      try {
        await compiler.compile();
      } catch (e: any) {
        expect(e.toString()).toMatch(/status defined more than once/);
      }
    });

    it('throws errors for invalid pre-aggregations in yaml data model', async () => {
      const cubes = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/validate_preaggs.yml'),
        'utf8'
      );
      const { compiler } = prepareCompiler([
        {
          content: cubes,
          fileName: 'validate_preaggs.yml',
        },
      ]);

      try {
        await compiler.compile();
        throw new Error('should throw earlier');
      } catch (e: any) {
        expect(e.toString()).toMatch(/"preAggregations\.autoRollupFail\.maxPreAggregations" must be a number/);
        expect(e.toString()).toMatch(/"preAggregations\.originalSqlFail\.partitionGranularity" must be one of/);
        expect(e.toString()).toMatch(/"preAggregations\.originalSqlFail\.timeDimension" is required/);
        expect(e.toString()).toMatch(/"preAggregations\.originalSqlFail2\.uniqueKeyColumns" must be an array/);
        expect(e.toString()).toMatch(/"preAggregations\.originalSqlFail2\.timeDimension" is required/);
        expect(e.toString()).toMatch(/"preAggregations\.rollupJoinFail" does not match any of the allowed types/);
        expect(e.toString()).toMatch(/"preAggregations\.rollupLambdaFail\.partitionGranularity" is not allowed/);
        // TODO preAggregations.rollupFail.timeDimension - should catch that it is an array, currently not catching
        expect(e.toString()).toMatch(/"preAggregations\.rollupFail2\.timeDimensions" must be an array/);
      }
    });

    it('throws errors for invalid pre-aggregations in js data model', async () => {
      const cubes = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/validate_preaggs.js'),
        'utf8'
      );
      const { compiler } = prepareCompiler([
        {
          content: cubes,
          fileName: 'validate_preaggs.js',
        },
      ]);

      try {
        await compiler.compile();
        throw new Error('should throw earlier');
      } catch (e: any) {
        expect(e.toString()).toMatch(/"preAggregations\.autoRollupFail\.maxPreAggregations" must be a number/);
        expect(e.toString()).toMatch(/"preAggregations\.originalSqlFail\.partitionGranularity" must be one of/);
        expect(e.toString()).toMatch(/"preAggregations\.originalSqlFail\.timeDimension" is required/);
        expect(e.toString()).toMatch(/"preAggregations\.originalSqlFail2\.uniqueKeyColumns" must be an array/);
        expect(e.toString()).toMatch(/"preAggregations\.originalSqlFail2\.timeDimension" is required/);
        expect(e.toString()).toMatch(/"preAggregations\.rollupJoinFail" does not match any of the allowed types/);
        expect(e.toString()).toMatch(/"preAggregations\.rollupLambdaFail\.partitionGranularity" is not allowed/);
        // TODO preAggregations.rollupFail.timeDimension - should catch that it is an array, currently not catching
        expect(e.toString()).toMatch(/"preAggregations\.rollupFail2\.timeDimensions" must be an array/);
      }
    });
  });

  it('visibility modifier', async () => {
    const { compiler, metaTransformer } = prepareJsCompiler([
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
    const { compiler, metaTransformer } = prepareJsCompiler([
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
    const { compiler, metaTransformer } = prepareJsCompiler([
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
    const { compiler, metaTransformer } = prepareJsCompiler([
      createCubeSchemaWithCustomGranularitiesAndTimeShift('orders')
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
    expect(gr.interval).toBe('6 months');

    gr = dg.granularities.find(g => g.name === 'half_year_by_1st_april');
    expect(gr).toBeDefined();
    expect(gr.title).toBe('Half year from Apr to Oct');
    expect(gr.interval).toBe('6 months');
    expect(gr.offset).toBe('3 months');

    // // Granularity defined without title -> titlize()
    gr = dg.granularities.find(g => g.name === 'half_year_by_1st_june');
    expect(gr).toBeDefined();
    expect(gr.title).toBe('Half Year By1 St June');
    expect(gr.interval).toBe('6 months');
    expect(gr.origin).toBe('2020-06-01 10:00:00');
  });

  describe('Joins', () => {
    it('join types (joins as object)', async () => {
      const { compiler, cubeEvaluator } = prepareJsCompiler([
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

      expect(cubeEvaluator.cubeFromPath('CubeA').joins).toMatchSnapshot();
    });

    it('join types (joins as array)', async () => {
      const { compiler, cubeEvaluator } = prepareJsCompiler([
        createCubeSchema({
          name: 'CubeA',
          joins: `[
            {
              name: 'CubeB',
              sql: \`SQL ON clause\`,
              relationship: 'one_to_one'
            },
            {
              name: 'CubeC',
              sql: \`SQL ON clause\`,
              relationship: 'one_to_many'
            },
            {
              name: 'CubeD',
              sql: \`SQL ON clause\`,
              relationship: 'many_to_one'
            },
          ]`
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

      expect(cubeEvaluator.cubeFromPath('CubeA').joins).toMatchSnapshot();
    });
  });

  describe('Access Policies', () => {
    it('valid schema with accessPolicy', async () => {
      const { compiler } = prepareJsCompiler([
        createCubeSchemaWithAccessPolicy('ProtectedCube'),
      ]);
      await compiler.compile();
      compiler.throwIfAnyErrors();
    });

    it('throw errors for nonexistent policy members with paths', async () => {
      const orders = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/orders_nonexist_acl.yml'),
        'utf8'
      );
      const orderUsers = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/order_users.yml'),
        'utf8'
      );
      const { compiler } = prepareCompiler([
        {
          content: orders,
          fileName: 'orders.yml',
        },
        {
          content: orderUsers,
          fileName: 'order_users.yml',
        },
      ]);

      try {
        await compiler.compile();
        throw new Error('should throw earlier');
      } catch (e: any) {
        expect(e.toString()).toMatch(/orders.other cannot be resolved. There's no such member or cube/);
      }
    });

    it('throw errors for incorrect policy members with paths', async () => {
      const orders = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/orders_incorrect_acl.yml'),
        'utf8'
      );
      const orderUsers = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/order_users.yml'),
        'utf8'
      );
      const { compiler } = prepareCompiler([
        {
          content: orders,
          fileName: 'orders.yml',
        },
        {
          content: orderUsers,
          fileName: 'order_users.yml',
        },
      ]);

      try {
        await compiler.compile();
        throw new Error('should throw earlier');
      } catch (e: any) {
        expect(e.toString()).toMatch(/Paths aren't allowed in the accessPolicy policy but 'order_users.name' provided as a filter member reference for orders/);
      }
    });
  });

  describe('Views', () => {
    it('extends custom granularities and timeshifts', async () => {
      const { compiler, metaTransformer } = prepareJsCompiler([
        createCubeSchemaWithCustomGranularitiesAndTimeShift('orders')
      ]);
      await compiler.compile();

      const { measures, dimensions } = metaTransformer.cubeEvaluator.evaluatedCubes.orders_view;
      expect(dimensions.createdAt).toMatchSnapshot();
      expect(measures.count_shifted_year).toMatchSnapshot();
    });

    it('views extends views', async () => {
      const modelContent = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/folders.yml'),
        'utf8'
      );
      const { compiler, metaTransformer } = prepareYamlCompiler(modelContent);
      await compiler.compile();

      const testView3 = metaTransformer.cubeEvaluator.evaluatedCubes.test_view3;
      expect(testView3.dimensions).toMatchSnapshot();
      expect(testView3.measures).toMatchSnapshot();
      expect(testView3.measures).toMatchSnapshot();
      expect(testView3.hierarchies).toMatchSnapshot();
      expect(testView3.folders).toMatchSnapshot();
    });

    it('throws errors for incorrect referenced includes members', async () => {
      const orders = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/orders.js'),
        'utf8'
      );
      const ordersView = `
        views:
          - name: orders_view
            cubes:
              - join_path: orders
                includes:
                  - id
                  - status
                  - nonexistent1
                  - nonexistent2.via.path
      `;

      const { compiler } = prepareCompiler([
        {
          content: orders,
          fileName: 'orders.js',
        },
        {
          content: ordersView,
          fileName: 'order_view.yml',
        },
      ]);

      try {
        await compiler.compile();
        throw new Error('should throw earlier');
      } catch (e: any) {
        expect(e.toString()).toMatch(/Paths aren't allowed in cube includes but 'nonexistent2\.via\.path' provided as include member/);
        expect(e.toString()).toMatch(/Member 'nonexistent1' is included in 'orders_view' but not defined in any cube/);
        expect(e.toString()).toMatch(/Member 'nonexistent2\.via\.path' is included in 'orders_view' but not defined in any cube/);
      }
    });

    it('throws errors for incorrect referenced excludes members with paths', async () => {
      const orders = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/orders.js'),
        'utf8'
      );
      const ordersView = `
        views:
          - name: orders_view
            cubes:
              - join_path: orders
                includes: "*"
                excludes:
                  - id
                  - status
                  - nonexistent3.ext
                  - nonexistent4
      `;

      const { compiler } = prepareCompiler([
        {
          content: orders,
          fileName: 'orders.js',
        },
        {
          content: ordersView,
          fileName: 'order_view.yml',
        },
      ]);

      try {
        await compiler.compile();
        throw new Error('should throw earlier');
      } catch (e: any) {
        expect(e.toString()).toMatch(/Paths aren't allowed in cube excludes but 'nonexistent3.ext' provided as exclude member/);
      }
    });

    it('throws errors for incorrect referenced excludes members with path', async () => {
      const orders = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/orders.js'),
        'utf8'
      );
      const ordersView = `
        views:
          - name: orders_view
            cubes:
              - join_path: orders
                includes: "*"
                excludes:
                  - id
                  - status
                  - nonexistent5.via.path
      `;

      const { compiler } = prepareCompiler([
        {
          content: orders,
          fileName: 'orders.js',
        },
        {
          content: ordersView,
          fileName: 'order_view.yml',
        },
      ]);

      try {
        await compiler.compile();
        throw new Error('should throw earlier');
      } catch (e: any) {
        expect(e.toString()).toMatch(/Paths aren't allowed in cube excludes but 'nonexistent5\.via\.path' provided as exclude member/);
      }
    });

    it('throws errors for conflicting members of included cubes', async () => {
      const orders = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/orders_big.js'),
        'utf8'
      );
      const orderUsers = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/order_users.yml'),
        'utf8'
      );
      const ordersView = `
        views:
          - name: orders_view
            cubes:
              - join_path: orders
                includes: "*"
              - join_path: orders.order_users
                includes: "*"
      `;

      const { compiler } = prepareCompiler([
        {
          content: orders,
          fileName: 'orders.js',
        },
        {
          content: orderUsers,
          fileName: 'order_users.yml',
        },
        {
          content: ordersView,
          fileName: 'order_view.yml',
        },
      ]);

      try {
        await compiler.compile();
        throw new Error('should throw earlier');
      } catch (e: any) {
        expect(e.toString()).toMatch(/Included member 'count' conflicts with existing member of 'orders_view'\. Please consider excluding this member or assigning it an alias/);
        expect(e.toString()).toMatch(/Included member 'id' conflicts with existing member of 'orders_view'\. Please consider excluding this member or assigning it an alias/);
      }
    });

    it('allows to override `title`, `description`, `meta`, and `format` on includes members', async () => {
      const orders = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/orders.js'),
        'utf8'
      );
      const ordersView = `
        views:
          - name: orders_view
            cubes:
              - join_path: orders
                includes:
                - name: status
                  alias: my_beloved_status
                  title: My Favorite and not Beloved Status!
                  description: Don't you believe this?
                  meta:
                    - whose: mine
                    - what: status

                - name: created_at
                  alias: my_beloved_created_at
                  title: My Favorite and not Beloved created_at!
                  description: Created at this point in time
                  meta:
                    - c1: iddqd
                    - c2: idkfa

                - name: count
                  title: My Overridden Count!
                  description: It's not possible!
                  format: percent
                  meta:
                    - whose: bread
                    - what: butter
                    - why: cheese

                - name: hello
                  title: My Overridden hierarchy!
      `;

      const { compiler, cubeEvaluator } = prepareCompiler([
        {
          content: orders,
          fileName: 'orders.js',
        },
        {
          content: ordersView,
          fileName: 'order_view.yml',
        },
      ]);

      await compiler.compile();
      compiler.throwIfAnyErrors();

      const cubeB = cubeEvaluator.cubeFromPath('orders_view');
      expect(cubeB).toMatchSnapshot();
    });
  });

  describe('Inheritance', () => {
    it('CubeB.js correctly extends cubeA.js (no additions)', async () => {
      const orders = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/orders_big.js'),
        'utf8'
      );
      const orderUsers = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/order_users.yml'),
        'utf8'
      );
      const ordersExt = 'cube(\'ordersExt\', { extends: orders })';

      const { compiler, cubeEvaluator } = prepareCompiler([
        {
          content: orders,
          fileName: 'orders.js',
        },
        {
          content: ordersExt,
          fileName: 'orders_ext.js',
        },
        {
          content: orderUsers,
          fileName: 'order_users.yml',
        },
      ]);
      await compiler.compile();
      compiler.throwIfAnyErrors();

      const cubeA = cubeEvaluator.cubeFromPath('orders');
      const cubeB = cubeEvaluator.cubeFromPath('ordersExt');

      CUBE_COMPONENTS.forEach(c => {
        expect(cubeA[c]).toEqual(cubeB[c]);
      });

      // accessPolicies are evaluated so they must ref cube's own members and not parent's ones.
      expect(cubeA.accessPolicy).toMatchSnapshot('accessPolicy');
      expect(cubeB.accessPolicy).toMatchSnapshot('accessPolicy');
    });

    it('CubeB.js correctly extends cubeA.js (with additions)', async () => {
      const orders = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/orders_big.js'),
        'utf8'
      );
      const orderUsers = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/order_users.yml'),
        'utf8'
      );
      const orderLineItems = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/line_items.yml'),
        'utf8'
      );
      const ordersExt = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/orders_ext.js'),
        'utf8'
      );

      const { compiler, cubeEvaluator } = prepareCompiler([
        {
          content: orders,
          fileName: 'orders.js',
        },
        {
          content: ordersExt,
          fileName: 'orders_ext.js',
        },
        {
          content: orderUsers,
          fileName: 'order_users.yml',
        },
        {
          content: orderLineItems,
          fileName: 'line_items.yml',
        },
      ]);
      await compiler.compile();
      compiler.throwIfAnyErrors();

      const cubeA = cubeEvaluator.cubeFromPath('orders');
      const cubeB = cubeEvaluator.cubeFromPath('ordersExt');

      CUBE_COMPONENTS.forEach(c => {
        expect(cubeA[c]).toMatchSnapshot(c);
        expect(cubeB[c]).toMatchSnapshot(c);
      });
    });

    it('CubeB.yml correctly extends cubeA.yml (no additions)', async () => {
      const orders = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/orders_big.yml'),
        'utf8'
      );
      const orderUsers = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/order_users.yml'),
        'utf8'
      );
      const ordersExt = `
    cubes:
      - name: ordersExt
        extends: orders
      `;

      const { compiler, cubeEvaluator } = prepareCompiler([
        {
          content: orders,
          fileName: 'orders.yml',
        },
        {
          content: ordersExt,
          fileName: 'orders_ext.yml',
        },
        {
          content: orderUsers,
          fileName: 'order_users.yml',
        },
      ]);
      await compiler.compile();
      compiler.throwIfAnyErrors();

      const cubeA = cubeEvaluator.cubeFromPath('orders');
      const cubeB = cubeEvaluator.cubeFromPath('ordersExt');

      CUBE_COMPONENTS.forEach(c => {
        expect(cubeA[c]).toEqual(cubeB[c]);
      });

      // accessPolicies are evaluated so they must ref cube's own members and not parent's ones.
      expect(cubeA.accessPolicy).toMatchSnapshot('accessPolicy');
      expect(cubeB.accessPolicy).toMatchSnapshot('accessPolicy');
    });

    it('CubeB.yml correctly extends cubeA.yml (with additions)', async () => {
      const orders = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/orders_big.yml'),
        'utf8'
      );
      const orderUsers = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/order_users.yml'),
        'utf8'
      );
      const orderLineItems = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/line_items.yml'),
        'utf8'
      );
      const ordersExt = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/orders_ext.yml'),
        'utf8'
      );

      const { compiler, cubeEvaluator } = prepareCompiler([
        {
          content: orders,
          fileName: 'orders.yml',
        },
        {
          content: ordersExt,
          fileName: 'orders_ext.yml',
        },
        {
          content: orderUsers,
          fileName: 'order_users.yml',
        },
        {
          content: orderLineItems,
          fileName: 'line_items.yml',
        },
      ]);
      await compiler.compile();
      compiler.throwIfAnyErrors();

      const cubeA = cubeEvaluator.cubeFromPath('orders');
      const cubeB = cubeEvaluator.cubeFromPath('ordersExt');

      CUBE_COMPONENTS.forEach(c => {
        expect(cubeA[c]).toMatchSnapshot(c);
        expect(cubeB[c]).toMatchSnapshot(c);
      });
    });

    it('CubeB.yml correctly extends cubeA.js (no additions)', async () => {
      const orders = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/orders_big.js'),
        'utf8'
      );
      const orderUsers = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/order_users.yml'),
        'utf8'
      );
      const ordersExt = `
    cubes:
      - name: ordersExt
        extends: orders
      `;

      const { compiler, cubeEvaluator } = prepareCompiler([
        {
          content: orders,
          fileName: 'orders.js',
        },
        {
          content: ordersExt,
          fileName: 'orders_ext.yml',
        },
        {
          content: orderUsers,
          fileName: 'order_users.yml',
        },
      ]);
      await compiler.compile();
      compiler.throwIfAnyErrors();

      const cubeA = cubeEvaluator.cubeFromPath('orders');
      const cubeB = cubeEvaluator.cubeFromPath('ordersExt');

      CUBE_COMPONENTS.forEach(c => {
        expect(cubeA[c]).toEqual(cubeB[c]);
      });

      // accessPolicies are evaluated so they must ref cube's own members and not parent's ones.
      expect(cubeA.accessPolicy).toMatchSnapshot('accessPolicy');
      expect(cubeB.accessPolicy).toMatchSnapshot('accessPolicy');
    });

    it('CubeB.yml correctly extends cubeA.js (with additions)', async () => {
      const orders = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/orders_big.js'),
        'utf8'
      );
      const orderUsers = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/order_users.yml'),
        'utf8'
      );
      const orderLineItems = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/line_items.yml'),
        'utf8'
      );
      const ordersExt = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/orders_ext.yml'),
        'utf8'
      );

      const { compiler, cubeEvaluator } = prepareCompiler([
        {
          content: orders,
          fileName: 'orders.js',
        },
        {
          content: ordersExt,
          fileName: 'orders_ext.yml',
        },
        {
          content: orderUsers,
          fileName: 'order_users.yml',
        },
        {
          content: orderLineItems,
          fileName: 'line_items.yml',
        },
      ]);
      await compiler.compile();
      compiler.throwIfAnyErrors();

      const cubeA = cubeEvaluator.cubeFromPath('orders');
      const cubeB = cubeEvaluator.cubeFromPath('ordersExt');

      CUBE_COMPONENTS.forEach(c => {
        expect(cubeA[c]).toMatchSnapshot(c);
        expect(cubeB[c]).toMatchSnapshot(c);
      });
    });

    it('CubeB.js correctly extends cubeA.yml (no additions)', async () => {
      const orders = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/orders_big.yml'),
        'utf8'
      );
      const orderUsers = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/order_users.yml'),
        'utf8'
      );
      const ordersExt = 'cube(\'ordersExt\', { extends: orders })';

      const { compiler, cubeEvaluator } = prepareCompiler([
        {
          content: orders,
          fileName: 'orders.yml',
        },
        {
          content: ordersExt,
          fileName: 'orders_ext.js',
        },
        {
          content: orderUsers,
          fileName: 'order_users.yml',
        },
      ]);
      await compiler.compile();
      compiler.throwIfAnyErrors();

      const cubeA = cubeEvaluator.cubeFromPath('orders');
      const cubeB = cubeEvaluator.cubeFromPath('ordersExt');

      CUBE_COMPONENTS.forEach(c => {
        expect(cubeA[c]).toEqual(cubeB[c]);
      });

      // accessPolicies are evaluated so they must ref cube's own members and not parent's ones.
      expect(cubeA.accessPolicy).toMatchSnapshot('accessPolicy');
      expect(cubeB.accessPolicy).toMatchSnapshot('accessPolicy');
    });

    it('CubeB.js correctly extends cubeA.yml (with additions)', async () => {
      const orders = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/orders_big.yml'),
        'utf8'
      );
      const orderUsers = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/order_users.yml'),
        'utf8'
      );
      const orderLineItems = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/line_items.yml'),
        'utf8'
      );
      const ordersExt = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/orders_ext.js'),
        'utf8'
      );

      const { compiler, cubeEvaluator } = prepareCompiler([
        {
          content: orders,
          fileName: 'orders.yml',
        },
        {
          content: ordersExt,
          fileName: 'orders_ext.js',
        },
        {
          content: orderUsers,
          fileName: 'order_users.yml',
        },
        {
          content: orderLineItems,
          fileName: 'line_items.yml',
        },
      ]);
      await compiler.compile();
      compiler.throwIfAnyErrors();

      const cubeA = cubeEvaluator.cubeFromPath('orders');
      const cubeB = cubeEvaluator.cubeFromPath('ordersExt');

      CUBE_COMPONENTS.forEach(c => {
        expect(cubeA[c]).toMatchSnapshot(c);
        expect(cubeB[c]).toMatchSnapshot(c);
      });
    });

    it('CubeB.js correctly extends cubeA.yml (with sql override)', async () => {
      const orders = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/orders_big.yml'),
        'utf8'
      );
      const orderUsers = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/order_users.yml'),
        'utf8'
      );
      const ordersExt = 'cube(\'ordersExt\', { extends: orders, sql: "SELECT * FROM other_orders" })';

      const { compiler, cubeEvaluator } = prepareCompiler([
        {
          content: orders,
          fileName: 'orders.yml',
        },
        {
          content: ordersExt,
          fileName: 'orders_ext.js',
        },
        {
          content: orderUsers,
          fileName: 'order_users.yml',
        },
      ]);
      await compiler.compile();
      compiler.throwIfAnyErrors();

      const cubeA = cubeEvaluator.cubeFromPath('orders');
      const cubeB = cubeEvaluator.cubeFromPath('ordersExt');

      CUBE_COMPONENTS.forEach(c => {
        expect(cubeA[c]).toEqual(cubeB[c]);
      });

      expect(cubeB.sql).toBeTruthy();
      expect(cubeB.sqlTable).toBeFalsy();
    });

    it('CubeB.yml correctly extends cubeA.js (with sql_table override)', async () => {
      const orders = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/orders_big.js'),
        'utf8'
      );
      const orderUsers = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/order_users.yml'),
        'utf8'
      );
      const ordersExt = `
    cubes:
      - name: ordersExt
        sql_table: orders_override
        extends: orders
      `;

      const { compiler, cubeEvaluator } = prepareCompiler([
        {
          content: orders,
          fileName: 'orders.js',
        },
        {
          content: ordersExt,
          fileName: 'orders_ext.yml',
        },
        {
          content: orderUsers,
          fileName: 'order_users.yml',
        },
      ]);
      await compiler.compile();
      compiler.throwIfAnyErrors();

      const cubeA = cubeEvaluator.cubeFromPath('orders');
      const cubeB = cubeEvaluator.cubeFromPath('ordersExt');

      CUBE_COMPONENTS.forEach(c => {
        expect(cubeA[c]).toEqual(cubeB[c]);
      });

      expect(cubeB.sqlTable).toBeTruthy();
      expect(cubeB.sql).toBeFalsy();
    });

    it('throws errors for invalid members in both cubes (parent and child)', async () => {
      const cubes = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/invalid_cubes.yaml'),
        'utf8'
      );
      const { compiler } = prepareCompiler([
        {
          content: cubes,
          fileName: 'invalid_cubes.yaml',
        },
      ]);

      try {
        await compiler.compile();
        throw new Error('should throw earlier');
      } catch (e: any) {
        expect(e.toString()).toMatch(/"measures\.parent_meas_no_type\.sql" is required/);
        expect(e.toString()).toMatch(/"measures\.parent_meas_no_type\.type" is required/);
        expect(e.toString()).toMatch(/"measures\.parent_meas_bad_type\.type" must be one of/);
        expect(e.toString()).toMatch(/"dimensions\.parent_dim_no_type" does not match any of the allowed types/);
        expect(e.toString()).toMatch(/"dimensions\.parent_dim_no_sql" does not match any of the allowed types/);
        expect(e.toString()).toMatch(/"dimensions\.child_dim_no_type" does not match any of the allowed types/);
        expect(e.toString()).toMatch(/"dimensions\.child_dim_bad_type" does not match any of the allowed types/);
        expect(e.toString()).toMatch(/"dimensions\.child_dim_no_sql" does not match any of the allowed types/);
      }
    });
  });

  describe('Calendar Cubes', () => {
    it('Valid calendar cubes', async () => {
      const orders = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/calendar_orders.yml'),
        'utf8'
      );
      const customCalendarJs = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/custom_calendar.js'),
        'utf8'
      );
      const customCalendarYaml = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/custom_calendar.yml'),
        'utf8'
      );

      const { compiler, cubeEvaluator } = prepareCompiler([
        {
          content: orders,
          fileName: 'calendar_orders.yml',
        },
        {
          content: customCalendarJs,
          fileName: 'custom_calendar.js',
        },
        {
          content: customCalendarYaml,
          fileName: 'custom_calendar.yml',
        },
      ]);
      await compiler.compile();
      compiler.throwIfAnyErrors();

      const customCalendarJsCube = cubeEvaluator.cubeFromPath('custom_calendar_js');
      const customCalendarYamlCube = cubeEvaluator.cubeFromPath('custom_calendar');

      expect(customCalendarJsCube).toMatchSnapshot('customCalendarJsCube');
      expect(customCalendarYamlCube).toMatchSnapshot('customCalendarYamlCube');
    });

    it('CubeB.js correctly extends cubeA.js (no additions)', async () => {
      const customCalendarJs = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/custom_calendar.js'),
        'utf8'
      );
      const customCalendarJsExt = 'cube(\'custom_calendar_js_ext\', { extends: custom_calendar_js })';

      const { compiler, cubeEvaluator } = prepareCompiler([
        {
          content: customCalendarJs,
          fileName: 'custom_calendar.js',
        },
        {
          content: customCalendarJsExt,
          fileName: 'custom_calendar_ext.js',
        },
      ]);
      await compiler.compile();
      compiler.throwIfAnyErrors();

      const cubeA = cubeEvaluator.cubeFromPath('custom_calendar_js');
      const cubeB = cubeEvaluator.cubeFromPath('custom_calendar_js_ext');

      CUBE_COMPONENTS.forEach(c => {
        expect(cubeA[c]).toEqual(cubeB[c]);
      });
    });

    it('CubeB.yml correctly extends cubeA.js (no additions)', async () => {
      const customCalendarYaml = fs.readFileSync(
        path.join(process.cwd(), '/test/unit/fixtures/custom_calendar.yml'),
        'utf8'
      );
      const customCalendarJsExt = 'cube(\'custom_calendar_js_ext\', { extends: custom_calendar })';

      const { compiler, cubeEvaluator } = prepareCompiler([
        {
          content: customCalendarYaml,
          fileName: 'custom_calendar.yml',
        },
        {
          content: customCalendarJsExt,
          fileName: 'custom_calendar_ext.js',
        },
      ]);
      await compiler.compile();
      compiler.throwIfAnyErrors();

      const cubeA = cubeEvaluator.cubeFromPath('custom_calendar');
      const cubeB = cubeEvaluator.cubeFromPath('custom_calendar_js_ext');

      CUBE_COMPONENTS.forEach(c => {
        expect(cubeA[c]).toEqual(cubeB[c]);
      });
    });
  });
});
