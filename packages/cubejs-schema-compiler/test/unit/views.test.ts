import { prepareYamlCompiler } from './PrepareCompiler';
import { createSchemaYaml } from './utils';

describe('Views YAML', () => {
  const schemaCompile = async (views: unknown[]) => {
    const { compiler, cubeEvaluator, metaTransformer } = prepareYamlCompiler(
      createSchemaYaml({
        cubes: [
          {
            name: 'CubeA',
            sql_table: 'cube_a',
            dimensions: [{
              name: 'id',
              sql: 'id',
              type: 'number',
              description: 'Description for CubeA.id',
              title: 'Title for CubeA.id',
              format: 'imageUrl',
              meta: {
                key: 'Meta.key for CubeA.id'
              },
              primary_key: true,
            }],
            joins: [{
              name: 'CubeC',
              relationship: 'one_to_one',
              sql: 'SQL clause',
            }],
            measures: [{
              name: 'count_a',
              type: 'number',
              sql: 'count(*)',
              meta: {
                key: 'Meta.key for CubeA.count_a'
              },
              title: 'Title for CubeA.count_a',
              description: 'Description for CubeA.count_a',
              format: 'number',
            }]
          },
          {
            name: 'CubeB',
            sql_table: 'cube_b',
            dimensions: [
              {
                name: 'id',
                sql: 'id',
                type: 'number',
                description: 'Description for CubeB.id',
                title: 'Title for CubeB.id',
                format: 'imageUrl',
                meta: {
                  key: 'Meta.key for CubeB.id'
                },
                primary_key: true,
              },
              {
                name: 'other_id',
                sql: 'other_id',
                type: 'number',
                description: 'Description for CubeB.other_id',
                title: 'Title for CubeB.other_id',
                format: 'imageUrl',
                meta: {
                  key: 'Meta.key for CubeB.other_id'
                },
              }
            ],
            measures: [{
              name: 'count_b',
              type: 'number',
              sql: 'count(*)',
              meta: {
                key: 'Meta.key for CubeB.count_b'
              },
              title: 'Title for CubeB.count_b',
              description: 'Description for CubeB.count_b',
              format: 'number',
            }]
          },
          // A -> C
          {
            name: 'CubeC',
            sql_table: 'cube_b',
            dimensions: [
              {
                name: 'id',
                sql: 'id',
                type: 'number',
                primary_key: true,
                description: 'Description for CubeC.id',
                title: 'Title for CubeC.id',
                format: 'imageUrl',
                meta: {
                  key: 'Meta.key for CubeC.id'
                },
              },
              {
                name: 'dimension_1',
                sql: 'dimension_1',
                type: 'number',
                description: 'Description for CubeC.dimension_1',
                title: 'Title for CubeC.dimension_1',
                format: 'imageUrl',
                meta: {
                  key: 'Meta.key for CubeC.dimension_1'
                },
              }
            ],
            measures: [{
              name: 'count_c',
              type: 'number',
              sql: 'count(*)',
              meta: {
                key: 'Meta.key for CubeC.count_c'
              },
              title: 'Title for CubeC.count_c',
              description: 'Description for CubeC.count_c',
              format: 'number',
            }]
          },
          {
            name: 'CubeBChild',
            extends: 'CubeB',
          }
        ],
        views
      })
    );
    await compiler.compile();

    return { compiler, cubeEvaluator, metaTransformer };
  };

  function dimensionFixtureForCube(aliasName: string, name: string = aliasName) {
    return {
      description: `Description for ${name}`,
      meta: {
        key: `Meta.key for ${name}`
      },
      ownedByCube: false,
      sql: expect.any(Function),
      aliasMember: aliasName,
      format: 'imageUrl',
      title: `Title for ${name}`,
      type: 'number',
    };
  }

  function measuresFixtureForCube(aliasName: string, name: string = aliasName) {
    return {
      description: `Description for ${name}`,
      meta: {
        key: `Meta.key for ${name}`
      },
      ownedByCube: false,
      sql: expect.any(Function),
      aliasMember: aliasName,
      format: 'number',
      aggType: 'number',
      title: `Title for ${name}`,
      type: 'number',
    };
  }

  it('includes * + prefix a,b,c', async () => {
    const { cubeEvaluator, metaTransformer } = await schemaCompile([{
      name: 'simple_view',
      cubes: [
        {
          join_path: 'CubeA',
          prefix: true,
          includes: '*'
        },
        {
          join_path: 'CubeB',
          prefix: true,
          includes: '*'
        },
      ]
    }]);

    const simpleViewDef = cubeEvaluator.getCubeDefinition('simple_view');

    expect(simpleViewDef.dimensions).toEqual({
      CubeA_id: dimensionFixtureForCube('CubeA.id'),
      CubeB_id: dimensionFixtureForCube('CubeB.id'),
      CubeB_other_id: dimensionFixtureForCube('CubeB.other_id'),
    });

    expect(simpleViewDef.measures).toEqual({
      CubeA_count_a: measuresFixtureForCube('CubeA.count_a'),
      CubeB_count_b: measuresFixtureForCube('CubeB.count_b'),
    });

    const simpleViewMeta = metaTransformer.cubes.map((def) => def.config).find((def) => def.name === 'simple_view');
    expect(simpleViewMeta).toBeDefined();

    expect(simpleViewMeta).toMatchSnapshot();
  });

  it('includes * + prefix a,b + exclude ids', async () => {
    const { cubeEvaluator, metaTransformer } = await schemaCompile([{
      name: 'simple_view',
      cubes: [
        {
          join_path: 'CubeA',
          prefix: true,
          includes: '*',
          excludes: [
            'id'
          ]
        },
        {
          join_path: 'CubeB',
          prefix: true,
          includes: '*',
          excludes: [
            'id'
          ]
        },
      ]
    }]);

    expect(cubeEvaluator.getCubeDefinition('simple_view').dimensions).toEqual({
      CubeB_other_id: dimensionFixtureForCube('CubeB.other_id'),
    });

    const simpleViewMeta = metaTransformer.cubes.map((def) => def.config).find((def) => def.name === 'simple_view');
    expect(simpleViewMeta).toBeDefined();

    expect(simpleViewMeta).toMatchSnapshot();
  });

  it('includes * + prefix b + exclude ids', async () => {
    const { cubeEvaluator, metaTransformer } = await schemaCompile([{
      name: 'simple_view',
      cubes: [
        {
          join_path: 'CubeA',
          includes: '*',
          excludes: [
            'id'
          ]
        },
        {
          join_path: 'CubeB',
          prefix: true,
          includes: '*',
          excludes: [
            'id'
          ]
        },
      ]
    }]);

    expect(cubeEvaluator.getCubeDefinition('simple_view').dimensions).toEqual({
      CubeB_other_id: dimensionFixtureForCube('CubeB.other_id'),
    });

    const simpleViewMeta = metaTransformer.cubes.map((def) => def.config).find((def) => def.name === 'simple_view');
    expect(simpleViewMeta).toBeDefined();

    expect(simpleViewMeta).toMatchSnapshot();
  });

  it('includes * (a,b) + exclude id from b', async () => {
    const { cubeEvaluator, metaTransformer } = await schemaCompile([{
      name: 'simple_view',
      cubes: [
        {
          join_path: 'CubeA',
          includes: '*',
        },
        {
          join_path: 'CubeB',
          includes: '*',
          excludes: [
            'id'
          ]
        },
      ]
    }]);

    expect(cubeEvaluator.getCubeDefinition('simple_view').dimensions).toEqual({
      id: dimensionFixtureForCube('CubeA.id'),
      other_id: dimensionFixtureForCube('CubeB.other_id'),
    });

    const simpleViewMeta = metaTransformer.cubes.map((def) => def.config).find((def) => def.name === 'simple_view');
    expect(simpleViewMeta).toBeDefined();

    expect(simpleViewMeta).toMatchSnapshot();
  });

  it('includes * (a,b, a.c) with prefix + exclude id from b,c', async () => {
    const { cubeEvaluator } = await schemaCompile([{
      name: 'simple_view',
      cubes: [
        {
          join_path: 'CubeA',
          includes: '*',
        },
        {
          join_path: 'CubeB',
          includes: '*',
          prefix: true,
          excludes: [
            'id'
          ]
        },
        {
          join_path: 'CubeA.CubeC',
          includes: '*',
          prefix: true,
          excludes: [
            'id'
          ]
        },
      ]
    }]);

    expect(cubeEvaluator.getCubeDefinition('simple_view').dimensions).toEqual({
      id: dimensionFixtureForCube('CubeA.id'),
      CubeB_other_id: dimensionFixtureForCube('CubeB.other_id'),
      CubeC_dimension_1: dimensionFixtureForCube('CubeC.dimension_1'),
    });
  });

  it('includes * (a,b, a.c) + exclude id from b,c', async () => {
    const { cubeEvaluator } = await schemaCompile([{
      name: 'simple_view',
      cubes: [
        {
          join_path: 'CubeA',
          includes: '*',
        },
        {
          join_path: 'CubeB',
          includes: '*',
          excludes: [
            'id'
          ]
        },
        {
          join_path: 'CubeA.CubeC',
          includes: '*',
          excludes: [
            'id'
          ]
        },
      ]
    }]);

    expect(cubeEvaluator.getCubeDefinition('simple_view').dimensions).toEqual({
      id: dimensionFixtureForCube('CubeA.id'),
      other_id: dimensionFixtureForCube('CubeB.other_id'),
      dimension_1: dimensionFixtureForCube('CubeC.dimension_1')
    });
  });

  it('includes * a, bchild) + exclude id from b,c', async () => {
    const { cubeEvaluator } = await schemaCompile([{
      name: 'simple_view',
      cubes: [
        {
          join_path: 'CubeA',
          includes: '*',
        },
        {
          join_path: 'CubeBChild',
          includes: '*',
          excludes: [
            'id'
          ]
        },
        {
          join_path: 'CubeA.CubeC',
          includes: '*',
          excludes: [
            'id'
          ]
        },
      ]
    }]);

    expect(cubeEvaluator.getCubeDefinition('simple_view').dimensions).toEqual({
      id: dimensionFixtureForCube('CubeA.id'),
      other_id: dimensionFixtureForCube('CubeBChild.other_id', 'CubeB.other_id'),
      dimension_1: dimensionFixtureForCube('CubeC.dimension_1')
    });
  });

  it('includes * (legacy)', async () => {
    const { cubeEvaluator } = await schemaCompile([{
      name: 'simple_view',
      includes: [
        'CubeA.id',
        // conflict
        // 'CubeB.id',
        'CubeB.other_id',
      ]
    }]);

    expect(cubeEvaluator.getCubeDefinition('simple_view').dimensions).toEqual({
      id: dimensionFixtureForCube('CubeA.id'),
      other_id: dimensionFixtureForCube('CubeB.other_id'),
    });
  });

  it('includes * (legacy) + exclude b.id', async () => {
    const { cubeEvaluator } = await schemaCompile([{
      name: 'simple_view',
      includes: [
        'CubeA.id',
        'CubeB.id',
        'CubeB.other_id',
      ],
      excludes: [
        'CubeB.id'
      ]
    }]);

    expect(cubeEvaluator.getCubeDefinition('simple_view').dimensions).toEqual({
      id: dimensionFixtureForCube('CubeA.id'),
      other_id: dimensionFixtureForCube('CubeB.other_id'),
    });
  });

  it('throws error for unresolved members', async () => {
    const { compiler } = prepareYamlCompiler(`
      cubes:
        - name: orders
          sql: SELECT * FROM orders
          measures:
            - name: count
              type: count
          dimensions:
            - name: id
              sql: id
              type: number
              primary_key: true
            - name: status
              sql: status
              type: string
      views:
        - name: test_view
          cubes:
            - join_path: orders
              includes:
                - name: count
                  alias: renamed_count
                - status
                - unknown
`);

    await expect(compiler.compile()).rejects.toThrow('test_view cube: Member \'unknown\'');
  });
});
