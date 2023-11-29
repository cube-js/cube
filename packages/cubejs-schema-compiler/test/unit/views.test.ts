import { prepareYamlCompiler } from './PrepareCompiler';
import { createSchemaYaml } from './utils';

describe('Views YAML', () => {
  const schemaCompile = async (views: unknown[]) => {
    const { compiler, cubeEvaluator } = prepareYamlCompiler(
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
              meta: {
                key: 'Meta.key for CubeA.id'
              },
              primary_key: true,
            }],
            joins: [{
              name: 'CubeC',
              relationship: 'one_to_one',
              sql: 'SQL clause',
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
                meta: {
                  key: 'Meta.key for CubeB.other_id'
                },
              }
            ]
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
                meta: {
                  key: 'Meta.key for CubeC.id'
                },
              },
              {
                name: 'dimension_1',
                sql: 'dimension_1',
                type: 'number',
                description: 'Description for CubeC.dimension_1',
                meta: {
                  key: 'Meta.key for CubeC.dimension_1'
                },
              }
            ]
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

    return { compiler, cubeEvaluator };
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
      type: 'number',
    };
  }

  it('includes * + prefix a,b,c', async () => {
    const { cubeEvaluator } = await schemaCompile([{
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

    expect(cubeEvaluator.getCubeDefinition('simple_view').dimensions).toEqual({
      CubeA_id: dimensionFixtureForCube('CubeA.id'),
      CubeB_id: dimensionFixtureForCube('CubeB.id'),
      CubeB_other_id: dimensionFixtureForCube('CubeB.other_id'),
    });
  });

  it('includes * + prefix a,b + exclude ids', async () => {
    const { cubeEvaluator } = await schemaCompile([{
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
  });

  it('includes * + prefix b + exclude ids', async () => {
    const { cubeEvaluator } = await schemaCompile([{
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
  });

  it('includes * (a,b) + exclude id from b', async () => {
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
      ]
    }]);

    expect(cubeEvaluator.getCubeDefinition('simple_view').dimensions).toEqual({
      id: dimensionFixtureForCube('CubeA.id'),
      other_id: dimensionFixtureForCube('CubeB.other_id'),
    });
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
});
