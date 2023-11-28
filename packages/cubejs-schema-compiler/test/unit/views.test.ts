import { prepareYamlCompiler } from './PrepareCompiler';
import { createSchemaYaml } from './utils';

describe('Views Includes/Excludes', () => {
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
              primary_key: true,
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
                primary_key: true,
              },
              {
                name: 'other_id',
                sql: 'other_id',
                type: 'number',
              }
            ]
          }
        ],
        views
      })
    );
    await compiler.compile();

    return { compiler, cubeEvaluator };
  };

  it('includes * + prefix a,b', async () => {
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
      CubeA_id: {
        description: undefined,
        meta: undefined,
        ownedByCube: false,
        sql: expect.any(Function),
        aliasMember: 'CubeA.id',
        type: 'number',
      },
      CubeB_id: {
        description: undefined,
        meta: undefined,
        ownedByCube: false,
        sql: expect.any(Function),
        aliasMember: 'CubeB.id',
        type: 'number',
      },
      CubeB_other_id: {
        description: undefined,
        meta: undefined,
        ownedByCube: false,
        sql: expect.any(Function),
        aliasMember: 'CubeB.other_id',
        type: 'number',
      },
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
      CubeB_other_id: {
        description: undefined,
        meta: undefined,
        ownedByCube: false,
        sql: expect.any(Function),
        aliasMember: 'CubeB.other_id',
        type: 'number',
      },
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
      CubeB_other_id: {
        description: undefined,
        meta: undefined,
        ownedByCube: false,
        sql: expect.any(Function),
        aliasMember: 'CubeB.other_id',
        type: 'number',
      },
    });
  });

  it('includes * + exclude id from b', async () => {
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
      id: {
        description: undefined,
        meta: undefined,
        ownedByCube: false,
        sql: expect.any(Function),
        aliasMember: 'CubeA.id',
        type: 'number',
      },
      other_id: {
        description: undefined,
        meta: undefined,
        ownedByCube: false,
        sql: expect.any(Function),
        aliasMember: 'CubeB.other_id',
        type: 'number',
      },
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
      id: {
        description: undefined,
        meta: undefined,
        ownedByCube: false,
        sql: expect.any(Function),
        aliasMember: 'CubeA.id',
        type: 'number',
      },
      other_id: {
        description: undefined,
        meta: undefined,
        ownedByCube: false,
        sql: expect.any(Function),
        aliasMember: 'CubeB.other_id',
        type: 'number',
      },
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
      id: {
        description: undefined,
        meta: undefined,
        ownedByCube: false,
        sql: expect.any(Function),
        aliasMember: 'CubeA.id',
        type: 'number',
      },
      other_id: {
        description: undefined,
        meta: undefined,
        ownedByCube: false,
        sql: expect.any(Function),
        aliasMember: 'CubeB.other_id',
        type: 'number',
      },
    });
  });
});
