import { PostgresQuery } from '../../../src';
import { DataSchemaCompiler } from '../../../src/compiler/DataSchemaCompiler';
import { JoinGraph } from '../../../src/compiler/JoinGraph';
import { CubeEvaluator } from '../../../src/compiler/CubeEvaluator';

import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

describe('Member expressions on views', () => {
  jest.setTimeout(200000);

  // language=JavaScript
  const model = `
    cube('single_cube', {
      sql: \`
          SELECT 1 AS id, 'foo' AS dim, 'one' AS test_dim, 100 AS val UNION ALL
          SELECT 2 AS id, 'foo' AS dim, 'two' AS test_dim, 300 AS val UNION ALL
          SELECT 3 AS id, 'bar' AS dim, 'three' AS test_dim, 500 AS val
        \`,
      measures: {
        val_sum: {
          type: 'sum',
          sql: 'val'
        },
        val_avg: {
          type: 'avg',
          sql: 'val'
        },
      },
      dimensions: {
        id: {
          type: 'number',
          sql: 'id',
          primaryKey: true,
        },
        dim: {
          type: 'string',
          sql: 'dim',
        },
        test_dim: {
          type: 'string',
          sql: 'test_dim',
        }
      }
    });

    view('single_view', {
      cubes: [
        {
          join_path: 'single_cube',
          includes: [
            'dim',
            'test_dim',
            'val_sum',
            'val_avg',
          ]
        },
      ]
    });

    cube('many_to_one_root', {
      sql: \`
          SELECT 1 AS id, 1 AS child_id, 'foo' AS dim, 'one' AS test_dim, 100 AS val UNION ALL
          SELECT 2 AS id, 1 AS child_id, 'foo' AS dim, 'two' AS test_dim, 300 AS val UNION ALL
          SELECT 3 AS id, 2 AS child_id, 'foo' AS dim, 'two' AS test_dim, 800 AS val UNION ALL
          SELECT 4 AS id, 3 AS child_id, 'bar' AS dim, 'three' AS test_dim, 500 AS val
        \`,
      joins: {
        many_to_one_child: {
          relationship: 'many_to_one',
          sql: \`\${CUBE.child_id} = \${many_to_one_child.id}\`
        },
      },
      measures: {
        val_sum: {
          type: 'sum',
          sql: 'val'
        },
        val_avg: {
          type: 'avg',
          sql: 'val'
        },
      },
      dimensions: {
        id: {
          type: 'number',
          sql: 'id',
          primaryKey: true,
        },
        child_id: {
          type: 'number',
          sql: 'child_id',
        },
        dim: {
          type: 'string',
          sql: 'dim',
        },
        test_dim: {
          type: 'string',
          sql: 'test_dim',
        }
      }
    });

    cube('many_to_one_child', {
      sql: \`
          SELECT 1 AS id, 'foo' AS dim, 'one' AS test_dim, 100 AS val UNION ALL
          SELECT 2 AS id, 'foo' AS dim, 'two' AS test_dim, 300 AS val UNION ALL
          SELECT 3 AS id, 'bar' AS dim, 'three' AS test_dim, 500 AS val
        \`,
      measures: {
        val_sum: {
          type: 'sum',
          sql: 'val'
        },
        val_avg: {
          type: 'avg',
          sql: 'val'
        },
      },
      dimensions: {
        id: {
          type: 'number',
          sql: 'id',
          primaryKey: true,
        },
        dim: {
          type: 'string',
          sql: 'dim',
        },
        test_dim: {
          type: 'string',
          sql: 'test_dim',
        }
      }
    });

    view('many_to_one_view', {
      cubes: [
        {
          join_path: 'many_to_one_root',
          includes: [
            'dim',
            'test_dim',
            'val_sum',
            'val_avg',
          ],
          prefix: true,
        },
        {
          join_path: 'many_to_one_root.many_to_one_child',
          includes: [
            'dim',
            'test_dim',
            'val_sum',
            'val_avg',
          ],
          prefix: true,
        },
      ]
    });

    cube('one_to_many_root', {
      sql: \`
          SELECT 1 AS id, 'foo' AS dim, 'one' AS test_dim, 100 AS val UNION ALL
          SELECT 2 AS id, 'foo' AS dim, 'two' AS test_dim, 300 AS val UNION ALL
          SELECT 3 AS id, 'bar' AS dim, 'three' AS test_dim, 500 AS val UNION ALL
          SELECT 4 AS id, 'bar' AS dim, 'four' AS test_dim, 500 AS val UNION ALL
          SELECT 5 AS id, 'bar' AS dim, 'five' AS test_dim, 500 AS val
        \`,
      joins: {
        one_to_many_child: {
          relationship: 'one_to_many',
          sql: \`\${CUBE.id} = \${one_to_many_child.parent_id}\`
        },
      },
      measures: {
        val_sum: {
          type: 'sum',
          sql: 'val'
        },
        val_avg: {
          type: 'avg',
          sql: 'val'
        },
      },
      dimensions: {
        id: {
          type: 'number',
          sql: 'id',
          primaryKey: true,
        },
        dim: {
          type: 'string',
          sql: 'dim',
        },
        test_dim: {
          type: 'string',
          sql: 'test_dim',
        }
      }
    });

    cube('one_to_many_child', {
      sql: \`
          SELECT 1 AS id, 1 AS parent_id, 'foo' AS dim, 'one' AS test_dim, 100 AS val UNION ALL
          SELECT 2 AS id, 1 AS parent_id, 'bar' AS dim, 'two' AS test_dim, 300 AS val UNION ALL
          SELECT 3 AS id, 2 AS parent_id, 'foo' AS dim, 'three' AS test_dim, 500 AS val
        \`,
      measures: {
        val_sum: {
          type: 'sum',
          sql: 'val'
        },
        val_avg: {
          type: 'avg',
          sql: 'val'
        },
      },
      dimensions: {
        id: {
          type: 'number',
          sql: 'id',
          primaryKey: true,
        },
        parent_id: {
          type: 'number',
          sql: 'parent_id',
        },
        dim: {
          type: 'string',
          sql: 'dim',
        },
        test_dim: {
          type: 'string',
          sql: 'test_dim',
        }
      }
    });

    view('one_to_many_view', {
      cubes: [
        {
          join_path: 'one_to_many_root',
          includes: [
            'dim',
            'test_dim',
            'val_sum',
            'val_avg',
          ],
          prefix: true,
        },
        {
          join_path: 'one_to_many_root.one_to_many_child',
          includes: [
            'dim',
            'test_dim',
            'val_sum',
            'val_avg',
          ],
          prefix: true,
        },
      ]
    });

    `;

  let compiler: DataSchemaCompiler;
  let joinGraph: JoinGraph;
  let cubeEvaluator: CubeEvaluator;

  beforeAll(async () => {
    ({ compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(model));
    await compiler.compile();
  });

  async function runQueryTest(q: unknown, expectedResult: unknown): Promise<void> {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, q);

    const res = await dbRunner.testQuery(query.buildSqlAndParams());

    expect(res).toEqual(expectedResult);
  }

  // Every test have this in common:
  // Request single dimension and avg measure from every cube, just to trigger full key query where possible
  // Then, on top of that, each test would request one additional measure, with member expression inside

  // TODO add test with calculation in measure based on two different dimensions from same cube
  // TODO add test with calculation in measure based on two different dimensions from different cubes

  type Config = {
    cubeName: string,
    baseQuery: {
      measures: Array<string>,
      dimensions: Array<string>,
      order: Array<{id: string, desc: boolean}>,
    },
    baseExpectedResults: Array<Record<string, string | null>>,
    testMeasures: Array<{
      name: string,
      expression: string,
      expectedResults: Array<Record<string, string | null>>
    }>,
  };

  const configs: Array<Config> = [
    {
      cubeName: 'single_cube',
      baseQuery: {
        measures: [
          'single_cube.val_avg',
        ],
        dimensions: [
          'single_cube.dim',
        ],
        order: [{
          id: 'single_cube.dim',
          desc: false,
        }]
      },
      baseExpectedResults: [
        {
          single_cube__dim: 'bar',
          single_cube__val_avg: '500.0000000000000000',
        },
        {
          single_cube__dim: 'foo',
          single_cube__val_avg: '200.0000000000000000',
        },
      ],
      testMeasures: [
        {
          name: 'one_sum',
          // eslint-disable-next-line no-template-curly-in-string
          expression: 'SUM(1)',
          expectedResults: [
            {
              single_cube_one_sum: '1',
            },
            {
              single_cube_one_sum: '2',
            },
          ],
        },
        {
          name: 'val_sum',
          // eslint-disable-next-line no-template-curly-in-string
          expression: '${single_cube.val_sum}',
          expectedResults: [
            {
              single_cube_val_sum: '500',
            },
            {
              single_cube_val_sum: '400',
            },
          ],
        },
        {
          name: 'distinct_dim',
          // eslint-disable-next-line no-template-curly-in-string
          expression: 'COUNT(DISTINCT ${single_cube.test_dim})',
          expectedResults: [
            {
              single_cube_distinct_dim: '1',
            },
            {
              single_cube_distinct_dim: '2',
            },
          ],
        },
      ],
    },
    {
      cubeName: 'single_view',
      baseQuery: {
        measures: [
          'single_view.val_avg',
        ],
        dimensions: [
          'single_view.dim',
        ],
        order: [{
          id: 'single_view.dim',
          desc: false,
        }]
      },
      baseExpectedResults: [
        {
          single_view__dim: 'bar',
          single_view__val_avg: '500.0000000000000000',
        },
        {
          single_view__dim: 'foo',
          single_view__val_avg: '200.0000000000000000',
        },
      ],
      testMeasures: [
        {
          name: 'one_sum',
          // eslint-disable-next-line no-template-curly-in-string
          expression: 'SUM(1)',
          expectedResults: [
            {
              single_view_one_sum: '1',
            },
            {
              single_view_one_sum: '2',
            },
          ],
        },
        {
          name: 'val_sum',
          // eslint-disable-next-line no-template-curly-in-string
          expression: '${single_view.val_sum}',
          expectedResults: [
            {
              single_view_val_sum: '500',
            },
            {
              single_view_val_sum: '400',
            },
          ],
        },
        {
          name: 'distinct_dim',
          // eslint-disable-next-line no-template-curly-in-string
          expression: 'COUNT(DISTINCT ${single_view.test_dim})',
          expectedResults: [
            {
              single_view_distinct_dim: '1',
            },
            {
              single_view_distinct_dim: '2',
            },
          ],
        },
      ],
    },
    {
      cubeName: 'many_to_one_view',
      baseQuery: {
        measures: [
          'many_to_one_view.many_to_one_root_val_avg',
          'many_to_one_view.many_to_one_child_val_avg',
        ],
        dimensions: [
          'many_to_one_view.many_to_one_root_dim',
          'many_to_one_view.many_to_one_child_dim',
        ],
        order: [
          {
            id: 'many_to_one_view.many_to_one_root_dim',
            desc: false,
          },
          {
            id: 'many_to_one_view.many_to_one_child_dim',
            desc: false,
          }
        ]
      },
      baseExpectedResults: [
        {
          many_to_one_view__many_to_one_root_dim: 'bar',
          many_to_one_view__many_to_one_child_dim: 'bar',
          many_to_one_view__many_to_one_root_val_avg: '500.0000000000000000',
          many_to_one_view__many_to_one_child_val_avg: '500.0000000000000000',
        },
        {
          many_to_one_view__many_to_one_root_dim: 'foo',
          many_to_one_view__many_to_one_child_dim: 'foo',
          many_to_one_view__many_to_one_root_val_avg: '400.0000000000000000',
          many_to_one_view__many_to_one_child_val_avg: '200.0000000000000000',
        },
      ],
      testMeasures: [
        {
          name: 'one_sum',
          // eslint-disable-next-line no-template-curly-in-string
          expression: 'SUM(1)',
          expectedResults: [
            {
              many_to_one_view_one_sum: '1',
            },
            {
              many_to_one_view_one_sum: '3',
            },
          ],
        },
        {
          name: 'root_val_sum',
          // eslint-disable-next-line no-template-curly-in-string
          expression: '${many_to_one_view.many_to_one_root_val_sum}',
          expectedResults: [
            {
              many_to_one_view_root_val_sum: '500',
            },
            {
              many_to_one_view_root_val_sum: '1200',
            },
          ],
        },
        {
          name: 'root_distinct_dim',
          // eslint-disable-next-line no-template-curly-in-string
          expression: 'COUNT(DISTINCT ${many_to_one_view.many_to_one_root_test_dim})',
          expectedResults: [
            {
              many_to_one_view_root_distinct_dim: '1',
            },
            {
              many_to_one_view_root_distinct_dim: '2',
            },
          ],
        },
        {
          name: 'child_val_sum',
          // eslint-disable-next-line no-template-curly-in-string
          expression: '${many_to_one_view.many_to_one_child_val_sum}',
          expectedResults: [
            {
              many_to_one_view_child_val_sum: '500',
            },
            {
              many_to_one_view_child_val_sum: '400',
            },
          ],
        },
        {
          name: 'child_distinct_dim',
          // eslint-disable-next-line no-template-curly-in-string
          expression: 'COUNT(DISTINCT ${many_to_one_view.many_to_one_child_test_dim})',
          expectedResults: [
            {
              many_to_one_view_child_distinct_dim: '1',
            },
            {
              many_to_one_view_child_distinct_dim: '2',
            },
          ],
        },
      ],
    },
    {
      cubeName: 'one_to_many_view',
      baseQuery: {
        measures: [
          'one_to_many_view.one_to_many_root_val_avg',
          'one_to_many_view.one_to_many_child_val_avg',
        ],
        dimensions: [
          'one_to_many_view.one_to_many_root_dim',
          'one_to_many_view.one_to_many_child_dim',
        ],
        order: [
          {
            id: 'one_to_many_view.one_to_many_root_dim',
            desc: false,
          },
          {
            id: 'one_to_many_view.one_to_many_child_dim',
            desc: false,
          }
        ]
      },
      baseExpectedResults: [
        {
          one_to_many_view__one_to_many_root_dim: 'bar',
          one_to_many_view__one_to_many_child_dim: null,
          one_to_many_view__one_to_many_root_val_avg: '500.0000000000000000',
          one_to_many_view__one_to_many_child_val_avg: null,
        },
        {
          one_to_many_view__one_to_many_root_dim: 'foo',
          one_to_many_view__one_to_many_child_dim: 'bar',
          one_to_many_view__one_to_many_root_val_avg: '100.0000000000000000',
          one_to_many_view__one_to_many_child_val_avg: '300.0000000000000000',
        },
        {
          one_to_many_view__one_to_many_root_dim: 'foo',
          one_to_many_view__one_to_many_child_dim: 'foo',
          one_to_many_view__one_to_many_root_val_avg: '200.0000000000000000',
          one_to_many_view__one_to_many_child_val_avg: '300.0000000000000000',
        },
      ],
      testMeasures: [
        {
          name: 'one_sum',
          // eslint-disable-next-line no-template-curly-in-string
          expression: 'SUM(1)',
          expectedResults: [
            {
              one_to_many_view_one_sum: '3',
            },
            {
              one_to_many_view_one_sum: '1',
            },
            {
              one_to_many_view_one_sum: '2',
            },
          ],
        },
        {
          name: 'root_val_sum',
          // eslint-disable-next-line no-template-curly-in-string
          expression: '${one_to_many_view.one_to_many_root_val_sum}',
          expectedResults: [
            {
              one_to_many_view_root_val_sum: '1500',
            },
            {
              one_to_many_view_root_val_sum: '100',
            },
            {
              one_to_many_view_root_val_sum: '400',
            },
          ],
        },
        {
          name: 'root_distinct_dim',
          // eslint-disable-next-line no-template-curly-in-string
          expression: 'COUNT(DISTINCT ${one_to_many_view.one_to_many_root_test_dim})',
          expectedResults: [
            {
              one_to_many_view_root_distinct_dim: '3',
            },
            {
              one_to_many_view_root_distinct_dim: '1',
            },
            {
              one_to_many_view_root_distinct_dim: '2',
            },
          ],
        },
        {
          name: 'child_val_sum',
          // eslint-disable-next-line no-template-curly-in-string
          expression: '${one_to_many_view.one_to_many_child_val_sum}',
          expectedResults: [
            {
              one_to_many_view_child_val_sum: null,
            },
            {
              one_to_many_view_child_val_sum: '300',
            },
            {
              one_to_many_view_child_val_sum: '600',
            },
          ],
        },
        {
          name: 'child_distinct_dim',
          // eslint-disable-next-line no-template-curly-in-string
          expression: 'COUNT(DISTINCT ${one_to_many_view.one_to_many_child_test_dim})',
          expectedResults: [
            {
              one_to_many_view_child_distinct_dim: '0',
            },
            {
              one_to_many_view_child_distinct_dim: '1',
            },
            {
              one_to_many_view_child_distinct_dim: '2',
            },
          ],
        },
      ],
    },
  ];

  for (const { cubeName, baseQuery, baseExpectedResults, testMeasures } of configs) {
    describe(cubeName, () => {
      for (const { name, expression, expectedResults } of testMeasures) {
        it(name, async () => runQueryTest(
          {
            ...baseQuery,
            measures: [
              ...baseQuery.measures,
              {
                // eslint-disable-next-line no-new-func
                expression: new Function(
                  cubeName,
                  `return \`${expression}\`;`
                ),
                name: `${cubeName}_${name}`,
                expressionName: `${cubeName}_${name}`,
                // eslint-disable-next-line no-template-curly-in-string
                definition: expression,
                cubeName,
              },
            ],
          },
          expectedResults.map((r, i) => ({
            ...baseExpectedResults[i],
            ...r,
          }))
        ));
      }
    });
  }
});
