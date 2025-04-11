import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { DataSchemaCompiler } from '../../../src/compiler/DataSchemaCompiler';
import { JoinGraph } from '../../../src/compiler/JoinGraph';
import { CubeEvaluator } from '../../../src/compiler/CubeEvaluator';

describe('Multiple join paths', () => {
  jest.setTimeout(200000);

  let compiler: DataSchemaCompiler;
  let joinGraph: JoinGraph;
  let cubeEvaluator: CubeEvaluator;

  beforeAll(async () => {
    // All joins would look like this
    // A-->B-->C-->X
    // |           ^
    // ├-->D-->E---┤
    // |           |
    // └-->F-------┘
    // View, pre-aggregations and all interesting parts should use ADEX path
    // It should NOT be the shortest one from A to X (that's AFX), nor first in join edges declaration (that's ABCX)
    // All join conditions would be essentially `FALSE`, but with different syntax, to be able to test SQL generation
    // Also, there should be only one way to cover cubes A and D with joins: A->D join

    // TODO in this model queries like [A.a_id, X.x_id] become ambiguous, probably we want to handle this better

    // language=JavaScript
    const prepared = prepareJsCompiler(`
      cube('A', {
        sql: 'SELECT 1 AS a_id, 100 AS a_value',

        joins: {
          B: {
            relationship: 'many_to_one',
            sql: "'A' = 'B'",
          },
          D: {
            relationship: 'many_to_one',
            sql: "'A' = 'D'",
          },
          F: {
            relationship: 'many_to_one',
            sql: "'A' = 'F'",
          },
        },

        dimensions: {
          a_id: {
            type: 'number',
            sql: 'a_id',
            primaryKey: true,
          },
        },

        measures: {
          a_sum: {
            sql: 'a_value',
            type: 'sum',
          },
        },
      });

      cube('B', {
        sql: 'SELECT 1 AS b_id, 100 AS b_value',

        joins: {
          C: {
            relationship: 'many_to_one',
            sql: "'B' = 'C'",
          },
        },

        dimensions: {
          b_id: {
            type: 'number',
            sql: 'b_id',
            primaryKey: true,
          },
        },

        measures: {
          b_sum: {
            sql: 'b_value',
            type: 'sum',
          },
        },
      });

      cube('C', {
        sql: 'SELECT 1 AS c_id, 100 AS c_value',

        joins: {
          X: {
            relationship: 'many_to_one',
            sql: "'C' = 'X'",
          },
        },

        dimensions: {
          c_id: {
            type: 'number',
            sql: 'c_id',
            primaryKey: true,
          },
        },

        measures: {
          c_sum: {
            sql: 'c_value',
            type: 'sum',
          },
        },
      });

      cube('D', {
        sql: 'SELECT 1 AS d_id, 100 AS d_value',

        joins: {
          E: {
            relationship: 'many_to_one',
            sql: "'D' = 'E'",
          },
        },

        dimensions: {
          d_id: {
            type: 'number',
            sql: 'd_id',
            primaryKey: true,
          },
        },

        measures: {
          d_sum: {
            sql: 'd_value',
            type: 'sum',
          },
        },
      });

      cube('E', {
        sql: 'SELECT 1 AS e_id, 100 AS e_value',

        joins: {
          X: {
            relationship: 'many_to_one',
            sql: "'E' = 'X'",
          },
        },

        dimensions: {
          e_id: {
            type: 'number',
            sql: 'e_id',
            primaryKey: true,
          },
        },

        measures: {
          e_sum: {
            sql: 'e_value',
            type: 'sum',
          },
        },
      });

      cube('F', {
        sql: 'SELECT 1 AS f_id, 100 AS f_value',

        joins: {
          X: {
            relationship: 'many_to_one',
            sql: "'F' = 'X'",
          },
        },

        dimensions: {
          f_id: {
            type: 'number',
            sql: 'f_id',
            primaryKey: true,
          },
        },

        measures: {
          f_sum: {
            sql: 'f_value',
            type: 'sum',
          },
        },
      });

      cube('X', {
        sql: 'SELECT 1 AS x_id, 100 AS x_value',

        dimensions: {
          x_id: {
            type: 'number',
            sql: 'x_id',
            primaryKey: true,
          },
          // This member should be:
          // * NOT ownedByCube
          // * reference only members of same cube
          // * included in view
          x_id_ref: {
            type: 'number',
            sql: \`\${x_id} + 1\`,
          },
        },

        measures: {
          x_sum: {
            sql: 'x_value',
            type: 'sum',
          },
        },
      });

      view('ADEX_view', {
        cubes: [
          {
            join_path: A,
            includes: [
              'a_id',
            ],
            prefix: false
          },
          {
            join_path: A.D.E.X,
            includes: [
              'x_id',
              'x_id_ref',
            ],
            prefix: false
          },
        ]
      });
    `);

    ({ compiler, joinGraph, cubeEvaluator } = prepared);
  });

  beforeEach(async () => {
    await compiler.compile();
  });

  describe('View and indirect members', () => {
    it('should respect join path from view declaration', async () => {
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [],
        dimensions: [
          'ADEX_view.a_id',
          'ADEX_view.x_id_ref',
        ],
      });

      const [sql, _params] = query.buildSqlAndParams();

      expect(sql).toMatch(/ON 'A' = 'D'/);
      expect(sql).toMatch(/ON 'D' = 'E'/);
      expect(sql).toMatch(/ON 'E' = 'X'/);
      expect(sql).not.toMatch(/ON 'A' = 'B'/);
      expect(sql).not.toMatch(/ON 'B' = 'C'/);
      expect(sql).not.toMatch(/ON 'C' = 'X'/);
      expect(sql).not.toMatch(/ON 'A' = 'F'/);
      expect(sql).not.toMatch(/ON 'F' = 'X'/);
    });
  });
});
