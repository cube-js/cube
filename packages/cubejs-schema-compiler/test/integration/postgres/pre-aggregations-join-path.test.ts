import { PreAggregationPartitionRangeLoader } from '@cubejs-backend/query-orchestrator';
import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { BigqueryQuery } from '../../../src/adapter/BigqueryQuery';
import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';
import { DataSchemaCompiler } from '../../../src/compiler/DataSchemaCompiler';
import { JoinGraph } from '../../../src/compiler/JoinGraph';
import { CubeEvaluator } from '../../../src/compiler/CubeEvaluator';

describe('PreAggregations join path', () => {
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
    // And join declaration would use ADEX path
    // It should NOT be the shortest one, nor first in join edges declaration
    // All join conditions would be essentially `FALSE`, but with different syntax, to be able to test SQL generation

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

        preAggregations: {
          adex: {
            type: 'rollup',
            dimensionReferences: [a_id, CUBE.D.E.X.x_id],
            measureReferences: [a_sum, D.E.X.x_sum],
            // TODO implement and test segmentReferences
            // TODO implement and test timeDimensionReference
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
        },

        measures: {
          x_sum: {
            sql: 'x_value',
            type: 'sum',
          },
        },
      });
    `);

    ({ compiler, joinGraph, cubeEvaluator } = prepared);
  });

  beforeEach(async () => {
    await compiler.compile();
  });

  it('should respect join path from pre-aggregation declaration', async () => {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [],
      dimensions: [
        'A.a_id'
      ],
    });

    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    const { loadSql } = preAggregationsDescription.find(p => p.preAggregationId === 'A.adex');

    expect(loadSql[0]).toMatch(/ON 'A' = 'D'/);
    expect(loadSql[0]).toMatch(/ON 'D' = 'E'/);
    expect(loadSql[0]).toMatch(/ON 'E' = 'X'/);
    expect(loadSql[0]).not.toMatch(/ON 'A' = 'B'/);
    expect(loadSql[0]).not.toMatch(/ON 'B' = 'C'/);
    expect(loadSql[0]).not.toMatch(/ON 'C' = 'X'/);
    expect(loadSql[0]).not.toMatch(/ON 'A' = 'F'/);
    expect(loadSql[0]).not.toMatch(/ON 'F' = 'X'/);
  });
});
