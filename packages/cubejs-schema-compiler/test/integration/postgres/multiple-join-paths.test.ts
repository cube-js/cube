import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { DataSchemaCompiler } from '../../../src/compiler/DataSchemaCompiler';
import { JoinGraph } from '../../../src/compiler/JoinGraph';
import { CubeEvaluator } from '../../../src/compiler/CubeEvaluator';
import { testWithPreAggregation } from './pre-aggregation-utils';

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
    // All join conditions would be essentially `TRUE` for ADEX joins and `FALSE` for everything else
    // But they would use different syntax, to be able to test SQL generation
    // Also, there should be only one way to cover cubes A and D with joins: A->D join

    // TODO in this model queries like [A.a_id, X.x_id] become ambiguous, probably we want to handle this better

    // language=JavaScript
    const prepared = prepareJsCompiler(`
      cube('A', {
        sql: "SELECT 1 AS a_id, CAST('1970-01-01' AS TIMESTAMPTZ) AS a_time, 100 AS a_value",

        joins: {
          B: {
            relationship: 'many_to_one',
            sql: "'A' = 'B'",
          },
          D: {
            relationship: 'many_to_one',
            sql: "'A' = 'D' OR TRUE",
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

          a_time: {
            type: 'time',
            sql: 'a_time',
          },
        },

        measures: {
          a_sum: {
            sql: 'a_value',
            type: 'sum',
          },
        },

        segments: {
          a_seg: {
            sql: 'a_id % 2 = 0',
          },
        },

        preAggregations: {
          adex_with_join_paths: {
            type: 'rollup',
            dimensions: [
              a_id,
              A.D.d_id,
              A.D.d_name_for_join_paths,
              A.D.E.X.x_id,
            ],
            measures: [
              a_sum,
            ],
            segments: [
              a_seg,
              A.D.d_seg,
              A.D.E.X.x_seg,
            ],
            timeDimension: A.D.E.X.x_time,
            granularity: 'day',
          },

          adex_cumulative_with_join_paths: {
            type: 'rollup',
            dimensions: [
              a_id,
              A.D.E.X.x_id,
            ],
            measures: [
              A.D.E.X.x_cumulative_sum,
            ],
            timeDimension: A.D.E.X.x_time,
            granularity: 'day',
          },

          ad_without_join_paths: {
            type: 'rollup',
            dimensions: [
              CUBE.a_id,
              D.d_id,
              D.d_name_for_no_join_paths,
            ],
            measures: [
              a_sum,
            ],
            segments: [
              a_seg,
              D.d_seg,
            ],
            timeDimension: D.d_time,
            granularity: 'day',
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

        segments: {
          b_seg: {
            sql: 'b_id % 2 = 0',
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

        segments: {
          c_seg: {
            sql: 'c_id % 2 = 0',
          },
        },
      });

      cube('D', {
        sql: "SELECT 1 AS d_id, 'foo' AS d_name, CAST('1970-01-01' AS TIMESTAMPTZ) AS d_time, 100 AS d_value",

        joins: {
          E: {
            relationship: 'many_to_one',
            sql: "'D' = 'E' OR TRUE",
          },
        },

        dimensions: {
          d_id: {
            type: 'number',
            sql: 'd_id',
            primaryKey: true,
          },
          // These are to select different preaggregations from query PoV
          d_name_for_join_paths: {
            type: 'string',
            sql: 'd_name',
          },
          d_name_for_no_join_paths: {
            type: 'string',
            sql: 'd_name',
          },
          d_time: {
            type: 'time',
            sql: 'd_time',
          },
        },

        measures: {
          d_sum: {
            sql: 'd_value',
            type: 'sum',
          },
        },

        segments: {
          d_seg: {
            sql: 'd_id % 2 = 0',
          },
        },
      });

      cube('E', {
        sql: 'SELECT 1 AS e_id, 100 AS e_value',

        joins: {
          X: {
            relationship: 'many_to_one',
            sql: "'E' = 'X' OR TRUE",
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

        segments: {
          e_seg: {
            sql: 'e_id % 2 = 0',
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

        segments: {
          f_seg: {
            sql: 'f_id % 2 = 0',
          },
        },
      });

      cube('X', {
        sql: "SELECT 1 AS x_id, 'foo' AS x_name, CAST('1970-01-01' AS TIMESTAMPTZ) AS x_time, 100 AS x_value",

        dimensions: {
          x_id: {
            type: 'number',
            sql: 'x_id',
            primaryKey: true,
          },
          x_name: {
            type: 'string',
            sql: 'x_name',
          },
          // This member should be:
          // * NOT ownedByCube
          // * reference only members of same cube
          // * included in view
          // * NOT included in pre-aggs (as well as at least one of its references)
          x_name_ref: {
            type: 'string',
            sql: \`\${x_name} || 'bar'\`,
          },
          x_time: {
            type: 'time',
            sql: 'x_time',
          },
        },

        measures: {
          x_sum: {
            sql: 'x_value',
            type: 'sum',
          },
          x_cumulative_sum: {
            sql: 'x_value',
            type: 'sum',
            rolling_window: {
                trailing: 'unbounded',
            },
          },
        },

        segments: {
          x_seg: {
            sql: 'x_id % 2 = 0',
          },
        },
      });

      view('ADEX_view', {
        cubes: [
          {
            join_path: A,
            includes: [
              'a_id',
              'a_sum',
              'a_seg',
            ],
            prefix: false
          },
          {
            join_path: A.D,
            includes: [
              'd_id',
              'd_name_for_join_paths',
              'd_name_for_no_join_paths',
              'd_time',
              'd_sum',
              'd_seg',
            ],
            prefix: false
          },
          {
            join_path: A.D.E.X,
            includes: [
              'x_id',
              'x_name_ref',
              'x_time',
              'x_sum',
              'x_seg',
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
          'ADEX_view.x_name_ref',
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

  describe('PreAggregations join path', () => {
    it('should respect join path from pre-aggregation declaration', async () => {
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [],
        dimensions: [
          'A.a_id'
        ],
      });

      const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
      const { loadSql } = preAggregationsDescription.find(p => p.preAggregationId === 'A.adex_with_join_paths');

      expect(loadSql[0]).toMatch(/ON 'A' = 'D'/);
      expect(loadSql[0]).toMatch(/ON 'D' = 'E'/);
      expect(loadSql[0]).toMatch(/ON 'E' = 'X'/);
      expect(loadSql[0]).not.toMatch(/ON 'A' = 'B'/);
      expect(loadSql[0]).not.toMatch(/ON 'B' = 'C'/);
      expect(loadSql[0]).not.toMatch(/ON 'C' = 'X'/);
      expect(loadSql[0]).not.toMatch(/ON 'A' = 'F'/);
      expect(loadSql[0]).not.toMatch(/ON 'F' = 'X'/);
    });

    it('should match pre-aggregation with join paths for simple direct query', async () => {
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'A.a_sum',
        ],
        dimensions: [
          'A.a_id',
          'D.d_id',
          'D.d_name_for_join_paths',
        ],
        segments: [
          'A.a_seg',
          'D.d_seg',
        ],
      });

      const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
      const preAggregation = preAggregationsDescription.find(p => p.preAggregationId === 'A.adex_with_join_paths');
      expect(preAggregation).toBeDefined();
    });

    it('should match pre-aggregation with join paths for query through view with same join path', async () => {
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'ADEX_view.a_sum',
        ],
        dimensions: [
          'ADEX_view.a_id',
          'ADEX_view.d_name_for_join_paths',
          'ADEX_view.x_id',
        ],
        segments: [
          'ADEX_view.a_seg',
          'ADEX_view.d_seg',
          'ADEX_view.x_seg',
        ],
        timeDimensions: [{
          dimension: 'ADEX_view.x_time',
          granularity: 'day',
        }],
      });

      const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
      const preAggregation = preAggregationsDescription.find(p => p.preAggregationId === 'A.adex_with_join_paths');
      expect(preAggregation).toBeDefined();
    });

    it('should match pre-aggregation without join paths for simple direct query', async () => {
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'A.a_sum',
        ],
        dimensions: [
          'A.a_id',
          'D.d_id',
          'D.d_name_for_no_join_paths',
        ],
        segments: [
          'A.a_seg',
          'D.d_seg',
        ],
        timeDimensions: [{
          dimension: 'D.d_time',
          granularity: 'day',
        }],
      });

      const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
      const preAggregation = preAggregationsDescription.find(p => p.preAggregationId === 'A.ad_without_join_paths');
      expect(preAggregation).toBeDefined();
    });

    it('should match pre-aggregation without join paths for query through view with same join path', async () => {
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'ADEX_view.a_sum',
        ],
        dimensions: [
          'ADEX_view.a_id',
          'ADEX_view.d_id',
          'ADEX_view.d_name_for_no_join_paths',
        ],
        segments: [
          'ADEX_view.a_seg',
          'ADEX_view.d_seg',
        ],
        timeDimensions: [{
          dimension: 'ADEX_view.d_time',
          granularity: 'day',
        }],
      });

      const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
      const preAggregation = preAggregationsDescription.find(p => p.preAggregationId === 'A.ad_without_join_paths');
      expect(preAggregation).toBeDefined();
    });

    function makeReferenceQueryFor(preAggregationId: string, withDateRange: boolean = false): PostgresQuery {
      const preAggregations = cubeEvaluator.preAggregations({
        preAggregationIds: [preAggregationId]
      });

      expect(preAggregations.length).toBe(1);
      const preAggregation = preAggregations[0];

      if (withDateRange) {
        preAggregation.references.timeDimensions = preAggregation.references.timeDimensions.map(td => ({
          ...td,
          dateRange: ['1970-01-01', '1970-01-02'],
        }));
      }

      return new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        ...preAggregation.references,
        preAggregationId: preAggregation.id,
        preAggregationsSchema: '',
        timezone: 'UTC',
      });
    }

    const preAggregationTests = [
      {
        preAggregationId: 'A.adex_with_join_paths',
        addTimeRange: false,
        expectedData: [
          {
            a__a_id: 1,
            a__a_seg: false,
            a__a_sum: '100',
            d__d_id: 1,
            d__d_name_for_join_paths: 'foo',
            d__d_seg: false,
            x__x_id: 1,
            x__x_seg: false,
            x__x_time_day: '1970-01-01T00:00:00.000Z',
          },
        ],
      },
      {
        preAggregationId: 'A.adex_cumulative_with_join_paths',
        addTimeRange: true,
        expectedData: [
          {
            a__a_id: 1,
            x__x_cumulative_sum: '100',
            x__x_id: 1,
            x__x_time_day: '1970-01-01T00:00:00.000Z',
          },
          {
            a__a_id: 1,
            x__x_cumulative_sum: '100',
            x__x_id: 1,
            x__x_time_day: '1970-01-02T00:00:00.000Z',
          },
        ],
      },
      {
        preAggregationId: 'A.ad_without_join_paths',
        addTimeRange: false,
        expectedData: [
          {
            a__a_id: 1,
            a__a_seg: false,
            a__a_sum: '100',
            d__d_id: 1,
            d__d_name_for_no_join_paths: 'foo',
            d__d_seg: false,
            d__d_time_day: '1970-01-01T00:00:00.000Z',
          },
        ],
      },
    ];
    for (const { preAggregationId, addTimeRange, expectedData } of preAggregationTests) {
      // eslint-disable-next-line no-loop-func
      it(`pre-aggregation ${preAggregationId} should match its own references`, async () => {
        // Always not using range, because reference query would have no range to start from
        // but should match pre-aggregation anyway
        const query = makeReferenceQueryFor(preAggregationId);

        const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
        const preAggregationFromQuery = preAggregationsDescription.find(p => p.preAggregationId === preAggregationId);
        if (preAggregationFromQuery === undefined) {
          throw expect(preAggregationFromQuery).toBeDefined();
        }
      });

      // eslint-disable-next-line no-loop-func
      it(`pre-aggregation ${preAggregationId} reference query should be executable`, async () => {
        // Adding date range for rolling window measure
        const query = makeReferenceQueryFor(preAggregationId, addTimeRange);

        const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
        const preAggregationFromQuery = preAggregationsDescription.find(p => p.preAggregationId === preAggregationId);
        if (preAggregationFromQuery === undefined) {
          throw expect(preAggregationFromQuery).toBeDefined();
        }

        const res = await testWithPreAggregation(preAggregationFromQuery, query);
        expect(res).toEqual(expectedData);
      });
    }
  });

  describe('Query level join hints', () => {
    it('should respect query level join hints', async () => {
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [],
        dimensions: [
          'A.a_id',
          'X.x_name_ref',
        ],
        joinHints: [
          ['A', 'D'],
          ['D', 'E'],
          ['E', 'X'],
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
