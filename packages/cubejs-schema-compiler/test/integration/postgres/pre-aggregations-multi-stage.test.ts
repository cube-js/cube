import {
  getEnv,
} from '@cubejs-backend/shared';
import R from 'ramda';
import { UserError } from '../../../src/compiler/UserError';
import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

describe('PreAggregationsMultiStage', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
  cube(\`visitors\`, {
    sql: \`
    select * from visitors WHERE \${FILTER_PARAMS.visitors.createdAt.filter('created_at')}
    \`,
    sqlAlias: 'vis',

    joins: {
      visitor_checkins: {
        relationship: 'hasMany',
        sql: \`\${CUBE}.id = \${visitor_checkins}.visitor_id\`
      }
    },

    measures: {
      count: {
        type: 'count'
      },
      revenue: {
        sql: 'amount',
        type: 'sum'
      },


      checkinsTotal: {
        sql: \`\${checkinsCount}\`,
        type: 'sum'
      },

      uniqueSourceCount: {
        sql: 'source',
        type: 'countDistinct'
      },

      countDistinctApprox: {
        sql: 'id',
        type: 'countDistinctApprox'
      },
      revenuePerId: {
        multi_stage: true,
        sql: \`\${revenue} / \${id}\`,
        type: 'sum',
        add_group_by: [visitors.id],
      },

      revenueAndTime: {
        multi_stage: true,
        sql: \`LENGTH(CONCAT(\${revenue}, ' - ', \${createdAtDay}))\`,
        type: 'sum',
        add_group_by: [createdAtDay],
      },

      ratio: {
        sql: \`\${uniqueSourceCount} / nullif(\${checkinsTotal}, 0)\`,
        type: 'number'
      },

      testMeas: {
        type: 'countDistinct',
        sql: \`\${createdAtDay}\`
      }
    },

    dimensions: {
      id: {
        type: 'number',
        sql: 'id',
        primaryKey: true
      },
      source: {
        type: 'string',
        sql: 'source'
      },
      createdAt: {
        type: 'time',
        sql: 'created_at',
      },
      checkinsCount: {
        type: 'number',
        sql: \`\${visitor_checkins.count}\`,
        subQuery: true,
        propagateFiltersToSubQuery: true
      },
      revTest: {
        sql: \`CONCAT(\${source},  \${createdAtDay})\`,
        type: 'string',
      },

      createdAtDay: {
        type: 'time',
        sql: \`\${createdAt.day}\`,
      },



    },

    segments: {
      google: {
        sql: \`source = 'google'\`
      }
    },

    preAggregations: {
        revenuePerIdRollup: {
            type: 'rollup',
            measureReferences: [revenue],
            dimensionReferences: [id],
            timeDimensionReference: createdAt,
            granularity: 'day',
            partitionGranularity: 'month',
        },
        revenueAndTimeAndCountRollup: {
            type: 'rollup',
            measureReferences: [revenue, count],
            dimensionReferences: [source],
            timeDimensionReference: createdAt,
            granularity: 'day',
            partitionGranularity: 'month',
        },
    }
  })



  cube('visitor_checkins', {
    sql: \`
    select * from visitor_checkins
    \`,

    sqlAlias: 'vc',

    measures: {
      count: {
        type: 'count'
      }
    },

    dimensions: {
      id: {
        type: 'number',
        sql: 'id',
        primaryKey: true
      },
      visitor_id: {
        type: 'number',
        sql: 'visitor_id'
      },
      source: {
        type: 'string',
        sql: 'source'
      },
      created_at: {
        type: 'time',
        sql: 'created_at',
      }
    },

  })

  cube('coach', {
    sql: \`
      SELECT 101 AS id, '2025-01-01'::TIMESTAMP AS time UNION ALL
      SELECT 102 AS id, '2025-02-01'::TIMESTAMP AS time UNION ALL
      SELECT 103 AS id, '2025-02-02'::TIMESTAMP AS time UNION ALL
      SELECT 104 AS id, '2025-03-01'::TIMESTAMP AS time UNION ALL
      SELECT 105 AS id, '2025-03-02'::TIMESTAMP AS time UNION ALL
      SELECT 106 AS id, '2025-03-03'::TIMESTAMP AS time UNION ALL
      SELECT 107 AS id, '2025-04-01'::TIMESTAMP AS time UNION ALL
      SELECT 108 AS id, '2025-04-02'::TIMESTAMP AS time UNION ALL
      SELECT 109 AS id, '2025-04-03'::TIMESTAMP AS time UNION ALL
      SELECT 110 AS id, '2025-04-04'::TIMESTAMP AS time UNION ALL
      SELECT 111 AS id, '2025-05-01'::TIMESTAMP AS time UNION ALL
      SELECT 112 AS id, '2025-05-02'::TIMESTAMP AS time UNION ALL
      SELECT 113 AS id, '2025-05-03'::TIMESTAMP AS time UNION ALL
      SELECT 114 AS id, '2025-05-04'::TIMESTAMP AS time UNION ALL
      SELECT 115 AS id, '2025-05-05'::TIMESTAMP AS time UNION ALL
      SELECT 116 AS id, '2025-06-01'::TIMESTAMP AS time UNION ALL
      SELECT 117 AS id, '2025-06-02'::TIMESTAMP AS time UNION ALL
      SELECT 118 AS id, '2025-06-03'::TIMESTAMP AS time UNION ALL
      SELECT 119 AS id, '2025-06-04'::TIMESTAMP AS time UNION ALL
      SELECT 120 AS id, '2025-06-05'::TIMESTAMP AS time UNION ALL
      SELECT 121 AS id, '2025-06-06'::TIMESTAMP AS time UNION ALL
      SELECT 122 AS id, '2025-07-01'::TIMESTAMP AS time UNION ALL
      SELECT 123 AS id, '2025-07-02'::TIMESTAMP AS time UNION ALL
      SELECT 124 AS id, '2025-07-03'::TIMESTAMP AS time UNION ALL
      SELECT 125 AS id, '2025-07-04'::TIMESTAMP AS time UNION ALL
      SELECT 126 AS id, '2025-07-05'::TIMESTAMP AS time UNION ALL
      SELECT 127 AS id, '2025-07-06'::TIMESTAMP AS time UNION ALL
      SELECT 128 AS id, '2025-07-07'::TIMESTAMP AS time
    \`,

    dimensions: {
      time: {
        sql: 'time',
        type: 'time',
        public: false,
      },
    },

    measures: {
      count_distinct: {
        sql: 'id',
        type: 'countDistinct',
        public: false,
      },
      count_distinct__sum_by_quarter_aux: {
        multi_stage: true,
        sql: \`\${count_distinct}\`,
        type: 'sum',
        add_group_by: [time.month],
        group_by: [time.quarter],
        public: false,
      },
      count_distinct__sum_by_quarter: {
        multi_stage: true,
        sql: \`\${count_distinct__sum_by_quarter_aux}\`,
        type: 'sum',
        add_group_by: [time.quarter],
      },
    },

    preAggregations: {
      main: {
        type: 'rollup',
        measures: [count_distinct],
        timeDimensions: [
          {
            dimension: time,
            granularity: 'month'
          },
          {
            dimension: time,
            granularity: 'quarter'
          }
        ]
      },
    },
  })

  cube('monthly_data', {
    sql: \`
      SELECT 1 AS id, 10 AS amount, 'a' AS category, '2017-01-10'::TIMESTAMP AS created_at UNION ALL
      SELECT 2 AS id, 20 AS amount, 'b' AS category, '2017-01-20'::TIMESTAMP AS created_at UNION ALL
      SELECT 4 AS id, 100 AS amount, 'a' AS category, '2017-02-10'::TIMESTAMP AS created_at UNION ALL
      SELECT 5 AS id, 100 AS amount, 'b' AS category, '2017-02-20'::TIMESTAMP AS created_at UNION ALL
      SELECT 10 AS id, 200 AS amount, 'a' AS category, '2017-03-10'::TIMESTAMP AS created_at UNION ALL
      SELECT 20 AS id, 200 AS amount, 'b' AS category, '2017-03-20'::TIMESTAMP AS created_at
    \`,

    sqlAlias: 'md',

    dimensions: {
      id: {
        type: 'number',
        sql: 'id',
        primaryKey: true
      },
      category: {
        type: 'string',
        sql: 'category'
      },
      created_at: {
        type: 'time',
        sql: 'created_at'
      },
    },

    measures: {
      revenue: {
        sql: 'amount',
        type: 'sum'
      },
      count: {
        type: 'count'
      },
      revenue_per_id: {
        multi_stage: true,
        sql: \`\${revenue} / \${id}\`,
        type: 'sum',
        add_group_by: [monthly_data.id],
      },
      count_by_category: {
        multi_stage: true,
        sql: \`\${count}\`,
        type: 'sum',
        add_group_by: [monthly_data.category],
      },
      prev_month_revenue: {
        multi_stage: true,
        sql: \`\${revenue}\`,
        type: 'number',
        timeShift: [{
          timeDimension: created_at,
          interval: '1 month',
          type: 'prior',
        }],
      },
    },

    preAggregations: {
      revenueById: {
        type: 'rollup',
        measureReferences: [revenue],
        dimensionReferences: [id],
        timeDimensionReference: created_at,
        granularity: 'day',
        partitionGranularity: 'month',
      },
      countByCat: {
        type: 'rollup',
        measureReferences: [revenue, count],
        dimensionReferences: [category],
        timeDimensionReference: created_at,
        granularity: 'day',
        partitionGranularity: 'month',
      },
    },
  })

   `);

  if (getEnv('nativeSqlPlanner')) {
    it('simple multi stage with add_group_by', () => compiler.compile().then(() => {
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'visitors.revenuePerId'
        ],
        timeDimensions: [{
          dimension: 'visitors.createdAt',
          granularity: 'day',
          dateRange: ['2017-01-01', '2017-01-30']
        }],
        timezone: 'America/Los_Angeles',
        order: [{
          id: 'visitors.createdAt'
        }],
        preAggregationsSchema: '',
        cubestoreSupportMultistage: true
      });

      const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
      const sqlAndParams = query.buildSqlAndParams();
      expect(preAggregationsDescription[0].tableName).toEqual('vis_revenue_per_id_rollup');
      expect(sqlAndParams[0]).toContain('vis_revenue_per_id_rollup');

      return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
        expect(res).toEqual(
          [
            {
              vis__created_at_day: '2017-01-02T00:00:00.000Z',
              vis__revenue_per_id: '100.0000000000000000'
            },
            {
              vis__created_at_day: '2017-01-04T00:00:00.000Z',
              vis__revenue_per_id: '100.0000000000000000'
            },
            {
              vis__created_at_day: '2017-01-05T00:00:00.000Z',
              vis__revenue_per_id: '100.0000000000000000'
            },
            {
              vis__created_at_day: '2017-01-06T00:00:00.000Z',
              vis__revenue_per_id: '200.0000000000000000'
            }
          ]

        );
      });
    }));

    it('simple multi stage with add_group_by and time proxy dimension', () => compiler.compile().then(() => {
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'visitors.revenueAndTime'
        ],
        dimensions: ['visitors.source'],
        timezone: 'America/Los_Angeles',
        order: [{
          id: 'visitors.source'
        }],
        preAggregationsSchema: '',
        cubestoreSupportMultistage: true
      });

      const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
      const sqlAndParams = query.buildSqlAndParams();
      expect(preAggregationsDescription[0].tableName).toEqual('vis_revenue_and_time_and_count_rollup');
      expect(sqlAndParams[0]).toContain('vis_revenue_and_time_and_count_rollup');

      return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
        expect(res).toEqual(
          [
            { vis__source: 'google', vis__revenue_and_time: '25' },
            { vis__source: 'some', vis__revenue_and_time: '50' },
            { vis__source: null, vis__revenue_and_time: '50' }
          ]

        );
      });
    }));

    it('multi stage with add_group_by and time proxy dimension and regular measure', () => compiler.compile().then(() => {
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'visitors.revenueAndTime',
          'visitors.count'
        ],
        dimensions: ['visitors.source'],
        timezone: 'America/Los_Angeles',
        order: [{
          id: 'visitors.source'
        }],
        preAggregationsSchema: '',
        cubestoreSupportMultistage: true
      });

      const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
      const sqlAndParams = query.buildSqlAndParams();
      expect(preAggregationsDescription[0].tableName).toEqual('vis_revenue_and_time_and_count_rollup');
      expect(sqlAndParams[0]).toContain('vis_revenue_and_time_and_count_rollup');
      expect(sqlAndParams[0]).not.toContain('select * from visitors');

      return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
        expect(res).toEqual(
          [
            { vis__source: 'google', vis__count: '1', vis__revenue_and_time: '25' },
            { vis__source: 'some', vis__count: '2', vis__revenue_and_time: '50' },
            { vis__source: null, vis__count: '3', vis__revenue_and_time: '50' }
          ]

        );
      });
    }));

    it('multi stage count_distinct sum by quarter with pre-aggregation', () => compiler.compile().then(() => {
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'coach.count_distinct__sum_by_quarter'
        ],
        timezone: 'UTC',
        preAggregationsSchema: '',
        cubestoreSupportMultistage: true
      });

      const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
      const sqlAndParams = query.buildSqlAndParams();
      expect(preAggregationsDescription[0].tableName).toEqual('coach_main');
      expect(sqlAndParams[0]).toContain('coach_main');

      return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
        expect(res).toEqual(
          [
            { coach__count_distinct__sum_by_quarter: '28' },
          ]
        );
      });
    }));

    it('two multi-stage measures with different pre-aggregations', () => compiler.compile().then(() => {
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'monthly_data.revenue_per_id',
          'monthly_data.count_by_category'
        ],
        timeDimensions: [{
          dimension: 'monthly_data.created_at',
          granularity: 'month',
          dateRange: ['2017-01-01', '2017-03-31']
        }],
        timezone: 'UTC',
        order: [{
          id: 'monthly_data.created_at'
        }],
        preAggregationsSchema: '',
        cubestoreSupportMultistage: true
      });

      const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
      const sqlAndParams = query.buildSqlAndParams();
      const tableNames = preAggregationsDescription.map((d: any) => d.tableName);
      expect(tableNames).toContain('md_revenue_by_id');
      expect(tableNames).toContain('md_count_by_cat');
      expect(sqlAndParams[0]).toContain('md_revenue_by_id');
      expect(sqlAndParams[0]).toContain('md_count_by_cat');

      return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
        expect(res).toEqual(
          [
            {
              md__created_at_month: '2017-01-01T00:00:00.000Z',
              md__revenue_per_id: '20.0000000000000000',
              md__count_by_category: '2'
            },
            {
              md__created_at_month: '2017-02-01T00:00:00.000Z',
              md__revenue_per_id: '45.0000000000000000',
              md__count_by_category: '2'
            },
            {
              md__created_at_month: '2017-03-01T00:00:00.000Z',
              md__revenue_per_id: '30.0000000000000000',
              md__count_by_category: '2'
            }
          ]
        );
      });
    }));

    it('multi-stage with time_shift loading different pre-aggregation partitions', () => compiler.compile().then(() => {
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'monthly_data.revenue_per_id',
          'monthly_data.prev_month_revenue'
        ],
        timeDimensions: [{
          dimension: 'monthly_data.created_at',
          granularity: 'month',
          dateRange: ['2017-02-01', '2017-03-31']
        }],
        timezone: 'UTC',
        order: [{
          id: 'monthly_data.created_at'
        }],
        preAggregationsSchema: '',
        cubestoreSupportMultistage: true
      });

      const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
      const sqlAndParams = query.buildSqlAndParams();
      expect(preAggregationsDescription.length).toBeGreaterThanOrEqual(1);
      expect(preAggregationsDescription.some((d: any) => d.tableName.startsWith('md_'))).toBe(true);

      return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
        expect(res).toEqual(
          [
            {
              md__created_at_month: '2017-02-01T00:00:00.000Z',
              md__revenue_per_id: '45.0000000000000000',
              md__prev_month_revenue: '30'
            },
            {
              md__created_at_month: '2017-03-01T00:00:00.000Z',
              md__revenue_per_id: '30.0000000000000000',
              md__prev_month_revenue: '200'
            }
          ]
        );
      });
    }));
  } else {
    it.skip('multi stage pre-aggregations', () => {
      // Skipping because it works only in Tesseract
    });
  }
});
