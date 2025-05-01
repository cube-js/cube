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
        preAggregationsSchema: ''
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
        preAggregationsSchema: ''
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
        preAggregationsSchema: ''
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
  } else {
    it.skip('multi stage pre-aggregations', () => {
      // Skipping because it works only in Tesseract
    });
  }
});
