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


      average: {
        sql: \`\${revenue} / \${count}\`,
        type: 'number'
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
        averagePreAgg: {
            type: 'rollup',
            measureReferences: [visitor_checkins.average, visitor_checkins.revenue, visitor_checkins.count],
            dimensionReferences: [source, id],
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
      },
      revenue: {
        sql: 'id',
        type: 'sum'
      },
      average: {
        sql: \`\${revenue} / \${count}\`,
        type: 'number'
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

  if (getEnv('nativeSqlPlanner') && getEnv('nativeSqlPlannerPreAggregations')) {
    it('calculated measure pre-aggregation', () => compiler.compile().then(() => {
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'visitor_checkins.average',
          'visitor_checkins.revenue',
          'visitor_checkins.count'
        ],
        dimensions: [
          'visitors.source'
        ],
        timezone: 'America/Los_Angeles',
        preAggregationsSchema: '',
        cubestoreSupportMultistage: true
      });

      const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
      const sqlAndParams = query.buildSqlAndParams();
      expect(preAggregationsDescription[0].tableName).toEqual('vis_average_pre_agg');
      expect(sqlAndParams[0]).toContain('vis_average_pre_agg');

      return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
        expect(res).toEqual(

          [
            {
              vis__source: null,
              vc__average: null,
              vc__revenue: null,
              vc__count: '0'
            },
            {
              vis__source: 'google',
              vc__average: '6.0000000000000000',
              vc__revenue: '6',
              vc__count: '1'
            },
            {
              vis__source: 'some',
              vc__average: '3.0000000000000000',
              vc__revenue: '15',
              vc__count: '5'
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
