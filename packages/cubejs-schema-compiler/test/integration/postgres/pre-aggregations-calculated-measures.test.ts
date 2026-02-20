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
            measureReferences: [average],
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

  //if (getEnv('nativeSqlPlanner')) {
  it('calculated measure pre-aggregation', () => compiler.compile().then(() => {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.average'
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
    //expect(preAggregationsDescription[0].tableName).toEqual('vis_revenue_per_id_rollup');
    //expect(sqlAndParams[0]).toContain('vis_revenue_per_id_rollup');
    console.log("!!!! sqlAndParams", sqlAndParams[0]);

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

  /* } else {
    it.skip('multi stage pre-aggregations', () => {
      // Skipping because it works only in Tesseract
    });
  } */
});
