import { getEnv } from '@cubejs-backend/shared';
import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

describe('PreAggregations string type measure', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
  cube(\`visitors_str\`, {
    sql: \`select * from visitors\`,
    sqlAlias: 'vstr',

    measures: {
      count: {
        type: 'count'
      },
      sources_list: {
        sql: \`STRING_AGG(\${CUBE}.source, ', ' ORDER BY \${CUBE}.source)\`,
        type: 'string'
      }
    },

    dimensions: {
      id: {
        type: 'number',
        sql: 'id',
        primaryKey: true
      },
      status: {
        type: 'number',
        sql: 'status'
      },
      createdAt: {
        type: 'time',
        sql: 'created_at',
      }
    },

    preAggregations: {
      stringMeasureRollup: {
        type: 'rollup',
        measures: [CUBE.sources_list, CUBE.count],
        dimensions: [CUBE.status],
      }
    }
  })
  `);

  it('string type measure without pre-aggregation', async () => {
    await compiler.compile();

    const query = new PostgresQuery(
      { joinGraph, cubeEvaluator, compiler },
      {
        measures: [
          'visitors_str.sources_list',
          'visitors_str.count',
        ],
        dimensions: ['visitors_str.status'],
        timeDimensions: [{
          dimension: 'visitors_str.createdAt',
          granularity: 'day',
          dateRange: ['2017-01-01', '2017-01-10'],
        }],
        timezone: 'America/Los_Angeles',
        order: [{ id: 'visitors_str.status' }],
      }
    );

    const sqlAndParams = query.buildSqlAndParams();
    // Should not use pre-aggregation since timeDimension is not in the rollup
    expect(sqlAndParams[0]).not.toContain('string_measure_rollup');

    return dbRunner.testQuery(sqlAndParams).then(res => {
      expect(res).toEqual([
        {
          vstr__status: 1,
          vstr__created_at_day: '2017-01-02T00:00:00.000Z',
          vstr__sources_list: 'some',
          vstr__count: '1',
        },
        {
          vstr__status: 1,
          vstr__created_at_day: '2017-01-04T00:00:00.000Z',
          vstr__sources_list: 'some',
          vstr__count: '1',
        },
        {
          vstr__status: 2,
          vstr__created_at_day: '2017-01-05T00:00:00.000Z',
          vstr__sources_list: 'google',
          vstr__count: '1',
        },
        {
          vstr__status: 2,
          vstr__created_at_day: '2017-01-06T00:00:00.000Z',
          vstr__sources_list: null,
          vstr__count: '2',
        },
      ]);
    });
  });

  if (getEnv('nativeSqlPlanner') && getEnv('nativeSqlPlannerPreAggregations')) {
    it('string type measure with pre-aggregation', async () => {
      await compiler.compile();

      const query = new PostgresQuery(
        { joinGraph, cubeEvaluator, compiler },
        {
          measures: [
            'visitors_str.sources_list',
            'visitors_str.count',
          ],
          dimensions: ['visitors_str.status'],
          timezone: 'America/Los_Angeles',
          preAggregationsSchema: '',
        }
      );

      const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
      const sqlAndParams = query.buildSqlAndParams();
      expect(preAggregationsDescription[0].tableName).toEqual('vstr_string_measure_rollup');
      expect(sqlAndParams[0]).toContain('vstr_string_measure_rollup');

      return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
        expect(res).toEqual([
          {
            vstr__status: 1,
            vstr__sources_list: 'some, some',
            vstr__count: '2',
          },
          {
            vstr__status: 2,
            vstr__sources_list: 'google',
            vstr__count: '4',
          },
        ]);
      });
    });
  } else {
    it.skip('string type measure with pre-aggregation', async () => {
      // This fixed only in Tesseract
    });
  }
});
