import { getEnv } from '@cubejs-backend/shared';
import { MssqlQuery } from '../../../src/adapter/MssqlQuery';
import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from '../postgres/PostgresDBRunner';

describe('MSSqlCumulativeMeasures', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
    cube(\`visitors\`, {
      sql: \`
      select * from ##visitors
      \`,

      joins: {},

      measures: {
        count: {
          type: 'count'
        },

        unboundedCount: {
          type: 'count',
          rollingWindow: {
            trailing: 'unbounded'
          }
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
          sql: 'created_at'
        },
      },

      preAggregations: {}
    })
    `);

  it('should group by the created_at field on the calculated granularity for unbounded trailing windows without dimension', () => compiler.compile().then(async () => {
    const query = new MssqlQuery(
      { joinGraph, cubeEvaluator, compiler },
      {
        measures: ['visitors.count', 'visitors.unboundedCount'],
        timeDimensions: [
          {
            dimension: 'visitors.createdAt',
            granularity: 'day',
            dateRange: ['2017-01-01', '2017-01-30'],
          },
        ],
        timezone: 'America/Los_Angeles',
        order: [
          {
            id: 'visitors.createdAt',
          },
        ],
      }
    );

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);

    if (getEnv('nativeSqlPlanner')) {
      // Tesseract uses LEFT JOIN + generate_series, which includes all dates with nulls for empty days
      expect(await dbRunner.testQuery(queryAndParams)).toEqual([
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-01T00:00:00.000Z'), visitors__unbounded_count: 1 },
        { visitors__count: 1, visitors__created_at_day: new Date('2017-01-02T00:00:00.000Z'), visitors__unbounded_count: 2 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-03T00:00:00.000Z'), visitors__unbounded_count: 2 },
        { visitors__count: 1, visitors__created_at_day: new Date('2017-01-04T00:00:00.000Z'), visitors__unbounded_count: 3 },
        { visitors__count: 1, visitors__created_at_day: new Date('2017-01-05T00:00:00.000Z'), visitors__unbounded_count: 4 },
        { visitors__count: 2, visitors__created_at_day: new Date('2017-01-06T00:00:00.000Z'), visitors__unbounded_count: 6 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-07T00:00:00.000Z'), visitors__unbounded_count: 6 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-08T00:00:00.000Z'), visitors__unbounded_count: 6 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-09T00:00:00.000Z'), visitors__unbounded_count: 6 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-10T00:00:00.000Z'), visitors__unbounded_count: 6 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-11T00:00:00.000Z'), visitors__unbounded_count: 6 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-12T00:00:00.000Z'), visitors__unbounded_count: 6 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-13T00:00:00.000Z'), visitors__unbounded_count: 6 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-14T00:00:00.000Z'), visitors__unbounded_count: 6 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-15T00:00:00.000Z'), visitors__unbounded_count: 6 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-16T00:00:00.000Z'), visitors__unbounded_count: 6 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-17T00:00:00.000Z'), visitors__unbounded_count: 6 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-18T00:00:00.000Z'), visitors__unbounded_count: 6 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-19T00:00:00.000Z'), visitors__unbounded_count: 6 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-20T00:00:00.000Z'), visitors__unbounded_count: 6 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-21T00:00:00.000Z'), visitors__unbounded_count: 6 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-22T00:00:00.000Z'), visitors__unbounded_count: 6 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-23T00:00:00.000Z'), visitors__unbounded_count: 6 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-24T00:00:00.000Z'), visitors__unbounded_count: 6 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-25T00:00:00.000Z'), visitors__unbounded_count: 6 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-26T00:00:00.000Z'), visitors__unbounded_count: 6 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-27T00:00:00.000Z'), visitors__unbounded_count: 6 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-28T00:00:00.000Z'), visitors__unbounded_count: 6 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-29T00:00:00.000Z'), visitors__unbounded_count: 6 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-30T00:00:00.000Z'), visitors__unbounded_count: 6 },
      ]);
    } else {
      // BaseQuery uses INNER JOIN + VALUES, which reduces nulls and returns only days with actual data
      expect(await dbRunner.testQuery(queryAndParams)).toEqual([
        {
          visitors__count: 1,
          visitors__created_at_day: new Date('2017-01-02T00:00:00.000Z'),
          visitors__unbounded_count: 2,
        },
        {
          visitors__count: 1,
          visitors__created_at_day: new Date('2017-01-04T00:00:00.000Z'),
          visitors__unbounded_count: 3,
        },
        {
          visitors__count: 1,
          visitors__created_at_day: new Date('2017-01-05T00:00:00.000Z'),
          visitors__unbounded_count: 4,
        },
        {
          visitors__count: 2,
          visitors__created_at_day: new Date('2017-01-06T00:00:00.000Z'),
          visitors__unbounded_count: 6,
        }
      ]);
    }
  }));

  it('should group by the created_at field on the calculated granularity for unbounded trailing windows with dimension', () => compiler.compile().then(async () => {
    const query = new MssqlQuery(
      { joinGraph, cubeEvaluator, compiler },
      {
        measures: ['visitors.count', 'visitors.unboundedCount'],
        dimensions: ['visitors.source'],
        timeDimensions: [
          {
            dimension: 'visitors.createdAt',
            granularity: 'day',
            dateRange: ['2017-01-01', '2017-01-30'],
          },
        ],
        timezone: 'America/Los_Angeles',
        order: [
          {
            id: 'visitors.createdAt',
          },
        ],
      }
    );

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);

    if (getEnv('nativeSqlPlanner')) {
      // Tesseract uses LEFT JOIN + generate_series, which includes all dates with nulls for empty days
      expect(await dbRunner.testQuery(queryAndParams)).toEqual([
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-01T00:00:00.000Z'), visitors__source: null, visitors__unbounded_count: 1 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-02T00:00:00.000Z'), visitors__source: null, visitors__unbounded_count: 1 },
        { visitors__count: 1, visitors__created_at_day: new Date('2017-01-02T00:00:00.000Z'), visitors__source: 'some', visitors__unbounded_count: 1 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-03T00:00:00.000Z'), visitors__source: null, visitors__unbounded_count: 1 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-03T00:00:00.000Z'), visitors__source: 'some', visitors__unbounded_count: 1 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-04T00:00:00.000Z'), visitors__source: null, visitors__unbounded_count: 1 },
        { visitors__count: 1, visitors__created_at_day: new Date('2017-01-04T00:00:00.000Z'), visitors__source: 'some', visitors__unbounded_count: 2 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-05T00:00:00.000Z'), visitors__source: null, visitors__unbounded_count: 1 },
        { visitors__count: 1, visitors__created_at_day: new Date('2017-01-05T00:00:00.000Z'), visitors__source: 'google', visitors__unbounded_count: 1 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-05T00:00:00.000Z'), visitors__source: 'some', visitors__unbounded_count: 2 },
        { visitors__count: 2, visitors__created_at_day: new Date('2017-01-06T00:00:00.000Z'), visitors__source: null, visitors__unbounded_count: 3 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-06T00:00:00.000Z'), visitors__source: 'google', visitors__unbounded_count: 1 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-06T00:00:00.000Z'), visitors__source: 'some', visitors__unbounded_count: 2 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-07T00:00:00.000Z'), visitors__source: null, visitors__unbounded_count: 3 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-07T00:00:00.000Z'), visitors__source: 'google', visitors__unbounded_count: 1 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-07T00:00:00.000Z'), visitors__source: 'some', visitors__unbounded_count: 2 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-08T00:00:00.000Z'), visitors__source: null, visitors__unbounded_count: 3 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-08T00:00:00.000Z'), visitors__source: 'google', visitors__unbounded_count: 1 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-08T00:00:00.000Z'), visitors__source: 'some', visitors__unbounded_count: 2 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-09T00:00:00.000Z'), visitors__source: null, visitors__unbounded_count: 3 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-09T00:00:00.000Z'), visitors__source: 'google', visitors__unbounded_count: 1 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-09T00:00:00.000Z'), visitors__source: 'some', visitors__unbounded_count: 2 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-10T00:00:00.000Z'), visitors__source: null, visitors__unbounded_count: 3 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-10T00:00:00.000Z'), visitors__source: 'google', visitors__unbounded_count: 1 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-10T00:00:00.000Z'), visitors__source: 'some', visitors__unbounded_count: 2 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-11T00:00:00.000Z'), visitors__source: null, visitors__unbounded_count: 3 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-11T00:00:00.000Z'), visitors__source: 'google', visitors__unbounded_count: 1 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-11T00:00:00.000Z'), visitors__source: 'some', visitors__unbounded_count: 2 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-12T00:00:00.000Z'), visitors__source: null, visitors__unbounded_count: 3 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-12T00:00:00.000Z'), visitors__source: 'google', visitors__unbounded_count: 1 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-12T00:00:00.000Z'), visitors__source: 'some', visitors__unbounded_count: 2 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-13T00:00:00.000Z'), visitors__source: null, visitors__unbounded_count: 3 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-13T00:00:00.000Z'), visitors__source: 'google', visitors__unbounded_count: 1 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-13T00:00:00.000Z'), visitors__source: 'some', visitors__unbounded_count: 2 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-14T00:00:00.000Z'), visitors__source: null, visitors__unbounded_count: 3 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-14T00:00:00.000Z'), visitors__source: 'google', visitors__unbounded_count: 1 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-14T00:00:00.000Z'), visitors__source: 'some', visitors__unbounded_count: 2 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-15T00:00:00.000Z'), visitors__source: null, visitors__unbounded_count: 3 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-15T00:00:00.000Z'), visitors__source: 'google', visitors__unbounded_count: 1 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-15T00:00:00.000Z'), visitors__source: 'some', visitors__unbounded_count: 2 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-16T00:00:00.000Z'), visitors__source: null, visitors__unbounded_count: 3 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-16T00:00:00.000Z'), visitors__source: 'google', visitors__unbounded_count: 1 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-16T00:00:00.000Z'), visitors__source: 'some', visitors__unbounded_count: 2 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-17T00:00:00.000Z'), visitors__source: null, visitors__unbounded_count: 3 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-17T00:00:00.000Z'), visitors__source: 'google', visitors__unbounded_count: 1 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-17T00:00:00.000Z'), visitors__source: 'some', visitors__unbounded_count: 2 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-18T00:00:00.000Z'), visitors__source: null, visitors__unbounded_count: 3 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-18T00:00:00.000Z'), visitors__source: 'google', visitors__unbounded_count: 1 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-18T00:00:00.000Z'), visitors__source: 'some', visitors__unbounded_count: 2 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-19T00:00:00.000Z'), visitors__source: null, visitors__unbounded_count: 3 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-19T00:00:00.000Z'), visitors__source: 'google', visitors__unbounded_count: 1 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-19T00:00:00.000Z'), visitors__source: 'some', visitors__unbounded_count: 2 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-20T00:00:00.000Z'), visitors__source: null, visitors__unbounded_count: 3 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-20T00:00:00.000Z'), visitors__source: 'google', visitors__unbounded_count: 1 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-20T00:00:00.000Z'), visitors__source: 'some', visitors__unbounded_count: 2 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-21T00:00:00.000Z'), visitors__source: null, visitors__unbounded_count: 3 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-21T00:00:00.000Z'), visitors__source: 'google', visitors__unbounded_count: 1 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-21T00:00:00.000Z'), visitors__source: 'some', visitors__unbounded_count: 2 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-22T00:00:00.000Z'), visitors__source: null, visitors__unbounded_count: 3 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-22T00:00:00.000Z'), visitors__source: 'google', visitors__unbounded_count: 1 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-22T00:00:00.000Z'), visitors__source: 'some', visitors__unbounded_count: 2 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-23T00:00:00.000Z'), visitors__source: null, visitors__unbounded_count: 3 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-23T00:00:00.000Z'), visitors__source: 'google', visitors__unbounded_count: 1 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-23T00:00:00.000Z'), visitors__source: 'some', visitors__unbounded_count: 2 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-24T00:00:00.000Z'), visitors__source: null, visitors__unbounded_count: 3 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-24T00:00:00.000Z'), visitors__source: 'google', visitors__unbounded_count: 1 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-24T00:00:00.000Z'), visitors__source: 'some', visitors__unbounded_count: 2 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-25T00:00:00.000Z'), visitors__source: null, visitors__unbounded_count: 3 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-25T00:00:00.000Z'), visitors__source: 'google', visitors__unbounded_count: 1 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-25T00:00:00.000Z'), visitors__source: 'some', visitors__unbounded_count: 2 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-26T00:00:00.000Z'), visitors__source: null, visitors__unbounded_count: 3 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-26T00:00:00.000Z'), visitors__source: 'google', visitors__unbounded_count: 1 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-26T00:00:00.000Z'), visitors__source: 'some', visitors__unbounded_count: 2 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-27T00:00:00.000Z'), visitors__source: null, visitors__unbounded_count: 3 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-27T00:00:00.000Z'), visitors__source: 'google', visitors__unbounded_count: 1 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-27T00:00:00.000Z'), visitors__source: 'some', visitors__unbounded_count: 2 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-28T00:00:00.000Z'), visitors__source: null, visitors__unbounded_count: 3 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-28T00:00:00.000Z'), visitors__source: 'google', visitors__unbounded_count: 1 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-28T00:00:00.000Z'), visitors__source: 'some', visitors__unbounded_count: 2 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-29T00:00:00.000Z'), visitors__source: null, visitors__unbounded_count: 3 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-29T00:00:00.000Z'), visitors__source: 'google', visitors__unbounded_count: 1 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-29T00:00:00.000Z'), visitors__source: 'some', visitors__unbounded_count: 2 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-30T00:00:00.000Z'), visitors__source: null, visitors__unbounded_count: 3 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-30T00:00:00.000Z'), visitors__source: 'google', visitors__unbounded_count: 1 },
        { visitors__count: null, visitors__created_at_day: new Date('2017-01-30T00:00:00.000Z'), visitors__source: 'some', visitors__unbounded_count: 2 },
      ]);
    } else {
      // BaseQuery uses INNER JOIN + VALUES, which reduces nulls and returns only days with actual data
      expect(await dbRunner.testQuery(queryAndParams)).toEqual([
        {
          visitors__count: 1,
          visitors__created_at_day: new Date('2017-01-02T00:00:00.000Z'),
          visitors__source: 'some',
          visitors__unbounded_count: 1
        },
        {
          visitors__count: 1,
          visitors__created_at_day: new Date('2017-01-04T00:00:00.000Z'),
          visitors__source: 'some',
          visitors__unbounded_count: 2,
        },
        {
          visitors__count: 1,
          visitors__created_at_day: new Date('2017-01-05T00:00:00.000Z'),
          visitors__source: 'google',
          visitors__unbounded_count: 1,
        },
        {
          visitors__count: 2,
          visitors__created_at_day: new Date('2017-01-06T00:00:00.000Z'),
          visitors__source: null,
          visitors__unbounded_count: 3,
        }
      ]);
    }
  }));
});
