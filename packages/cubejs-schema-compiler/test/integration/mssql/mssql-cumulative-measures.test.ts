import { MssqlQuery } from '../../../src/adapter/MssqlQuery';
import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { MSSqlDbRunner } from './MSSqlDbRunner';

describe('MSSqlCumulativeMeasures', () => {
  jest.setTimeout(200000);

  const dbRunner = new MSSqlDbRunner();

  afterAll(async () => {
    await dbRunner.tearDown();
  });

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

    expect(await dbRunner.testQuery(query.buildSqlAndParams())).toEqual([
      {
        visitors__count: 1,
        visitors__created_at_day: new Date('2017-01-03T00:00:00.000Z'),
        visitors__unbounded_count: 2,
      },
      {
        visitors__count: 1,
        visitors__created_at_day: new Date('2017-01-05T00:00:00.000Z'),
        visitors__unbounded_count: 3,
      },
      {
        visitors__count: 1,
        visitors__created_at_day: new Date('2017-01-06T00:00:00.000Z'),
        visitors__unbounded_count: 4,
      },
      {
        visitors__count: 2,
        visitors__created_at_day: new Date('2017-01-07T00:00:00.000Z'),
        visitors__unbounded_count: 6,
      }
    ]);
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

    expect(await dbRunner.testQuery(query.buildSqlAndParams())).toEqual([
      {
        visitors__count: 1,
        visitors__created_at_day: new Date('2017-01-03T00:00:00.000Z'),
        visitors__source: 'some',
        visitors__unbounded_count: 1
      },
      {
        visitors__count: 1,
        visitors__created_at_day: new Date('2017-01-05T00:00:00.000Z'),
        visitors__source: 'some',
        visitors__unbounded_count: 2,
      },
      {
        visitors__count: 1,
        visitors__created_at_day: new Date('2017-01-06T00:00:00.000Z'),
        visitors__source: 'google',
        visitors__unbounded_count: 1,
      },
      {
        visitors__count: 2,
        visitors__created_at_day: new Date('2017-01-07T00:00:00.000Z'),
        visitors__source: null,
        visitors__unbounded_count: 3,
      }
    ]);
  }));
});
