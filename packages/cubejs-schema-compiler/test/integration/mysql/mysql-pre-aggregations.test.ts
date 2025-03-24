import R from 'ramda';
import { MysqlQuery } from '../../../src/adapter/MysqlQuery';
import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { MySqlDbRunner } from './MySqlDbRunner';

describe('MySqlPreAggregations', () => {
  jest.setTimeout(200000);

  const dbRunner = new MySqlDbRunner();

  afterAll(async () => {
    await dbRunner.tearDown();
  });

  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
    cube(\`visitors\`, {
      sql: \`
      select * from visitors
      \`,

      measures: {
        count: {
          type: 'count'
        },

        uniqueSourceCount: {
          sql: 'source',
          type: 'countDistinct'
        },

        countDistinctApprox: {
          sql: 'id',
          type: 'countDistinctApprox'
        },
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
        }
      },

      segments: {
        google: {
          sql: \`source = 'google'\`
        }
      },

      preAggregations: {
        partitioned: {
          type: 'rollup',
          measureReferences: [count],
          dimensionReferences: [source],
          timeDimensionReference: createdAt,
          granularity: 'day',
          partitionGranularity: 'month',
          scheduledRefresh: true
        },
        googleRollup: {
          type: 'rollup',
          measureReferences: [count],
          segmentReferences: [google],
          timeDimensionReference: createdAt,
          granularity: 'day',
        },
      }
    })
    `);

  function replaceTableName(query, preAggregation, suffix) {
    const [toReplace, params] = query;
    console.log(toReplace);
    preAggregation = Array.isArray(preAggregation) ? preAggregation : [preAggregation];
    return [
      preAggregation.reduce((replacedQuery, desc) => replacedQuery.replace(new RegExp(desc.tableName, 'g'), `${desc.tableName}_${suffix}`), toReplace),
      params
    ];
  }

  function tempTablePreAggregations(preAggregationsDescriptions) {
    return R.unnest(preAggregationsDescriptions.map(desc => desc.invalidateKeyQueries.concat([
      [desc.loadSql[0].replace('CREATE TABLE', 'CREATE TEMPORARY TABLE'), desc.loadSql[1]]
    ])));
  }

  it('in db timezone', () => compiler.compile().then(() => {
    const query = new MysqlQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.count'
      ],
      dimensions: [
        'visitors.source'
      ],
      timezone: 'America/Los_Angeles',
      preAggregationsSchema: '',
      timeDimensions: [{
        dimension: 'visitors.createdAt',
        granularity: 'day',
        dateRange: ['2017-01-02T16:00:00.000', '2017-01-02T18:00:00.000'] // TODO fix MySQL pre-aggregation return incorrect results on DST switch
      }],
      order: [{
        id: 'visitors.createdAt'
      }],
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);

    const queries = tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [
          {
            visitors__source: 'some',
            visitors__created_at_day: '2017-01-02 00:00:00',
            visitors__count: 1
          }
        ]
      );
    });
  }));

  it('partitioned', () => compiler.compile().then(() => {
    const query = new MysqlQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.count'
      ],
      dimensions: [
        'visitors.source'
      ],
      timezone: 'America/Los_Angeles',
      preAggregationsSchema: '',
      timeDimensions: [{
        dimension: 'visitors.createdAt',
        granularity: 'day',
        dateRange: ['2016-12-30', '2017-01-30'] // TODO fix MySQL pre-aggregation return incorrect results on DST switch
      }],
      order: [{
        id: 'visitors.createdAt'
      }],
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);

    const queries = tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [
          {
            visitors__source: 'some',
            visitors__created_at_day: '2017-01-02 00:00:00',
            visitors__count: 1
          },
          {
            visitors__source: 'some',
            visitors__created_at_day: '2017-01-04 00:00:00',
            visitors__count: 1
          },
          {
            visitors__source: 'google',
            visitors__created_at_day: '2017-01-05 00:00:00',
            visitors__count: 1
          },
          {
            visitors__source: null,
            visitors__created_at_day: '2017-01-06 00:00:00',
            visitors__count: 2
          }
        ]
      );
    });
  }));

  it('partitioned scheduled refresh', () => compiler.compile().then(async () => {
    const query = new MysqlQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.count'
      ],
      dimensions: [
        'visitors.source'
      ],
      timezone: 'UTC',
      preAggregationsSchema: '',
      timeDimensions: [{
        dimension: 'visitors.createdAt',
        granularity: 'day',
        dateRange: ['2016-12-30', '2017-01-30']
      }],
      order: [{
        id: 'visitors.createdAt'
      }],
    });

    const preAggregations = cubeEvaluator.scheduledPreAggregations();
    const partitionedPreAgg =
        preAggregations.find(p => p.preAggregationName === 'partitioned' && p.cube === 'visitors');

    const minMaxQueries = query.preAggregationStartEndQueries('visitors', partitionedPreAgg?.preAggregation);

    console.log(minMaxQueries);

    const res = await dbRunner.testQueries(minMaxQueries);

    console.log(res);

    expect(res[0][Object.keys(res[0])[0]]).toEqual('2017-01-07 00:00:00');
  }));

  it('segment', () => compiler.compile().then(() => {
    const query = new MysqlQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.count'
      ],
      dimensions: [],
      segments: ['visitors.google'],
      timezone: 'UTC',
      preAggregationsSchema: '',
      timeDimensions: [{
        dimension: 'visitors.createdAt',
        granularity: 'day',
        dateRange: ['2016-12-30', '2017-01-06']
      }],
      order: [{
        id: 'visitors.createdAt'
      }],
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);

    const queries = tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [
          {
            visitors__created_at_day: '2017-01-06 00:00:00',
            visitors__count: 1
          }
        ]
      );
    });
  }));
});
