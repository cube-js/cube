import R from 'ramda';
import { OracleQuery } from '../../../src/adapter/OracleQuery';
import { prepareCompiler } from '../../unit/PrepareCompiler';
import { OracleDbRunner } from './OracleDbRunner';

describe('OraclePreAggregations', () => {
  jest.setTimeout(20000000);

  const dbRunner = new OracleDbRunner();
  beforeAll(async () => {
    await dbRunner.containerLazyInit();
  });

  afterAll(async () => {
    await dbRunner.tearDown();
  });

  const { compiler, joinGraph, cubeEvaluator } = prepareCompiler(`
    cube(\`visitors\`, {
      sql: \`
      SELECT * FROM VISITORS
      \`,

      measures: {
        count: {
          type: 'count'
        },
        
        uniqueSourceCount: {
          sql: 'SOURCE',
          type: 'countDistinct'
        },
        
        countDistinctApprox: {
          sql: 'ID',
          type: 'countDistinctApprox'
        },
      },

      dimensions: {
        id: {
          type: 'number',
          sql: 'ID',
          primaryKey: true
        },
        source: {
          type: 'string',
          sql: 'SOURCE'
        },
        createdAt: {
          type: 'time',
          sql: 'CREATED_AT'
        }
      },
      
      segments: {
        google: {
          sql: \`(CASE WHEN SOURCE = 'google' THEN 1 ELSE 0 END)\`
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

  function tempTablePreAggregations(preAggregationsDescriptions) {
    return R.unnest(preAggregationsDescriptions.map(desc => desc.invalidateKeyQueries.concat([
      [desc.loadSql[0].replace('CREATE TABLE', 'CREATE TABLE'), desc.loadSql[1]]
    ])));
  }

  it('in db timezone', () => compiler.compile().then(() => {
    const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
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
        dateRange: ['2017-01-02T16:00:00', '2017-01-02T18:00:00']
      }],
      order: [{
        id: 'visitors.createdAt'
      }],
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(JSON.stringify(queryAndParams));
    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);

    const queries = tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.evaluateQueryWithPreAggregations(query).then((res) => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [
          {
            visitors__source: 'some',
            visitors__created_at_day: '2017-01-02T00:00:00.000Z',
            visitors__count: 1
          }
        ]
      );
    });
  }));

  it('partitioned', () => compiler.compile().then(() => {
    const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
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
        dateRange: ['2016-12-30', '2017-01-30'],
      }],
      order: [{
        id: 'visitors.createdAt'
      }],
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(JSON.stringify(queryAndParams));
    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);

    const queries = tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.evaluateQueryWithPreAggregations(query).then((res) => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [
          {
            visitors__source: 'some',
            visitors__created_at_day: '2017-01-02T00:00:00.000Z',
            visitors__count: 1
          },
          {
            visitors__source: 'some',
            visitors__created_at_day: '2017-01-04T00:00:00.000Z',
            visitors__count: 1
          },
          {
            visitors__source: 'google',
            visitors__created_at_day: '2017-01-05T00:00:00.000Z',
            visitors__count: 1
          },
          {
            visitors__source: null,
            visitors__created_at_day: '2017-01-06T00:00:00.000Z',
            visitors__count: 2
          }
        ]
      );
    });
  }));

  it('partitioned scheduled refresh', () => compiler.compile().then(async () => {
    const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
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

    expect(res[0][Object.keys(res[0])[0]]).toEqual('2017-01-07T00:00:00.000Z');
  }));

  it('segment', () => compiler.compile().then(() => {
    const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
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
    console.log(JSON.stringify(queryAndParams));
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);

    const queries = tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.evaluateQueryWithPreAggregations(query).then((res) => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [
          {
            visitors__created_at_day: '2017-01-06T00:00:00.000Z',
            visitors__count: 1
          }
        ]
      );
    });
  }));
});
