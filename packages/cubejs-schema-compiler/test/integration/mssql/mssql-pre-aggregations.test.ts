import R from 'ramda';
import { MssqlQuery } from '../../../src/adapter/MssqlQuery';
import { prepareCompiler } from '../../unit/PrepareCompiler';
import { MSSqlDbRunner } from './MSSqlDbRunner';
import { createJoinedCubesSchema } from '../../unit/utils';

describe('MSSqlPreAggregations', () => {
  jest.setTimeout(200000);

  const dbRunner = new MSSqlDbRunner();

  afterAll(async () => {
    await dbRunner.tearDown();
  });

  const { compiler, joinGraph, cubeEvaluator } = prepareCompiler(`
    cube(\`visitors\`, {
      sql: \`
      select * from ##visitors
      \`,
      
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
        
        ratio: {
          sql: \`1.0 * \${uniqueSourceCount} / nullif(\${checkinsTotal}, 0)\`,
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
          sql: 'created_at'
        },
        checkinsCount: {
          type: 'number',
          sql: \`\${visitor_checkins.count}\`,
          subQuery: true
        }
      },
      
      segments: {
        google: {
          sql: \`source = 'google'\`
        }
      },
      
      preAggregations: {
        default: {
          type: 'originalSql'
        },
        googleRollup: {
          type: 'rollup',
          measureReferences: [checkinsTotal],
          segmentReferences: [google],
          timeDimensionReference: createdAt,
          granularity: 'day',
        },
        approx: {
          type: 'rollup',
          measureReferences: [countDistinctApprox],
          timeDimensionReference: createdAt,
          granularity: 'day'
        },
        ratioRollup: {
          type: 'rollup',
          measureReferences: [checkinsTotal, uniqueSourceCount],
          timeDimensionReference: createdAt,
          granularity: 'day'
        },
        partitioned: {
          type: 'rollup',
          measureReferences: [checkinsTotal],
          dimensionReferences: [source],
          timeDimensionReference: createdAt,
          granularity: 'day',
          partitionGranularity: 'month',
          refreshKey: {
            every: '1 hour',
            incremental: true,
            updateWindow: '7 day'
          }
        },
        multiStage: {
          useOriginalSqlPreAggregations: true,
          type: 'rollup',
          measureReferences: [checkinsTotal],
          timeDimensionReference: createdAt,
          granularity: 'month',
          partitionGranularity: 'day'
        }
      }
    })
    
    
    cube('visitor_checkins', {
      sql: \`
      select * from ##visitor_checkins
      \`,

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
          sql: 'created_at'
        }
      },
      
      preAggregations: {
        main: {
          type: 'originalSql'
        },
        auto: {
          type: 'autoRollup',
          maxPreAggregations: 20
        }
      }
    })
    
    cube('GoogleVisitors', {
      extends: visitors,
      sql: \`select v.* from \${visitors.sql()} v where v.source = 'google'\`
    })
    `);

  const joinedSchemaCompilers = prepareCompiler(createJoinedCubesSchema());

  function replaceTableName(query, preAggregation, suffix) {
    const [toReplace, params] = query;
    console.log(toReplace);
    preAggregation = Array.isArray(preAggregation) ? preAggregation : [preAggregation];
    return [
      preAggregation.reduce(
        (replacedQuery, desc) => replacedQuery.replace(new RegExp(desc.tableName, 'g'), `##${desc.tableName}_${suffix}`),
        toReplace
      ),
      params,
    ];
  }

  function tempTablePreAggregations(preAggregationsDescriptions) {
    return R.unnest(
      preAggregationsDescriptions.map((desc) => desc.invalidateKeyQueries.concat([[desc.loadSql[0], desc.loadSql[1]]]))
    );
  }

  it('simple pre-aggregation', () => compiler.compile().then(() => {
    const query = new MssqlQuery(
      { joinGraph, cubeEvaluator, compiler },
      {
        measures: ['visitors.count'],
        timeDimensions: [
          {
            dimension: 'visitors.createdAt',
            granularity: 'day',
            dateRange: ['2017-01-01', '2017-01-30'],
          },
        ],
        timezone: 'UTC',
        order: [
          {
            id: 'visitors.createdAt',
          },
        ],
        preAggregationsSchema: '',
      }
    );

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);

    return dbRunner
      .evaluateQueryWithPreAggregations(query)
      .then((res) => {
        expect(res).toEqual([
          {
            visitors__created_at_day: new Date('2017-01-03T00:00:00.000Z'),
            visitors__count: 1,
          },
          {
            visitors__created_at_day: new Date('2017-01-05T00:00:00.000Z'),
            visitors__count: 1,
          },
          {
            visitors__created_at_day: new Date('2017-01-06T00:00:00.000Z'),
            visitors__count: 1,
          },
          {
            visitors__created_at_day: new Date('2017-01-07T00:00:00.000Z'),
            visitors__count: 2,
          },
        ]);
      });
  }));

  it('hourly refresh with 7 day updateWindow', () => compiler.compile()
    .then(() => {
      const query = new MssqlQuery({
        joinGraph,
        cubeEvaluator,
        compiler
      }, {
        measures: [
          'visitors.checkinsTotal'
        ],
        dimensions: [
          'visitors.source'
        ],
        timeDimensions: [{
          dimension: 'visitors.createdAt',
          granularity: 'day',
          dateRange: ['2017-01-01', '2017-01-25']
        }],
        timezone: 'America/Los_Angeles',
        order: [{
          id: 'visitors.createdAt'
        }],
        preAggregationsSchema: ''
      });

      const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();

      expect(preAggregationsDescription[0].invalidateKeyQueries[0][0].replace(/(\r\n|\n|\r)/gm, '')
        .replace(/\s+/g, ' '))
        .toMatch('SELECT CASE WHEN CURRENT_TIMESTAMP < DATEADD(day, 7, CAST(@_1 AS DATETIME2)) THEN FLOOR((DATEDIFF(SECOND,\'1970-01-01\', GETUTCDATE())) / 3600) END');

      return dbRunner
        .evaluateQueryWithPreAggregations(query)
        .then(res => {
          expect(res)
            .toEqual([
              {
                visitors__created_at_day: new Date('2017-01-03T00:00:00.000Z'),
                visitors__checkins_total: 3,
                visitors__source: 'some',
              },
              {
                visitors__created_at_day: new Date('2017-01-05T00:00:00.000Z'),
                visitors__checkins_total: 2,
                visitors__source: 'some',
              },
              {
                visitors__created_at_day: new Date('2017-01-06T00:00:00.000Z'),
                visitors__checkins_total: 1,
                visitors__source: 'google',
              },
              {
                visitors__created_at_day: new Date('2017-01-07T00:00:00.000Z'),
                visitors__checkins_total: 0,
                visitors__source: null
              }

            ]);
        });
    }));

  it('leaf measure pre-aggregation', () => compiler.compile().then(() => {
    const query = new MssqlQuery(
      { joinGraph, cubeEvaluator, compiler },
      {
        measures: ['visitors.ratio'],
        timeDimensions: [
          {
            dimension: 'visitors.createdAt',
            granularity: 'day',
            dateRange: ['2017-01-01', '2017-01-30'],
          },
        ],
        timezone: 'UTC',
        order: [
          {
            id: 'visitors.createdAt',
          },
        ],
        preAggregationsSchema: '',
      }
    );

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);
    expect(preAggregationsDescription[0].loadSql[0]).toMatch(/visitors_ratio/);

    return dbRunner
      .evaluateQueryWithPreAggregations(query)
      .then((res) => {
        expect(res).toEqual([
          {
            visitors__created_at_day: new Date('2017-01-03T00:00:00.000Z'),
            visitors__ratio: 0.333333333333,
          },
          {
            visitors__created_at_day: new Date('2017-01-05T00:00:00.000Z'),
            visitors__ratio: 0.5,
          },
          {
            visitors__created_at_day: new Date('2017-01-06T00:00:00.000Z'),
            visitors__ratio: 1,
          },
          {
            visitors__created_at_day: new Date('2017-01-07T00:00:00.000Z'),
            visitors__ratio: null,
          },
        ]);
      });
  }));

  it('segment', () => compiler.compile().then(() => {
    const query = new MssqlQuery(
      { joinGraph, cubeEvaluator, compiler },
      {
        measures: ['visitors.checkinsTotal'],
        dimensions: [],
        segments: ['visitors.google'],
        timezone: 'UTC',
        preAggregationsSchema: '',
        timeDimensions: [
          {
            dimension: 'visitors.createdAt',
            granularity: 'day',
            dateRange: ['2016-12-30', '2017-01-06'],
          },
        ],
        order: [
          {
            id: 'visitors.createdAt',
          },
        ],
      }
    );

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);

    const queries = tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner
      .evaluateQueryWithPreAggregations(query)
      .then((res) => {
        console.log(JSON.stringify(res));
        expect(res).toEqual([
          {
            visitors__created_at_day: new Date('2017-01-06T00:00:00.000Z'),
            visitors__checkins_total: 1,
          },
        ]);
      });
  }));

  it('aggregating on top of sub-queries without filters', async () => {
    await joinedSchemaCompilers.compiler.compile();
    const query = new MssqlQuery({
      joinGraph: joinedSchemaCompilers.joinGraph,
      cubeEvaluator: joinedSchemaCompilers.cubeEvaluator,
      compiler: joinedSchemaCompilers.compiler,
    },
    {
      dimensions: ['E.eval'],
      measures: ['B.bval_sum'],
      order: [{ id: 'B.bval_sum' }],
    });
    const sql = query.buildSqlAndParams();
    return dbRunner
      .testQuery(sql)
      .then((res) => {
        expect(res).toEqual([
          {
            e__eval: 'E',
            b__bval_sum: 20,
          },
          {
            e__eval: 'F',
            b__bval_sum: 40,
          },
          {
            e__eval: 'G',
            b__bval_sum: 60,
          },
          {
            e__eval: 'H',
            b__bval_sum: 80,
          },
        ]);
      });
  });

  it('aggregating on top of sub-queries with filter', async () => {
    await joinedSchemaCompilers.compiler.compile();
    const query = new MssqlQuery({
      joinGraph: joinedSchemaCompilers.joinGraph,
      cubeEvaluator: joinedSchemaCompilers.cubeEvaluator,
      compiler: joinedSchemaCompilers.compiler,
    },
    {
      dimensions: ['E.eval'],
      measures: ['B.bval_sum'],
      filters: [{
        member: 'E.eval',
        operator: 'equals',
        values: ['E'],
      }],
      order: [{ id: 'B.bval_sum' }],
    });
    const sql = query.buildSqlAndParams();
    return dbRunner
      .testQuery(sql)
      .then((res) => {
        expect(res).toEqual([
          {
            e__eval: 'E',
            b__bval_sum: 20,
          },
        ]);
      });
  });
});
