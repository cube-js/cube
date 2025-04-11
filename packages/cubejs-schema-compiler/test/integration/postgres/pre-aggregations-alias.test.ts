import R from 'ramda';
import { UserError } from '../../../src/compiler/UserError';
import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

describe('PreAggregationsAlias', () => {
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
        sql: 'id',
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

      ratio: {
        sql: \`\${uniqueSourceCount} / nullif(\${checkinsTotal}, 0)\`,
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
        subQuery: true,
        propagateFiltersToSubQuery: true
      }
    },

    segments: {
      google: {
        sql: \`source = 'google'\`
      }
    },

    preAggregations: {
      default: {
        sqlAlias: 'visitors_alias_d',
        type: 'originalSql',
        refreshKey: {
          sql: 'select NOW()'
        },
        indexes: {
          source: {
            columns: ['source', 'created_at']
          }
        },
        partitionGranularity: 'day',
        timeDimensionReference: createdAt
      },
    }
  })


  cube(\`rollup_visitors\`, {
    sql: \`
    select * from visitors WHERE \${FILTER_PARAMS.visitors.createdAt.filter('created_at')}
    \`,
    sqlAlias: 'rvis',

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
        sql: 'id',
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

      ratio: {
        sql: \`\${uniqueSourceCount} / nullif(\${checkinsTotal}, 0)\`,
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
        subQuery: true,
        propagateFiltersToSubQuery: true
      }
    },

    segments: {
      google: {
        sql: \`source = 'google'\`
      }
    },

    preAggregations: {
      veryVeryLongTableNameForPreAggregation: {
        sqlAlias: 'rollupalias',
        type: 'rollup',
        timeDimensionReference: createdAt,
        granularity: 'day',
        measureReferences: [count, revenue],
        dimensionReferences: [source],
      },
    }
  })

  cube(\`rollup_partition_month_visitors\`, {
    sql: \`
    select * from visitors WHERE \${FILTER_PARAMS.visitors.createdAt.filter('created_at')}
    \`,
    sqlAlias: 'rvis',

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
        sql: 'id',
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

      ratio: {
        sql: \`\${uniqueSourceCount} / nullif(\${checkinsTotal}, 0)\`,
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
        subQuery: true,
        propagateFiltersToSubQuery: true
      }
    },

    segments: {
      google: {
        sql: \`source = 'google'\`
      }
    },

    preAggregations: {
      veryVeryLongTableNameForPreAggregation: {
        sqlAlias: 'rollupalias',
        type: 'rollup',
        timeDimensionReference: createdAt,
        partitionGranularity: 'month',
        granularity: 'day',
        measureReferences: [count, revenue],
        dimensionReferences: [source],
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
        sql: 'created_at'
      }
    },

    preAggregations: {
      main: {
        type: 'originalSql',
        sqlAlias: 'pma',
      },
    }
  })

  cube('GoogleVisitors', {
    refreshKey: {
      immutable: true,
    },
    extends: visitors,
    sql: \`select v.* from \${visitors.sql()} v where v.source = 'google'\`,
    sqlAlias: 'googlevis',
  })

  cube('GoogleVisitorsLongName', {
    refreshKey: {
      immutable: true,
    },
    extends: visitors,
    sql: \`select v.* from \${visitors.sql()} v where v.source = 'google'\`,
    sqlAlias: 'veryVeryVeryVeryVeryVeryLongSqlAliasForTestItOnPostgresqlDataBase',
  })

    `);

  function replaceTableName(query, preAggregation, suffix) {
    const [toReplace, params] = query;
    preAggregation = Array.isArray(preAggregation) ? preAggregation : [preAggregation];
    return [
      preAggregation.reduce(
        (replacedQuery, desc) => replacedQuery
          .replace(new RegExp(desc.tableName, 'g'), `${desc.tableName}_${suffix}`)
          .replace(/CREATE INDEX (?!i_)/, `CREATE INDEX i_${suffix}_`),
        toReplace
      ),
      params
    ];
  }

  function tempTablePreAggregations(preAggregationsDescriptions) {
    return R.unnest(preAggregationsDescriptions.map(
      desc => desc.invalidateKeyQueries.concat([
        [desc.loadSql[0].replace('CREATE TABLE', 'CREATE TEMP TABLE'), desc.loadSql[1]]
      ]).concat(
        (desc.indexesSql || []).map(({ sql }) => sql)
      )
    ));
  }

  it('rollup pre-aggregation with sqlAlias', () => compiler.compile().then(() => {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'rollup_visitors.count'
      ],
      timeDimensions: [{
        dimension: 'rollup_visitors.createdAt',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-30']
      }],
      timezone: 'America/Los_Angeles',
      order: [{
        id: 'rollup_visitors.createdAt'
      }],
      preAggregationsSchema: ''
    });

    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    expect(preAggregationsDescription[0].tableName).toEqual('rvis_rollupalias');
    expect(query.buildSqlAndParams()[0]).toContain('rvis_rollupalias');

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      expect(res).toEqual(
        [
          { rvis__created_at_day: '2017-01-02T00:00:00.000Z', rvis__count: '1' },
          { rvis__created_at_day: '2017-01-04T00:00:00.000Z', rvis__count: '1' },
          { rvis__created_at_day: '2017-01-05T00:00:00.000Z', rvis__count: '1' },
          { rvis__created_at_day: '2017-01-06T00:00:00.000Z', rvis__count: '2' }
        ]
      );
    });
  }));

  it('rollup time (partition by month) pre-aggregation with sqlAlias', () => compiler.compile().then(() => {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'rollup_partition_month_visitors.count'
      ],
      timeDimensions: [{
        dimension: 'rollup_partition_month_visitors.createdAt',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-30']
      }],
      timezone: 'America/Los_Angeles',
      order: [{
        id: 'rollup_partition_month_visitors.createdAt'
      }],
      preAggregationsSchema: ''
    });

    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    expect(preAggregationsDescription[0].tableName).toEqual('rvis_rollupalias');
    expect(query.buildSqlAndParams()[0]).toContain('rvis_rollupalias');

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      expect(res).toEqual(
        [
          { rvis__created_at_day: '2017-01-02T00:00:00.000Z', rvis__count: '1' },
          { rvis__created_at_day: '2017-01-04T00:00:00.000Z', rvis__count: '1' },
          { rvis__created_at_day: '2017-01-05T00:00:00.000Z', rvis__count: '1' },
          { rvis__created_at_day: '2017-01-06T00:00:00.000Z', rvis__count: '2' }
        ]
      );
    });
  }));

  it('simple pre-aggregation with sqlAlias', () => compiler.compile().then(() => {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.count'
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
    expect(preAggregationsDescription[0].tableName).toEqual('vis_visitors_alias_d');

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      expect(res).toEqual(
        [
          {
            vis__created_at_day: '2017-01-02T00:00:00.000Z',
            vis__count: '1'
          },
          {
            vis__created_at_day: '2017-01-04T00:00:00.000Z',
            vis__count: '1'
          },
          {
            vis__created_at_day: '2017-01-05T00:00:00.000Z',
            vis__count: '1'
          },
          {
            vis__created_at_day: '2017-01-06T00:00:00.000Z',
            vis__count: '2'
          }
        ]
      );
    });
  }));

  it('immutable partition default refreshKey', () => compiler.compile().then(() => {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'GoogleVisitors.checkinsTotal'
      ],
      dimensions: [
        'GoogleVisitors.source'
      ],
      timeDimensions: [{
        dimension: 'GoogleVisitors.createdAt',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-30']
      }],
      timezone: 'America/Los_Angeles',
      order: [{
        id: 'GoogleVisitors.createdAt'
      }],
      preAggregationsSchema: ''
    });
    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    expect(preAggregationsDescription[0].tableName).toEqual('googlevis_visitors_alias_d');
    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      expect(res).toEqual(
        [{ googlevis__source: 'google', googlevis__created_at_day: '2017-01-05T00:00:00.000Z', googlevis__checkins_total: '1' }]
      );
    });
  }));

  it('check errors for too long name', () => compiler.compile().then(() => {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'GoogleVisitorsLongName.checkinsTotal'
      ],
      dimensions: [
        'GoogleVisitorsLongName.source'
      ],
      timeDimensions: [{
        dimension: 'GoogleVisitorsLongName.createdAt',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-30']
      }],
      timezone: 'America/Los_Angeles',
      order: [{
        id: 'GoogleVisitorsLongName.createdAt'
      }],
      preAggregationsSchema: ''
    });

    try {
      // eslint-disable-next-line no-unused-expressions
      query.preAggregations?.preAggregationsDescription();
    } catch (error) {
      expect(error).toBeInstanceOf(UserError);
    }
  }));
});
