import { PreAggregationPartitionRangeLoader } from '@cubejs-backend/query-orchestrator';
import {
  getEnv,
} from '@cubejs-backend/shared';
import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { BigqueryQuery } from '../../../src/adapter/BigqueryQuery';
import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

describe('PreAggregations', () => {
  jest.setTimeout(200000);

  // language=JavaScript
  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
    cube(\`visitors\`, {
      sql: \`
      select * from visitors WHERE \${FILTER_PARAMS.visitors.createdAt.filter('created_at')}
      AND \${FILTER_PARAMS.ReferenceOriginalSql.createdAt.filter('created_at')}
      \`,

      joins: {
        visitor_checkins: {
          relationship: 'hasMany',
          sql: \`\${CUBE.id} = \${visitor_checkins.visitor_id}\`
        },

        cards: {
          relationship: 'hasMany',
          sql: \`\${CUBE.id} = \${cards.visitorId}\`
        }
      },

      measures: {
        count: {
          type: 'count'
        },

        countAnother: {
          type: 'count'
        },

        checkinsTotal: {
          sql: \`\${checkinsCount}\`,
          type: 'sum'
        },

        checkinsRollingTotal: {
          sql: \`\${checkinsCount}\`,
          type: 'sum',
          rollingWindow: {
            trailing: 'unbounded'
          }
        },

        checkinsRolling2day: {
          sql: \`\${checkinsCount}\`,
          type: 'sum',
          rollingWindow: {
            trailing: '2 day'
          }
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
        },

        googleUniqueSourceCount: {
          sql: \`\${CUBE.source}\`,
          filters: [{
            sql: \`\${CUBE}.source = 'google'\`
          }],
          type: 'countDistinct'
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
        shortSource: {
          type: 'string',
          sql: \`SUBSTRING(\${source}, 0, 2)\`
        },
        sourceAndId: {
          type: 'string',
          sql: \`\${source} || '_' || \${id}\`,
        },
        createdAt: {
          type: 'time',
          sql: 'created_at',
          granularities: {
            hourTenMinOffset: {
              interval: '1 hour',
              offset: '10 minutes'
            },
            halfYear: {
              interval: '6 months',
              origin: '2017-01-01'
            }
          }
        },
        signedUpAt: {
          type: 'time',
          sql: \`\${createdAt}\`
        },
        createdAtDay: {
          type: 'time',
          sql: \`\${createdAt.day}\`
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
          type: 'originalSql',
          refreshKey: {
            sql: 'select NOW()'
          },
          indexes: {
            source: {
              columns: ['source', 'created_at']
            }
          },
          partitionGranularity: 'month',
          timeDimensionReference: createdAt
        },
        googleRollup: {
          type: 'rollup',
          measureReferences: [checkinsTotal],
          segmentReferences: [google],
          timeDimensionReference: createdAt,
          granularity: 'week',
        },
        approx: {
          type: 'rollup',
          measureReferences: [countDistinctApprox],
          timeDimensionReference: createdAt,
          granularity: 'day'
        },
        multiStage: {
          useOriginalSqlPreAggregations: true,
          type: 'rollup',
          measureReferences: [checkinsTotal],
          timeDimensionReference: createdAt,
          granularity: 'month',
          partitionGranularity: 'day',
          refreshKey: {
            sql: \`SELECT CASE WHEN \${FILTER_PARAMS.visitors.createdAt.filter((from, to) => \`\${to}::timestamp > now()\`)} THEN now() END\`
          }
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
        partitionedHourly: {
          type: 'rollup',
          measureReferences: [checkinsTotal],
          dimensionReferences: [source],
          timeDimensionReference: createdAt,
          granularity: 'hour',
          partitionGranularity: 'hour'
        },
        partitionedHourlyForRange: {
          type: 'rollup',
          measureReferences: [checkinsTotal],
          dimensionReferences: [source, createdAt],
          timeDimensionReference: createdAt,
          granularity: 'hour',
          partitionGranularity: 'hour'
        },
        ratioRollup: {
          type: 'rollup',
          measureReferences: [checkinsTotal, uniqueSourceCount],
          timeDimensionReference: createdAt,
          granularity: 'day'
        },
        uniqueSourceCountRollup: {
          type: 'rollup',
          measures: [uniqueSourceCount],
          dimensions: [source],
          timeDimension: createdAt,
          granularity: 'day'
        },
        googleUniqueSourceCountRollup: {
          type: 'rollup',
          measures: [googleUniqueSourceCount],
          dimensions: [source],
          timeDimension: signedUpAt,
          granularity: 'day'
        },
        forJoin: {
          type: 'rollup',
          dimensionReferences: [id, source]
        },
        forJoinIncCards: {
          type: 'rollup',
          dimensionReferences: [id, source, cards.visitorId]
        },
        partitionedHourlyForJoin: {
          type: 'rollup',
          dimensionReferences: [id, source],
          timeDimensionReference: createdAt,
          granularity: 'hour',
          partitionGranularity: 'hour'
        },
        partitionedRolling: {
          type: 'rollup',
          measureReferences: [checkinsRollingTotal, checkinsRolling2day],
          dimensionReferences: [source],
          timeDimensionReference: createdAt,
          granularity: 'hour',
          partitionGranularity: 'month'
        },
        countCustomGranularity: {
          measures: [count],
          timeDimension: createdAt,
          granularity: 'hourTenMinOffset',
          allowNonStrictDateRangeMatch: false
        },
        countAnotherCountCustomGranularity: {
          measures: [countAnother],
          timeDimension: createdAt,
          granularity: 'halfYear',
          allowNonStrictDateRangeMatch: false
        },
        countAnotherCountCustomGranularityNonStrict: {
          measures: [countAnother],
          timeDimension: createdAt,
          granularity: 'halfYear',
          allowNonStrictDateRangeMatch: true
        },
        sourceAndIdRollup: {
          measures: [count],
          dimensions: [sourceAndId, source],
          timeDimension: createdAt,
          granularity: 'hour',
          allowNonStrictDateRangeMatch: true
        },
        visitorsMultiplied: {
          measures: [count],
          dimensions: [visitor_checkins.source],
          timeDimension: createdAt,
          granularity: 'day'
        }
      }
    })

    cube('visitor_checkins2', {
      sql: \`
      select * from visitor_checkins
      \`,

      sqlAlias: 'vc2',

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
        forLambdaS: {
          type: 'rollup',
          measureReferences: [count],
          dimensionReferences: [visitor_id],
          timeDimensionReference: created_at,
          partitionGranularity: 'day',
          granularity: 'day'
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
          type: 'originalSql'
        },
        lambda: {
          type: 'rollupLambda',
          rollups: [visitor_checkins.forLambda, visitor_checkins2.forLambdaS],
        },
        forJoin: {
          type: 'rollup',
          measureReferences: [count],
          dimensionReferences: [visitor_id]
        },
        joined: {
          type: 'rollupJoin',
          measureReferences: [count],
          dimensionReferences: [visitors.source],
          rollupReferences: [visitor_checkins.forJoin, visitors.forJoin],
        },
        forLambda: {
          type: 'rollup',
          measureReferences: [count],
          dimensionReferences: [visitor_id],
          timeDimensionReference: created_at,
          partitionGranularity: 'day',
          granularity: 'day'
        },
        joinedPartitioned: {
          type: 'rollupJoin',
          measureReferences: [count],
          dimensionReferences: [visitors.source],
          timeDimensionReference: visitors.createdAt,
          granularity: 'hour',
          rollupReferences: [visitor_checkins.forJoin, visitors.partitionedHourlyForJoin],
        },
        joinedIncCards: {
          type: 'rollupJoin',
          measureReferences: [count],
          dimensionReferences: [visitors.source, cards.visitorId],
          rollupReferences: [visitor_checkins.forJoin, visitors.forJoinIncCards],
        },
        partitioned: {
          type: 'rollup',
          measureReferences: [count],
          timeDimensionReference: EveryHourVisitors.createdAt,
          granularity: 'day',
          partitionGranularity: 'month',
          scheduledRefresh: true,
          refreshRangeStart: {
            sql: "SELECT NOW() - interval '30 day'"
          },
          refreshKey: {
            every: '1 hour',
            incremental: true,
            updateWindow: '1 day'
          }
        },
        emptyPartitioned: {
          type: 'rollup',
          measureReferences: [count],
          timeDimensionReference: EmptyHourVisitors.createdAt,
          granularity: 'hour',
          partitionGranularity: 'month',
          scheduledRefresh: true,
          refreshKey: {
            every: '1 hour',
            incremental: true,
            updateWindow: '1 day'
          }
        }
      }
    });

    cube('cards', {
      sql: \`
      select * from cards
      \`,

      joins: {
        visitor_checkins: {
          relationship: 'one_to_many',
          sql: \`\${CUBE.visitorId} = \${visitor_checkins.visitor_id}\`
        }
      },

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

        visitorId: {
          type: 'number',
          sql: 'visitor_id'
        }
      },

      preAggregations: {
        forJoin: {
          type: 'rollup',
          measureReferences: [count],
          dimensionReferences: [visitorId]
        },
      }
    });

    cube('GoogleVisitors', {
      refreshKey: {
        immutable: true,
      },
      extends: visitors,
      sql: \`select v.* from \${visitors.sql()} v where v.source = 'google'\`
    })

    cube('EveryHourVisitors', {
      refreshKey: {
        immutable: true,
      },
      extends: visitors,
      sql: \`select v.* from \${visitors.sql()} v where v.source = 'google'\`,

      preAggregations: {
        default: {
          type: 'originalSql',
          refreshKey: {
            sql: 'select NOW()'
          }
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
            updateWindow: '1 day'
          }
        }
      }
    })

    cube('EmptyHourVisitors', {
      extends: EveryHourVisitors,
      sql: \`select v.* from \${visitors.sql()} v where created_at < '2000-01-01'\`
    })

    cube('ReferenceOriginalSql', {
      extends: visitors,
      sql: \`select v.* from \${visitors.sql()} v where v.source = 'google'\`,

      preAggregations: {
        partitioned: {
          type: 'rollup',
          measureReferences: [count],
          dimensionReferences: [source],
          timeDimensionReference: createdAt,
          granularity: 'day',
          partitionGranularity: 'month',
          refreshKey: {
            every: '1 hour',
            incremental: true,
            updateWindow: '1 day'
          },
          useOriginalSqlPreAggregations: true
        }
      }
    })

    cube('LambdaVisitors', {
      extends: visitors,

      preAggregations: {
        partitionedLambda: {
          type: 'rollupLambda',
          rollups: [partitioned, RealTimeLambdaVisitors.partitioned]
        },
        partitioned: {
          type: 'rollup',
          measures: [count],
          dimensions: [id, source],
          timeDimension: createdAt,
          granularity: 'day',
          partitionGranularity: 'month',
          refreshKey: {
            every: '1 hour',
            incremental: true,
            updateWindow: '1 day'
          }
        }
      }
    });

    cube('RealTimeLambdaVisitors', {
      dataSource: 'ksql',
      extends: visitors,

      preAggregations: {
        partitioned: {
          type: 'rollup',
          measures: [count],
          dimensions: [id, source],
          timeDimension: createdAt,
          granularity: 'day',
          build_range_start: { sql: "SELECT DATE_SUB(NOW(), interval '96 hour')" },
          build_range_end: { sql: "SELECT NOW()" },
          partitionGranularity: 'day'
        }
      }
    });

    view('visitors_view', {
      cubes: [{
        join_path: visitors,
        includes: '*'
      }]
    });

    view('cards_visitors_checkins_view', {
      cubes: [
        {
          join_path: visitors,
          includes: ['count', 'createdAt']
        },
        {
          join_path: visitors.cards,
          includes: [{ name: 'visitorId', alias: 'visitorIdFromCards'}]
        },
        {
          join_path: visitors.cards.visitor_checkins,
          includes: ['source']
        }
      ]
    });

    cube('cube_pre_agg_proxy_a', {
      sql: \`SELECT '2025-10-01 12:00:00'::timestamp as starts_at\`,

      dimensions: {
        starts_at: {
          sql: \`\${CUBE}.starts_at\`,
          type: 'time'
        }
      }
    });

    cube('cube_pre_agg_proxy_b', {
      sql: \`SELECT 'id' as id\`,

      joins: {
        cube_pre_agg_proxy_a: {
          relationship: 'one_to_one',
          sql: '1 = 1'
        }
      },

      dimensions: {
        id: {
          sql: \`\${CUBE}.id\`,
          type: 'string',
          primary_key: true
        },

        terminal_date: {
          type: 'time',
          sql: \`\${cube_pre_agg_proxy_a.starts_at}\`
        }
      },

      pre_aggregations: {
        main: {
          time_dimension: terminal_date,
          granularity: 'day'
        }
      }
    });

    cube('cube_1', {
      sql: \`SELECT 1 as id, 'dim_1' as dim_1\`,

      joins: {
        cube_2: {
          relationship: 'many_to_one',
          sql: \`\${CUBE.dim_1} = \${cube_2.dim_1}\`
        }
      },

      dimensions: {
        id: {
          sql: 'id',
          type: 'string',
          primary_key: true
        },

        dim_1: {
          sql: 'dim_1',
          type: 'string'
        },
      },

      pre_aggregations: {
        aaa: {
          dimensions: [
            dim_1
          ]
        },
        rollupJoin: {
          type: 'rollupJoin',
          dimensions: [
            dim_1,
            CUBE.cube_2.dim_1,
            CUBE.cube_2.dim_2  // XXX
          ],
          rollups: [
            aaa,
            cube_2.bbb
          ]
        }
      }
    });

    cube('cube_2', {
      sql: \`SELECT 2 as id, 'dim_1' as dim_1, 'dim_2' as dim_2\`,

      dimensions: {
        id: {
          sql: 'id',
          type: 'string',
          primary_key: true
        },

        dim_1: {
          sql: 'dim_1',
          type: 'string'
        },

        dim_2: {
          sql: 'dim_2',
          type: 'string'
        },
      },

      pre_aggregations: {
        bbb: {
          dimensions: [
            dim_1,
            dim_2,
          ]
        }
      }
    });

    cube('cube_x', {
      sql: \`SELECT 1 as id, 'dim_x' as dim_x\`,

      joins: {
        cube_y: {
          relationship: 'many_to_one',
          sql: \`\${CUBE.dim_x} = \${cube_y.dim_x}\`
        }
      },

      dimensions: {
        id: {
          sql: 'id',
          type: 'string',
          primary_key: true
        },

        dim_x: {
          sql: 'dim_x',
          type: 'string'
        },
      },

      pre_aggregations: {
        xxx: {
          dimensions: [
            dim_x
          ]
        },
        rollupJoinThreeCubes: {
          type: 'rollupJoin',
          dimensions: [
            dim_x,
            cube_y.dim_y,
            cube_z.dim_z
          ],
          rollups: [
            xxx,
            cube_y.yyy,
            cube_z.zzz
          ]
        }
      }
    });

    cube('cube_y', {
      sql: \`SELECT 2 as id, 'dim_x' as dim_x, 'dim_y' as dim_y\`,

      joins: {
        cube_z: {
          relationship: 'many_to_one',
          sql: \`\${CUBE.dim_y} = \${cube_z.dim_y}\`
        }
      },

      dimensions: {
        id: {
          sql: 'id',
          type: 'string',
          primary_key: true
        },

        dim_x: {
          sql: 'dim_x',
          type: 'string'
        },

        dim_y: {
          sql: 'dim_y',
          type: 'string'
        },
      },

      pre_aggregations: {
        yyy: {
          dimensions: [
            dim_x,
            dim_y,
          ]
        }
      }
    });

    cube('cube_z', {
      sql: \`SELECT 3 as id, 'dim_y' as dim_y, 'dim_z' as dim_z\`,

      dimensions: {
        id: {
          sql: 'id',
          type: 'string',
          primary_key: true
        },

        dim_y: {
          sql: 'dim_y',
          type: 'string'
        },

        dim_z: {
          sql: 'dim_z',
          type: 'string'
        },
      },

      pre_aggregations: {
        zzz: {
          dimensions: [
            dim_y,
            dim_z,
          ]
        }
      }
    });

    cube('cube_a', {
      sql: \`SELECT 1 as id, 'dim_a' as dim_a\`,

      joins: {
        cube_b: {
          relationship: 'many_to_one',
          sql: \`\${CUBE.dim_a} = \${cube_b.dim_a}\`
        },
        cube_c: {
          relationship: 'many_to_one',
          sql: \`\${CUBE.dim_a} = \${cube_c.dim_a}\`
        }
      },

      dimensions: {
        id: {
          sql: 'id',
          type: 'string',
          primary_key: true
        },

        dim_a: {
          sql: 'dim_a',
          type: 'string'
        },

        dim_b: {
          sql: 'dim_b',
          type: 'string'
        },
      },

      pre_aggregations: {
        aaa_rollup: {
          dimensions: [
            dim_a
          ]
        },
        rollupJoinAB: {
          type: 'rollupJoin',
          dimensions: [
            dim_a,
            CUBE.cube_b.dim_b,
            CUBE.cube_b.cube_c.dim_c
          ],
          rollups: [
            aaa_rollup,
            cube_b.bbb_rollup
          ]
        }
      }
    });

    cube('cube_b', {
      sql: \`SELECT 2 as id, 'dim_a' as dim_a, 'dim_b' as dim_b\`,

      joins: {
        cube_c: {
          relationship: 'many_to_one',
          sql: \`\${CUBE.dim_b} = \${cube_c.dim_b}\`
        }
      },

      dimensions: {
        id: {
          sql: 'id',
          type: 'string',
          primary_key: true
        },

        dim_a: {
          sql: 'dim_a',
          type: 'string'
        },

        dim_b: {
          sql: 'dim_b',
          type: 'string'
        },
      },

      pre_aggregations: {
        bbb_rollup: {
          dimensions: [
            dim_a,
            dim_b,
            cube_c.dim_c
          ]
        }
      }
    });

    cube('cube_c', {
      sql: \`SELECT 3 as id, 'dim_a' as dim_a, 'dim_b' as dim_b, 'dim_c' as dim_c\`,

      dimensions: {
        id: {
          sql: 'id',
          type: 'string',
          primary_key: true
        },

        dim_a: {
          sql: 'dim_a',
          type: 'string'
        },

        dim_b: {
          sql: 'dim_b',
          type: 'string'
        },

        dim_c: {
          sql: 'dim_c',
          type: 'string'
        },
      }
    });

    view('view_abc', {
      cubes: [
        {
          join_path: cube_a,
          includes: ['dim_a']
        },
        {
          join_path: cube_a.cube_b,
          includes: ['dim_b']
        },
        {
          join_path: cube_a.cube_b.cube_c,
          includes: ['dim_c']
        }
      ]
    });

    // Cube with not full paths in rollupJoin pre-aggregation
    cube('cube_a_to_fail_pre_agg', {
      sql: \`SELECT 1 as id, 'dim_a' as dim_a\`,

      joins: {
        cube_b: {
          relationship: 'many_to_one',
          sql: \`\${CUBE.dim_a} = \${cube_b.dim_a}\`
        },
        cube_c: {
          relationship: 'many_to_one',
          sql: \`\${CUBE.dim_a} = \${cube_c.dim_a}\`
        }
      },

      dimensions: {
        id: {
          sql: 'id',
          type: 'string',
          primary_key: true
        },

        dim_a: {
          sql: 'dim_a',
          type: 'string'
        },

        dim_b: {
          sql: 'dim_b',
          type: 'string'
        },
      },

      pre_aggregations: {
        aaa_rollup: {
          dimensions: [
            dim_a
          ]
        },
        rollupJoinAB: {
          type: 'rollupJoin',
          dimensions: [
            dim_a,
            cube_b.dim_b,
            cube_c.dim_c
          ],
          rollups: [
            aaa_rollup,
            cube_b.bbb_rollup
          ]
        }
      }
    });

    // Models with transitive joins for rollupJoin matching
    cube('merchant_dims', {
      sql: \`
        SELECT 101 AS merchant_sk, 'M1' AS merchant_id
        UNION ALL
        SELECT 102 AS merchant_sk, 'M2' AS merchant_id
      \`,

      dimensions: {
        merchant_sk: {
          sql: 'merchant_sk',
          type: 'number',
          primary_key: true
        },
        merchant_id: {
          sql: 'merchant_id',
          type: 'string'
        }
      }
    });

    cube('product_dims', {
      sql: \`
        SELECT 201 AS product_sk, 'P1' AS product_id
        UNION ALL
        SELECT 202 AS product_sk, 'P2' AS product_id
      \`,

      dimensions: {
        product_sk: {
          sql: 'product_sk',
          type: 'number',
          primary_key: true
        },
        product_id: {
          sql: 'product_id',
          type: 'string'
        }
      }
    });

    cube('merchant_and_product_dims', {
      sql: \`
        SELECT 'M1' AS merchant_id, 'P1' AS product_id, 'Organic' AS acquisition_channel, 'SOLD' AS status
        UNION ALL
        SELECT 'M1' AS merchant_id, 'P2' AS product_id, 'Paid' AS acquisition_channel, 'PAID' AS status
        UNION ALL
        SELECT 'M2' AS merchant_id, 'P1' AS product_id, 'Referral' AS acquisition_channel, 'RETURNED' AS status
      \`,

      dimensions: {
        product_id: {
          sql: 'product_id',
          type: 'string',
          primary_key: true
        },
        merchant_id: {
          sql: 'merchant_id',
          type: 'string',
          primary_key: true
        },
        status: {
          sql: 'status',
          type: 'string'
        },
        acquisition_channel: {
          sql: 'acquisition_channel',
          type: 'string'
        }
      },

      pre_aggregations: {
        bridge_rollup: {
          dimensions: [
            merchant_id,
            product_id,
            acquisition_channel,
            status
          ]
        }
      }
    });

    cube('other_facts', {
      sql: \`
        SELECT 1 AS id, 1 AS fact_id, 'OF1' AS fact
        UNION ALL
        SELECT 2 AS id, 2 AS fact_id, 'OF2' AS fact
        UNION ALL
        SELECT 3 AS id, 3 AS fact_id, 'OF3' AS fact
      \`,

      dimensions: {
        other_fact_id: {
          sql: 'id',
          type: 'number',
          primary_key: true
        },
        fact_id: {
          sql: 'fact_id',
          type: 'number'
        },
        fact: {
          sql: 'fact',
          type: 'string'
        }
      },

      pre_aggregations: {
        bridge_rollup: {
          dimensions: [
            fact_id,
            fact
          ]
        }
      }

    });

    cube('test_facts', {
      sql: \`
        SELECT 1 AS id, 101 AS merchant_sk, 201 AS product_sk, 100 AS amount
        UNION ALL
        SELECT 2 AS id, 101 AS merchant_sk, 202 AS product_sk, 150 AS amount
        UNION ALL
        SELECT 3 AS id, 102 AS merchant_sk, 201 AS product_sk, 200 AS amount
      \`,

      joins: {
        merchant_dims: {
          relationship: 'many_to_one',
          sql: \`\${CUBE.merchant_sk} = \${merchant_dims.merchant_sk}\`
        },
        product_dims: {
          relationship: 'many_to_one',
          sql: \`\${CUBE.product_sk} = \${product_dims.product_sk}\`
        },
        // Transitive join - depends on merchant_dims and product_dims
        merchant_and_product_dims: {
          relationship: 'many_to_one',
          sql: \`\${merchant_dims.merchant_id} = \${merchant_and_product_dims.merchant_id} AND \${product_dims.product_id} = \${merchant_and_product_dims.product_id}\`
        },
        other_facts: {
          relationship: 'one_to_many',
          sql: \`\${CUBE.id} = \${other_facts.fact_id}\`
        },
      },

      dimensions: {
        id: {
          sql: 'id',
          type: 'number',
          primary_key: true
        },
        merchant_sk: {
          sql: 'merchant_sk',
          type: 'number'
        },
        product_sk: {
          sql: 'product_sk',
          type: 'number'
        },
        acquisition_channel: {
          sql: \`\${merchant_and_product_dims.acquisition_channel}\`,
          type: 'string'
        }
      },

      measures: {
        amount_sum: {
          sql: 'amount',
          type: 'sum'
        }
      },

      pre_aggregations: {
        facts_rollup: {
          dimensions: [
            id,
            merchant_sk,
            merchant_dims.merchant_sk,
            merchant_dims.merchant_id,
            merchant_and_product_dims.merchant_id,
            product_sk,
            product_dims.product_sk,
            product_dims.product_id,
            merchant_and_product_dims.product_id,
            acquisition_channel,
            merchant_and_product_dims.status
          ]
        },
        rollupJoinTransitive: {
          type: 'rollupJoin',
          dimensions: [
            merchant_sk,
            product_sk,
            CUBE.merchant_and_product_dims.status,
            CUBE.other_facts.fact
          ],
          rollups: [
            facts_rollup,
            other_facts.bridge_rollup
          ]
        }
      }
    });

  `);

  it('simple pre-aggregation', async () => {
    await compiler.compile();

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

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    console.log(query.preAggregations?.preAggregationsDescription());
    expect(query.preAggregations?.preAggregationForQuery?.canUsePreAggregation).toEqual(true);

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      expect(res).toEqual(
        [
          {
            visitors__created_at_day: '2017-01-02T00:00:00.000Z',
            visitors__count: '1'
          },
          {
            visitors__created_at_day: '2017-01-04T00:00:00.000Z',
            visitors__count: '1'
          },
          {
            visitors__created_at_day: '2017-01-05T00:00:00.000Z',
            visitors__count: '1'
          },
          {
            visitors__created_at_day: '2017-01-06T00:00:00.000Z',
            visitors__count: '2'
          }
        ]
      );
    });
  });

  if (getEnv('nativeSqlPlanner')) {
    it.skip('FIXME(tesseract): simple pre-aggregation proxy time dimension', () => {
      // Should work after fallback for pre-aggregations is fully turned off
    });
    /* it('simple pre-aggregation proxy time dimension', () => compiler.compile().then(() => {
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'visitors.count'
        ],
        dimensions: [
          'visitors.createdAtDay',
        ],
        timezone: 'America/Los_Angeles',
        order: [{
          id: 'visitors.createdAtDay'
        }],
        preAggregationsSchema: ''
      });

      const queryAndParams = query.buildSqlAndParams();
      console.log(queryAndParams);
      console.log(query.preAggregations?.preAggregationsDescription());
      expect(query.preAggregations?.preAggregationForQuery?.canUsePreAggregation).toEqual(true);

      return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
        expect(res).toEqual(
          [
            {
              visitors__created_at_day: '2016-09-06T00:00:00.000Z',
              visitors__count: '1'
            },
            {
              visitors__created_at_day: '2017-01-02T00:00:00.000Z',
              visitors__count: '1'
            },
            {
              visitors__created_at_day: '2017-01-04T00:00:00.000Z',
              visitors__count: '1'
            },
            {
              visitors__created_at_day: '2017-01-05T00:00:00.000Z',
              visitors__count: '1'
            },
            {
              visitors__created_at_day: '2017-01-06T00:00:00.000Z',
              visitors__count: '2'
            }
          ]
        );
      });
    })); */
  }

  it('simple pre-aggregation (allowNonStrictDateRangeMatch: true)', async () => {
    await compiler.compile();
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.count'
      ],
      timeDimensions: [{
        dimension: 'visitors.createdAt',
        dateRange: ['2017-01-01 00:10:00.000', '2017-01-29 22:59:59.999'],
        granularity: 'hour',
      }],
      timezone: 'America/Los_Angeles',
      preAggregationsSchema: ''
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    expect(query.preAggregations?.preAggregationForQuery?.canUsePreAggregation).toEqual(true);
    expect(queryAndParams[0]).toMatch(/visitors_source_and_id_rollup/);

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      expect(res).toEqual(
        [
          {
            visitors__count: '1',
            visitors__created_at_hour: '2017-01-02T16:00:00.000Z',
          },
          {
            visitors__count: '1',
            visitors__created_at_hour: '2017-01-04T16:00:00.000Z',
          },
          {
            visitors__count: '1',
            visitors__created_at_hour: '2017-01-05T16:00:00.000Z',
          },
          {
            visitors__count: '2',
            visitors__created_at_hour: '2017-01-06T16:00:00.000Z',
          },
        ]
      );
    });
  });

  it('simple pre-aggregation with custom granularity (exact match)', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.count'
      ],
      timeDimensions: [{
        dimension: 'visitors.createdAt',
        dateRange: ['2017-01-01 00:10:00.000', '2017-01-29 22:09:59.999'],
        granularity: 'hourTenMinOffset',
      }],
      timezone: 'America/Los_Angeles',
      preAggregationsSchema: ''
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    expect(query.preAggregations?.preAggregationForQuery?.canUsePreAggregation).toEqual(true);
    expect(queryAndParams[0]).toMatch(/visitors_count_custom_granularity/);

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      expect(res).toEqual(
        [
          {
            visitors__count: '1',
            visitors__created_at_hourTenMinOffset: '2017-01-02T15:10:00.000Z',
          },
          {
            visitors__count: '1',
            visitors__created_at_hourTenMinOffset: '2017-01-04T15:10:00.000Z',
          },
          {
            visitors__count: '1',
            visitors__created_at_hourTenMinOffset: '2017-01-05T15:10:00.000Z',
          },
          {
            visitors__count: '2',
            visitors__created_at_hourTenMinOffset: '2017-01-06T15:10:00.000Z',
          },
        ]
      );
    });
  });

  it('simple pre-aggregation with custom granularity (exact match) 2', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.countAnother'
      ],
      timeDimensions: [{
        dimension: 'visitors.createdAt',
        dateRange: ['2017-01-01 00:00:00.000', '2017-12-31 23:59:59.999'],
        granularity: 'halfYear',
      }],
      timezone: 'UTC',
      preAggregationsSchema: ''
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    expect(query.preAggregations?.preAggregationForQuery?.canUsePreAggregation).toEqual(true);
    expect(queryAndParams[0]).toMatch(/visitors_count_another_count_custom_granularity/);

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      expect(res).toEqual([
        {
          visitors__count_another: '5',
          visitors__created_at_halfYear: '2017-01-01T00:00:00.000Z',
        },
      ]);
    });
  });

  it('pre-aggregation with custom granularity should match its own references (allowNonStrictDateRangeMatch=false)', async () => {
    await compiler.compile();

    const preAggregationId = 'visitors.countAnotherCountCustomGranularity';
    const preAggregations = cubeEvaluator.preAggregations({ preAggregationIds: [preAggregationId] });

    const preAggregation = preAggregations[0];
    if (preAggregation === undefined) {
      throw expect(preAggregation).toBeDefined();
    }

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      ...preAggregation.references,
      preAggregationId: preAggregation.id,
      timezone: 'UTC',
    });

    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    const preAggregationFromQuery = preAggregationsDescription.find(p => p.preAggregationId === preAggregation.id);
    if (preAggregationFromQuery === undefined) {
      throw expect(preAggregationFromQuery).toBeDefined();
    }

    expect(preAggregationFromQuery.preAggregationId).toBe(preAggregationId);
  });

  it('pre-aggregation with custom granularity should match its own references (allowNonStrictDateRangeMatch=true)', async () => {
    await compiler.compile();

    const preAggregationId = 'visitors.countAnotherCountCustomGranularityNonStrict';
    const preAggregations = cubeEvaluator.preAggregations({ preAggregationIds: [preAggregationId] });

    const preAggregation = preAggregations[0];
    if (preAggregation === undefined) {
      throw expect(preAggregation).toBeDefined();
    }

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      ...preAggregation.references,
      preAggregationId: preAggregation.id,
      timezone: 'UTC',
    });

    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    const preAggregationFromQuery = preAggregationsDescription.find(p => p.preAggregationId === preAggregation.id);
    if (preAggregationFromQuery === undefined) {
      throw expect(preAggregationFromQuery).toBeDefined();
    }

    expect(preAggregationFromQuery.preAggregationId).toBe(preAggregationId);
  });

  it('leaf measure pre-aggregation', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.ratio'
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

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);
    expect((<any>preAggregationsDescription)[0].loadSql[0]).toMatch(/visitors_ratio/);

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      expect(res).toEqual(
        [
          {
            visitors__created_at_day: '2017-01-02T00:00:00.000Z',
            visitors__ratio: '0.33333333333333333333'
          },
          {
            visitors__created_at_day: '2017-01-04T00:00:00.000Z',
            visitors__ratio: '0.50000000000000000000'
          },
          {
            visitors__created_at_day: '2017-01-05T00:00:00.000Z',
            visitors__ratio: '1.00000000000000000000'
          },
          {
            visitors__created_at_day: '2017-01-06T00:00:00.000Z',
            visitors__ratio: null
          }
        ]
      );
    });
  });

  it('leaf measure view pre-aggregation', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors_view.ratio'
      ],
      timeDimensions: [{
        dimension: 'visitors_view.createdAt',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-30']
      }],
      timezone: 'America/Los_Angeles',
      order: [{
        id: 'visitors_view.createdAt'
      }],
      preAggregationsSchema: ''
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);
    expect((<any>preAggregationsDescription)[0].loadSql[0]).toMatch(/visitors_ratio/);

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      expect(res).toEqual(
        [
          {
            visitors_view__created_at_day: '2017-01-02T00:00:00.000Z',
            visitors_view__ratio: '0.33333333333333333333'
          },
          {
            visitors_view__created_at_day: '2017-01-04T00:00:00.000Z',
            visitors_view__ratio: '0.50000000000000000000'
          },
          {
            visitors_view__created_at_day: '2017-01-05T00:00:00.000Z',
            visitors_view__ratio: '1.00000000000000000000'
          },
          {
            visitors_view__created_at_day: '2017-01-06T00:00:00.000Z',
            visitors_view__ratio: null
          }
        ]
      );
    });
  });

  it('non-additive measure view pre-aggregation', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors_view.uniqueSourceCount'
      ],
      timeDimensions: [{
        dimension: 'visitors_view.signedUpAt',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-30']
      }],
      timezone: 'America/Los_Angeles',
      order: [{
        id: 'visitors_view.createdAt'
      }],
      preAggregationsSchema: ''
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);
    expect((<any>preAggregationsDescription)[0].loadSql[0]).toMatch(/visitors_ratio/);

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      expect(res).toEqual(
        [
          {
            visitors_view__signed_up_at_day: '2017-01-02T00:00:00.000Z',
            visitors_view__unique_source_count: '1'
          },
          {
            visitors_view__signed_up_at_day: '2017-01-04T00:00:00.000Z',
            visitors_view__unique_source_count: '1'
          },
          {
            visitors_view__signed_up_at_day: '2017-01-05T00:00:00.000Z',
            visitors_view__unique_source_count: '1'
          },
          {
            visitors_view__signed_up_at_day: '2017-01-06T00:00:00.000Z',
            visitors_view__unique_source_count: '0'
          }
        ]
      );
    });
  });

  it('non-additive single value view filter', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors_view.uniqueSourceCount'
      ],
      timeDimensions: [{
        dimension: 'visitors_view.signedUpAt',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-30']
      }],
      filters: [{
        dimension: 'visitors_view.source',
        operator: 'equals',
        values: ['google']
      }],
      timezone: 'America/Los_Angeles',
      order: [{
        id: 'visitors_view.createdAt'
      }],
      preAggregationsSchema: ''
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);
    expect((<any>preAggregationsDescription)[0].loadSql[0]).toMatch(/visitors_unique_source_count/);

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      expect(res).toEqual(
        [
          {
            visitors_view__signed_up_at_day: '2017-01-05T00:00:00.000Z',
            visitors_view__unique_source_count: '1'
          }
        ]
      );
    });
  });

  it('non-additive view dimension', async () => {
    await compiler.compile();
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors_view.uniqueSourceCount'
      ],
      dimensions: [
        'visitors_view.source'
      ],
      timeDimensions: [{
        dimension: 'visitors_view.signedUpAt',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-30']
      }],
      timezone: 'America/Los_Angeles',
      order: [{
        id: 'visitors_view.createdAt'
      }],
      preAggregationsSchema: ''
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);
    expect((<any>preAggregationsDescription)[0].loadSql[0]).toMatch(/visitors_unique_source_count/);

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      expect(res).toEqual(
        [
          {
            visitors_view__source: 'google',
            visitors_view__signed_up_at_day: '2017-01-05T00:00:00.000Z',
            visitors_view__unique_source_count: '1'
          },
          {
            visitors_view__source: 'some',
            visitors_view__signed_up_at_day: '2017-01-02T00:00:00.000Z',
            visitors_view__unique_source_count: '1'
          },
          {
            visitors_view__source: 'some',
            visitors_view__signed_up_at_day: '2017-01-04T00:00:00.000Z',
            visitors_view__unique_source_count: '1'
          },
          {
            visitors_view__source: null,
            visitors_view__signed_up_at_day: '2017-01-06T00:00:00.000Z',
            visitors_view__unique_source_count: '0'
          }
        ]

      );
    });
  });
  it('non-additive proxy but not direct alias dimension', async () => {
    await compiler.compile();
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors_view.uniqueSourceCount'
      ],
      dimensions: [
        'visitors.shortSource'
      ],
      timeDimensions: [{
        dimension: 'visitors_view.signedUpAt',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-30']
      }],
      timezone: 'America/Los_Angeles',
      order: [{
        id: 'visitors_view.createdAt'
      }],
      preAggregationsSchema: ''
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);
    expect((<any>preAggregationsDescription)[0].type).toEqual('originalSql');
  });

  it('non-additive single value view filtered measure', async () => {
    await compiler.compile();
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors_view.googleUniqueSourceCount'
      ],
      timeDimensions: [{
        dimension: 'visitors_view.signedUpAt',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-30']
      }],
      filters: [{
        dimension: 'visitors_view.source',
        operator: 'equals',
        values: ['google']
      }],
      timezone: 'America/Los_Angeles',
      order: [{
        id: 'visitors_view.createdAt'
      }],
      preAggregationsSchema: ''
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);
    expect((<any>preAggregationsDescription)[0].loadSql[0]).toMatch(/visitors_google_unique_source_count/);

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      expect(res).toEqual(
        [
          {
            visitors_view__signed_up_at_day: '2017-01-05T00:00:00.000Z',
            visitors_view__google_unique_source_count: '1'
          }
        ]
      );
    });
  });

  if (!getEnv('nativeSqlPlanner')) {
    it('multiplied measure no match', async () => {
      await compiler.compile();

      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'visitors.count'
        ],
        dimensions: ['visitor_checkins.source'],
        order: [{
          id: 'visitor_checkins.source'
        }],
        timezone: 'America/Los_Angeles',
        preAggregationsSchema: ''
      });

      const queryAndParams = query.buildSqlAndParams();
      console.log(queryAndParams);
      expect(queryAndParams[0]).toMatch(/count\(distinct/ig);
      expect(queryAndParams[0]).toMatch(/visitors_default/ig);
      const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
      console.log(preAggregationsDescription);
      expect((<any>preAggregationsDescription).filter(p => p.type === 'rollup').length).toBe(0);

      return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
        expect(res).toEqual(
          [
            {
              vc__source: 'google',
              visitors__count: '1'
            },
            {
              vc__source: null,
              visitors__count: '6'
            },
          ]
        );
      });
    });
  } else {
    it.skip('FIXME(tesseract): multiplied measure no match', async () => {
      // This should be fixed in Tesseract.

    });
  }

  it('multiplied measure match', async () => {
    await compiler.compile();
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.count'
      ],
      dimensions: ['visitor_checkins.source'],
      timeDimensions: [{
        dimension: 'visitors.createdAt',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-30']
      }],
      order: [{
        id: 'visitors.createdAt'
      }, {
        id: 'visitor_checkins.source'
      }],
      timezone: 'America/Los_Angeles',
      preAggregationsSchema: ''
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);
    expect((<any>preAggregationsDescription)[0].loadSql[0]).toMatch(/multiplied/);

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      expect(res).toEqual(
        [
          {
            vc__source: 'google',
            visitors__count: '1',
            visitors__created_at_day: '2017-01-02T00:00:00.000Z',
          },
          {
            vc__source: null,
            visitors__count: '1',
            visitors__created_at_day: '2017-01-02T00:00:00.000Z',
          },
          {
            vc__source: null,
            visitors__count: '1',
            visitors__created_at_day: '2017-01-04T00:00:00.000Z',
          },
          {
            vc__source: null,
            visitors__count: '1',
            visitors__created_at_day: '2017-01-05T00:00:00.000Z',
          },
          {
            vc__source: null,
            visitors__count: '2',
            visitors__created_at_day: '2017-01-06T00:00:00.000Z',
          },
        ]
      );
    });
  });

  if (getEnv('nativeSqlPlanner')) {
    it.skip('FIXME(tesseract): non-match because of join tree difference (through the view)', () => {
      // This should be fixed in Tesseract.
    });
  } else {
    it('non-match because of join tree difference (through the view)', async () => {
      await compiler.compile();
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'cards_visitors_checkins_view.count'
        ],
        dimensions: ['cards_visitors_checkins_view.source'],
        timeDimensions: [{
          dimension: 'cards_visitors_checkins_view.createdAt',
          granularity: 'day',
          dateRange: ['2017-01-01', '2017-01-30']
        }],
        order: [{
          id: 'cards_visitors_checkins_view.createdAt'
        }, {
          id: 'cards_visitors_checkins_view.source'
        }],
        timezone: 'America/Los_Angeles',
        preAggregationsSchema: ''
      });

      const queryAndParams = query.buildSqlAndParams();
      console.log(queryAndParams);
      expect((<any>query).preAggregations.preAggregationForQuery).toBeUndefined();

      return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
        expect(res).toEqual(
          [
            {
              cards_visitors_checkins_view__count: '1',
              cards_visitors_checkins_view__created_at_day: '2017-01-02T00:00:00.000Z',
              cards_visitors_checkins_view__source: 'google',
            },
            {
              cards_visitors_checkins_view__count: '1',
              cards_visitors_checkins_view__created_at_day: '2017-01-02T00:00:00.000Z',
              cards_visitors_checkins_view__source: null,
            },
            {
              cards_visitors_checkins_view__count: '1',
              cards_visitors_checkins_view__created_at_day: '2017-01-04T00:00:00.000Z',
              cards_visitors_checkins_view__source: null,
            },
            {
              cards_visitors_checkins_view__count: '1',
              cards_visitors_checkins_view__created_at_day: '2017-01-05T00:00:00.000Z',
              cards_visitors_checkins_view__source: null,
            },
            {
              cards_visitors_checkins_view__count: '2',
              cards_visitors_checkins_view__created_at_day: '2017-01-06T00:00:00.000Z',
              cards_visitors_checkins_view__source: null,
            },
          ]
        );
      });
    });
  }

  if (getEnv('nativeSqlPlanner')) {
    it.skip('FIXME(tesseract): non-match because of requesting only joined cube members', () => {
      // This should be fixed in Tesseract.
    });
  } else {
    it('non-match because of requesting only joined cube members', async () => {
      await compiler.compile();
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        dimensions: ['visitor_checkins.source'],
        order: [{
          id: 'visitor_checkins.source'
        }],
        timezone: 'America/Los_Angeles',
        preAggregationsSchema: ''
      });

      const queryAndParams = query.buildSqlAndParams();
      console.log(queryAndParams);
      expect((<any>query).preAggregations.preAggregationForQuery).toBeUndefined();

      return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
        expect(res).toEqual([
          {
            vc__source: 'google',
          },
          {
            vc__source: null,
          },
        ]);
      });
    });
  }

  it('non-leaf additive measure', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors_view.count'
      ],
      dimensions: ['visitors_view.sourceAndId'],
      timezone: 'America/Los_Angeles',
      order: [{
        id: 'visitors_view.sourceAndId'
      }],
      preAggregationsSchema: ''
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);
    expect((<any>preAggregationsDescription)[0].loadSql[0]).toMatch(/visitors_source_and_id_rollup/);

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      expect(res).toEqual(
        [
          {
            visitors_view__count: '1',
            visitors_view__source_and_id: 'google_3'
          },
          {
            visitors_view__count: '1',
            visitors_view__source_and_id: 'some_1'
          },
          {
            visitors_view__count: '1',
            visitors_view__source_and_id: 'some_2'
          },
          {
            visitors_view__count: '3',
            visitors_view__source_and_id: null
          }
        ]
      );
    });
  });

  it('non-leaf additive measure with time dimension', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors_view.count'
      ],
      dimensions: ['visitors_view.sourceAndId'],
      timezone: 'America/Los_Angeles',
      timeDimensions: [{
        dimension: 'visitors_view.signedUpAt',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-30']
      }],
      order: [{
        id: 'visitors_view.createdAt',
      }, {
        id: 'visitors_view.sourceAndId'
      }],
      preAggregationsSchema: ''
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);
    expect((<any>preAggregationsDescription)[0].loadSql[0]).toMatch(/visitors_source_and_id_rollup/);

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      expect(res).toEqual(
        [
          {
            visitors_view__count: '1',
            visitors_view__signed_up_at_day: '2017-01-05T00:00:00.000Z',
            visitors_view__source_and_id: 'google_3'
          },
          {
            visitors_view__count: '1',
            visitors_view__signed_up_at_day: '2017-01-02T00:00:00.000Z',
            visitors_view__source_and_id: 'some_1'
          },
          {
            visitors_view__count: '1',
            visitors_view__signed_up_at_day: '2017-01-04T00:00:00.000Z',
            visitors_view__source_and_id: 'some_2'
          },
          {
            visitors_view__count: '2',
            visitors_view__signed_up_at_day: '2017-01-06T00:00:00.000Z',
            visitors_view__source_and_id: null
          }
        ]
      );
    });
  });

  it('inherited original sql', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'GoogleVisitors.count'
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

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      expect(res).toEqual(
        [
          {
            google_visitors__created_at_day: '2017-01-05T00:00:00.000Z',
            google_visitors__count: '1'
          }
        ]
      );
    });
  });

  it('immutable partition default refreshKey', async () => {
    await compiler.compile();

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

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    console.log(JSON.stringify(preAggregationsDescription, null, 2));

    expect(preAggregationsDescription[0].invalidateKeyQueries[0][0]).toMatch(/NOW\(\) </);

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      expect(res).toEqual(
        [
          {
            google_visitors__source: 'google',
            google_visitors__created_at_day: '2017-01-05T00:00:00.000Z',
            google_visitors__checkins_total: '1'
          }
        ]
      );
    });
  });

  it('immutable every hour', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'EveryHourVisitors.checkinsTotal'
      ],
      dimensions: [
        'EveryHourVisitors.source'
      ],
      timeDimensions: [{
        dimension: 'EveryHourVisitors.createdAt',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-25']
      }],
      timezone: 'America/Los_Angeles',
      order: [{
        id: 'EveryHourVisitors.createdAt'
      }],
      preAggregationsSchema: ''
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    console.log(JSON.stringify(preAggregationsDescription, null, 2));

    expect(preAggregationsDescription[0].invalidateKeyQueries[0][0]).toMatch(/NOW\(\) </);
    expect(preAggregationsDescription[0].invalidateKeyQueries[0][1][0]).toEqual(
      PreAggregationPartitionRangeLoader.TO_PARTITION_RANGE
    );

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      expect(res).toEqual(
        [
          {
            every_hour_visitors__source: 'google',
            every_hour_visitors__created_at_day: '2017-01-05T00:00:00.000Z',
            every_hour_visitors__checkins_total: '1'
          }
        ]
      );
    });
  });

  it('reference original sql', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'ReferenceOriginalSql.count'
      ],
      dimensions: [
        'ReferenceOriginalSql.source'
      ],
      timeDimensions: [{
        dimension: 'ReferenceOriginalSql.createdAt',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-25']
      }],
      timezone: 'America/Los_Angeles',
      order: [{
        id: 'ReferenceOriginalSql.createdAt'
      }],
      preAggregationsSchema: ''
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    console.log(JSON.stringify(preAggregationsDescription, null, 2));

    // For extended cubes pre-aggregations from parents are treated as local
    expect(preAggregationsDescription[0].tableName).toEqual('reference_original_sql_default');

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      expect(res).toEqual(
        [
          {
            reference_original_sql__source: 'google',
            reference_original_sql__created_at_day: '2017-01-05T00:00:00.000Z',
            reference_original_sql__count: '1'
          }
        ]
      );
    });
  });

  it('partitioned scheduled refresh', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitor_checkins.count'
      ],
      timeDimensions: [{
        dimension: 'EveryHourVisitors.createdAt',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-25']
      }],
      timezone: 'UTC',
      order: [{
        id: 'EveryHourVisitors.createdAt'
      }],
      preAggregationsSchema: ''
    });

    const preAggregations = cubeEvaluator.scheduledPreAggregations();
    const partitionedPreAgg =
        preAggregations.find(p => p.preAggregationName === 'partitioned' && p.cube === 'visitor_checkins');

    const minMaxQueries = query.preAggregationStartEndQueries('visitor_checkins', partitionedPreAgg?.preAggregation);

    console.log(minMaxQueries);

    expect(minMaxQueries[0][0]).toMatch(/NOW/);

    const res = await dbRunner.testQueries(minMaxQueries);

    expect(res).toEqual(
      [{ max: '2017-01-06T00:00:00.000Z' }]
    );
  });

  it('empty scheduled refresh', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitor_checkins.count'
      ],
      timeDimensions: [{
        dimension: 'EmptyHourVisitors.createdAt',
        granularity: 'hour',
        dateRange: ['2017-01-01', '2017-01-25']
      }],
      timezone: 'UTC',
      order: [{
        id: 'EmptyHourVisitors.createdAt'
      }],
      preAggregationsSchema: ''
    });

    const preAggregations = cubeEvaluator.scheduledPreAggregations();
    const partitionedPreAgg =
        preAggregations.find(p => p.preAggregationName === 'emptyPartitioned' && p.cube === 'visitor_checkins');

    const minMaxQueries = query.preAggregationStartEndQueries('visitor_checkins', partitionedPreAgg?.preAggregation);

    console.log(minMaxQueries);

    const res = await dbRunner.testQueries(minMaxQueries);

    expect(res).toEqual(
      [{ max: null }]
    );
  });

  it('mutable partition default refreshKey', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.checkinsTotal'
      ],
      dimensions: [
        'visitors.source'
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

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    console.log(JSON.stringify(preAggregationsDescription, null, 2));

    expect(preAggregationsDescription[0].invalidateKeyQueries[0][0]).toMatch(/FLOOR/);

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      expect(res).toEqual(
        [
          {
            visitors__source: 'some',
            visitors__created_at_day: '2017-01-02T00:00:00.000Z',
            visitors__checkins_total: '3'
          },
          {
            visitors__source: 'some',
            visitors__created_at_day: '2017-01-04T00:00:00.000Z',
            visitors__checkins_total: '2'
          },
          {
            visitors__source: 'google',
            visitors__created_at_day: '2017-01-05T00:00:00.000Z',
            visitors__checkins_total: '1'
          },
          {
            visitors__source: null,
            visitors__created_at_day: '2017-01-06T00:00:00.000Z',
            visitors__checkins_total: '0'
          }
        ]
      );
    });
  });

  it('hll bigquery rollup', async () => {
    await compiler.compile();

    const query = new BigqueryQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.countDistinctApprox'
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

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription()[0];
    console.log(preAggregationsDescription);

    expect(queryAndParams[0]).toMatch(/HLL_COUNT\.MERGE/);
    expect(preAggregationsDescription.loadSql[0]).toMatch(/HLL_COUNT\.INIT/);
  });

  it('sub query', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.count'
      ],
      order: [{ id: 'visitors.checkinsCount' }],
      dimensions: ['visitors.checkinsCount'],
      timezone: 'America/Los_Angeles',
      preAggregationsSchema: ''
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);

    const queries = dbRunner.tempTablePreAggregations(preAggregationsDescription);
    expect(preAggregationsDescription[1].loadSql[0]).toMatch(/vc_main/);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [
          { visitors__checkins_count: '0', visitors__count: '3' },
          { visitors__checkins_count: '1', visitors__count: '1' },
          { visitors__checkins_count: '2', visitors__count: '1' },
          { visitors__checkins_count: '3', visitors__count: '1' }
        ]
      );
    });
  });

  it('multi-stage', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.checkinsTotal'
      ],
      timezone: 'America/Los_Angeles',
      preAggregationsSchema: '',
      timeDimensions: [{
        dimension: 'visitors.createdAt',
        granularity: 'month',
        dateRange: ['2017-01-01', '2017-01-31']
      }],
      order: [{
        id: 'visitors.createdAt'
      }],
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);

    const queries = dbRunner.tempTablePreAggregations(preAggregationsDescription);

    const desc = preAggregationsDescription.find(e => e.tableName === 'visitors_multi_stage');
    expect(desc.invalidateKeyQueries[0][1][0]).toEqual(PreAggregationPartitionRangeLoader.TO_PARTITION_RANGE);

    const vcMainDesc = preAggregationsDescription.find(e => e.tableName === 'vc_main');
    expect(vcMainDesc.invalidateKeyQueries.length).toEqual(1);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [
          {
            visitors__created_at_month: '2017-01-01T00:00:00.000Z',
            visitors__checkins_total: '6'
          }
        ]
      );
    });
  });

  it('incremental renewal threshold', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.checkinsTotal'
      ],
      dimensions: [
        'visitors.source'
      ],
      timezone: 'America/Los_Angeles',
      preAggregationsSchema: '',
      timeDimensions: [{
        dimension: 'visitors.createdAt',
        granularity: 'day',
        dateRange: [
          '2017-01-06',
          '2017-01-31'
        ]
      }],
      order: [{
        id: 'visitors.createdAt'
      }],
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    console.log(JSON.stringify(preAggregationsDescription, null, 2));
    const partitionedTables = preAggregationsDescription
      .filter(({ tableName }) => tableName.indexOf('visitors_partitioned') === 0);

    expect(partitionedTables[0].invalidateKeyQueries[0][2].updateWindowSeconds).toEqual(86400 * 7);
    expect(partitionedTables[0].invalidateKeyQueries[0][2].renewalThresholdOutsideUpdateWindow).toEqual(86400);

    const queries = dbRunner.tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [{
          visitors__checkins_total: '0',
          visitors__created_at_day: '2017-01-06T00:00:00.000Z',
          visitors__source: null,
        }]
      );
    });
  });

  it('partitioned', async () => {
    await compiler.compile();
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.checkinsTotal'
      ],
      dimensions: [
        'visitors.source'
      ],
      timezone: 'America/Los_Angeles',
      preAggregationsSchema: '',
      timeDimensions: [{
        dimension: 'visitors.createdAt',
        granularity: 'day',
        dateRange: ['2016-12-30', '2017-01-05']
      }],
      order: [{
        id: 'visitors.createdAt'
      }],
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(JSON.stringify(preAggregationsDescription, null, 2));
    expect(query.preAggregations?.preAggregationForQuery?.canUsePreAggregation).toEqual(true);

    const queries = dbRunner.tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [
          {
            visitors__source: 'some',
            visitors__created_at_day: '2017-01-02T00:00:00.000Z',
            visitors__checkins_total: '3'
          },
          {
            visitors__source: 'some',
            visitors__created_at_day: '2017-01-04T00:00:00.000Z',
            visitors__checkins_total: '2'
          },
          {
            visitors__source: 'google',
            visitors__created_at_day: '2017-01-05T00:00:00.000Z',
            visitors__checkins_total: '1'
          }
        ]
      );
    });
  });

  it('partitioned inDateRange', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.checkinsTotal'
      ],
      dimensions: [
        'visitors.source'
      ],
      timezone: 'America/Los_Angeles',
      preAggregationsSchema: '',
      filters: [{
        member: 'visitors.createdAt',
        operator: 'inDateRange',
        values: ['2016-12-30', '2017-01-05']
      }],
      order: [{
        id: 'visitors.checkinsTotal'
      }],
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(JSON.stringify(preAggregationsDescription, null, 2));

    expect(query.preAggregations?.preAggregationForQuery?.canUsePreAggregation).toEqual(true);
    const queries = dbRunner.tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [
          {
            visitors__source: 'google',
            visitors__checkins_total: '1'
          },
          {
            visitors__source: 'some',
            visitors__checkins_total: '5'
          }
        ]
      );
    });
  });

  it('partitioned hourly', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.checkinsTotal'
      ],
      dimensions: [
        'visitors.source'
      ],
      timezone: 'UTC',
      preAggregationsSchema: '',
      timeDimensions: [{
        dimension: 'visitors.createdAt',
        granularity: 'hour',
        dateRange: ['2017-01-02', '2017-01-05']
      }],
      order: [{
        id: 'visitors.createdAt'
      }],
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);

    expect(query.preAggregations?.preAggregationForQuery?.canUsePreAggregation).toEqual(true);
    const queries = dbRunner.tempTablePreAggregations(preAggregationsDescription);

    expect(queries.filter(([q]) => !!q.match(/3600/)).length).toBeGreaterThanOrEqual(1);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [
          {
            visitors__source: 'some',
            visitors__created_at_hour: '2017-01-03T00:00:00.000Z',
            visitors__checkins_total: '3'
          },
          {
            visitors__source: 'some',
            visitors__created_at_hour: '2017-01-05T00:00:00.000Z',
            visitors__checkins_total: '2'
          }
        ]
      );
    });
  });

  it('partitioned rolling', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.checkinsRollingTotal',
      ],
      dimensions: [
        'visitors.source'
      ],
      timezone: 'UTC',
      preAggregationsSchema: '',
      timeDimensions: [{
        dimension: 'visitors.createdAt',
        granularity: 'day',
        dateRange: ['2017-01-02', '2017-01-05']
      }],
      order: [{
        id: 'visitors.createdAt'
      }, {
        id: 'visitors.source'
      }],
      cubestoreSupportMultistage: getEnv('nativeSqlPlanner')
    });

    const queryAndParams = query.buildSqlAndParams();
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);
    expect(query.preAggregations?.preAggregationForQuery?.canUsePreAggregation).toEqual(true);

    const queries = dbRunner.tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [
          {
            visitors__source: null,
            visitors__created_at_day: '2017-01-02T00:00:00.000Z',
            visitors__checkins_rolling_total: '0'
          },
          {
            visitors__source: 'some',
            visitors__created_at_day: '2017-01-03T00:00:00.000Z',
            visitors__checkins_rolling_total: '3'
          },
          {
            visitors__source: null,
            visitors__created_at_day: '2017-01-03T00:00:00.000Z',
            visitors__checkins_rolling_total: '0'
          },
          {
            visitors__source: 'some',
            visitors__created_at_day: '2017-01-04T00:00:00.000Z',
            visitors__checkins_rolling_total: '3'
          },
          {
            visitors__source: null,
            visitors__created_at_day: '2017-01-04T00:00:00.000Z',
            visitors__checkins_rolling_total: '0'
          },
          {
            visitors__source: 'some',
            visitors__created_at_day: '2017-01-05T00:00:00.000Z',
            visitors__checkins_rolling_total: '5'
          },
          {
            visitors__source: null,
            visitors__created_at_day: '2017-01-05T00:00:00.000Z',
            visitors__checkins_rolling_total: '0'
          },
        ]
      );
    });
  });

  it('partitioned rolling 2 day', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.checkinsRolling2day',
      ],
      dimensions: [
        'visitors.source'
      ],
      timezone: 'UTC',
      preAggregationsSchema: '',
      timeDimensions: [{
        dimension: 'visitors.createdAt',
        granularity: 'day',
        dateRange: ['2017-01-02', '2017-01-05']
      }],
      order: [{
        id: 'visitors.createdAt'
      }, {
        id: 'visitors.source'
      }],
      cubestoreSupportMultistage: getEnv('nativeSqlPlanner')
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);
    expect(query.preAggregations?.preAggregationForQuery?.canUsePreAggregation).toEqual(true);

    const queries = dbRunner.tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [
          {
            visitors__source: null,
            visitors__created_at_day: '2017-01-02T00:00:00.000Z',
            visitors__checkins_rolling2day: null
          },
          {
            visitors__source: 'some',
            visitors__created_at_day: '2017-01-03T00:00:00.000Z',
            visitors__checkins_rolling2day: '3'
          },
          {
            visitors__source: 'some',
            visitors__created_at_day: '2017-01-04T00:00:00.000Z',
            visitors__checkins_rolling2day: '3'
          },
          {
            visitors__source: 'some',
            visitors__created_at_day: '2017-01-05T00:00:00.000Z',
            visitors__checkins_rolling2day: '2'
          },
        ]
      );
    });
  });

  it('not aligned time dimension', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.checkinsTotal'
      ],
      dimensions: [
        'visitors.source'
      ],
      timezone: 'UTC',
      preAggregationsSchema: '',
      timeDimensions: [{
        dimension: 'visitors.createdAt',
        granularity: 'hour',
        dateRange: ['2017-01-02T00:00:00.000', '2017-01-05T00:15:59.999']
      }],
      order: [{
        id: 'visitors.createdAt'
      }],
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);
    expect(preAggregationsDescription.length).toEqual(2);

    const queries = dbRunner.tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [
          {
            visitors__source: 'some',
            visitors__created_at_hour: '2017-01-03T00:00:00.000Z',
            visitors__checkins_total: '3'
          },
          {
            visitors__source: 'some',
            visitors__created_at_hour: '2017-01-05T00:00:00.000Z',
            visitors__checkins_total: '2'
          }
        ]
      );
    });
  });

  it('segment', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.checkinsTotal'
      ],
      dimensions: [],
      segments: ['visitors.google'],
      timezone: 'America/Los_Angeles',
      preAggregationsSchema: '',
      timeDimensions: [{
        dimension: 'visitors.createdAt',
        granularity: 'week',
        dateRange: ['2016-12-26', '2017-01-08']
      }],
      order: [{
        id: 'visitors.createdAt'
      }],
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);

    const queries = dbRunner.tempTablePreAggregations(preAggregationsDescription);

    expect(queries.filter(([q]) => !!q.match(/3600/)).length).toBeGreaterThanOrEqual(1);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [
          {
            visitors__created_at_week: '2017-01-02T00:00:00.000Z',
            visitors__checkins_total: '1'
          }
        ]
      );
    });
  });

  if (getEnv('nativeSqlPlanner') && getEnv('nativeSqlPlannerPreAggregations')) {
    it('rollup lambda', async () => {
      await compiler.compile();

      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'visitor_checkins.count',
        ],
        dimensions: ['visitor_checkins.visitor_id'],
        timeDimensions: [{
          dimension: 'visitor_checkins.created_at',
          granularity: 'day',
          dateRange: ['2016-12-26', '2017-01-08']
        }],
        timezone: 'America/Los_Angeles',
        preAggregationsSchema: '',
        order: [{
          id: 'visitor_checkins.visitor_id',
        }],
      });

      const queryAndParams = query.buildSqlAndParams();
      console.log(queryAndParams);
      const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
      console.log(preAggregationsDescription);

      console.log(query.preAggregations?.rollupMatchResultDescriptions());

      const queries = dbRunner.tempTablePreAggregations(preAggregationsDescription);

      console.log(JSON.stringify(queries.concat(queryAndParams)));

      return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
        console.log(JSON.stringify(res));
        expect(res).toEqual(
          [
            {
              vc__visitor_id: 1,
              vc__created_at_day: '2017-01-02T00:00:00.000Z',
              vc__count: '2'
            },
            {
              vc__visitor_id: 1,
              vc__created_at_day: '2017-01-03T00:00:00.000Z',
              vc__count: '2'
            },
            {
              vc__visitor_id: 1,
              vc__created_at_day: '2017-01-04T00:00:00.000Z',
              vc__count: '2'
            },
            {
              vc__visitor_id: 2,
              vc__created_at_day: '2017-01-04T00:00:00.000Z',
              vc__count: '4'
            },
            {
              vc__visitor_id: 3,
              vc__created_at_day: '2017-01-05T00:00:00.000Z',
              vc__count: '2'
            }
          ]
        );
      });
    });
  } else {
    it.skip('rollup lambda: baseQuery generate wrong sql for not external pre-aggregations', async () => {
      // This should be fixed in Tesseract.

    });
  }

  it('rollup join', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitor_checkins.count',
      ],
      dimensions: ['visitors.source'],
      preAggregationsSchema: '',
      order: [{
        id: 'visitors.source',
      }],
      timezone: 'UTC',
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);

    expect(queryAndParams[0]).toContain('visitors_for_join');
    expect(queryAndParams[0]).toContain('vc_for_join');

    console.log(query.preAggregations?.rollupMatchResultDescriptions());

    const queries = dbRunner.tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [
          { visitors__source: 'google', vc__count: '1' },
          { visitors__source: 'some', vc__count: '5' },
          { visitors__source: null, vc__count: null },
        ],
      );
    });
  });

  it('rollup join existing joins', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitor_checkins.count',
      ],
      dimensions: ['visitors.source', 'cards.visitorId'],
      preAggregationsSchema: '',
      order: [{
        id: 'visitors.source',
      }, {
        id: 'cards.visitorId',
      }],
      timezone: 'UTC',
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);

    expect(queryAndParams[0]).toContain('visitors_for_join_inc_cards');
    expect(queryAndParams[0]).toContain('vc_for_join');

    console.log(query.preAggregations?.rollupMatchResultDescriptions());

    const queries = dbRunner.tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [
          { visitors__source: 'google', cards__visitor_id: 3, vc__count: '1' },
          { visitors__source: 'some', cards__visitor_id: 1, vc__count: '3' },
          { visitors__source: 'some', cards__visitor_id: null, vc__count: '2' },
          { visitors__source: null, cards__visitor_id: null, vc__count: null },
        ],
      );
    });
  });

  it('rollup join partitioned', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitor_checkins.count',
      ],
      dimensions: ['visitors.source'],
      timezone: 'America/Los_Angeles',
      preAggregationsSchema: '',
      timeDimensions: [{
        dimension: 'visitors.createdAt',
        granularity: 'hour',
        dateRange: ['2017-01-03', '2017-01-04']
      }],
      order: [{
        id: 'visitors.createdAt'
      }],
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);

    expect(queryAndParams[0]).toContain('visitors_partitioned_hourly_for_join');
    expect(queryAndParams[0]).toContain('vc_for_join');

    console.log(query.preAggregations?.rollupMatchResultDescriptions());

    const queries = dbRunner.tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [
          {
            visitors__source: 'some',
            visitors__created_at_hour: '2017-01-04T16:00:00.000Z',
            vc__count: '2'
          }
        ],
      );
    });
  });

  it('partitioned without time', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.checkinsTotal'
      ],
      dimensions: [
        'visitors.source'
      ],
      timezone: 'America/Los_Angeles',
      preAggregationsSchema: '',
      timeDimensions: [],
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);

    const queries = dbRunner.tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [
          { visitors__source: 'some', visitors__checkins_total: '5' },
          { visitors__source: 'google', visitors__checkins_total: '1' },
          { visitors__source: null, visitors__checkins_total: '0' }
        ]
      );
    });
  });

  it('partitioned huge span', async () => {
    await compiler.compile();

    let queryAndParams;
    let preAggregationsDescription;
    let query;

    for (let i = 0; i < 10; i++) {
      query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'visitors.checkinsTotal'
        ],
        dimensions: [
          'visitors.source'
        ],
        timezone: 'UTC',
        preAggregationsSchema: '',
        timeDimensions: [{
          dimension: 'visitors.createdAt',
          granularity: 'day',
          dateRange: ['2000-12-30', '2017-01-06']
        }],
        order: [{
          id: 'visitors.createdAt'
        }],
      });
      queryAndParams = query.buildSqlAndParams();
      preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    }

    console.log(queryAndParams);
    console.log(preAggregationsDescription);

    const queries = dbRunner.tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [
          {
            visitors__source: null,
            visitors__created_at_day: '2016-09-07T00:00:00.000Z',
            visitors__checkins_total: '0'
          },
          {
            visitors__source: 'some',
            visitors__created_at_day: '2017-01-03T00:00:00.000Z',
            visitors__checkins_total: '3'
          },
          {
            visitors__source: 'some',
            visitors__created_at_day: '2017-01-05T00:00:00.000Z',
            visitors__checkins_total: '2'
          },
          {
            visitors__source: 'google',
            visitors__created_at_day: '2017-01-06T00:00:00.000Z',
            visitors__checkins_total: '1'
          }
        ]
      );
    });
  });

  it('simple view', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors_view.checkinsTotal'
      ],
      dimensions: [
        'visitors_view.source'
      ],
      timezone: 'America/Los_Angeles',
      preAggregationsSchema: '',
      timeDimensions: [{
        dimension: 'visitors_view.createdAt',
        granularity: 'day',
        dateRange: ['2016-12-30', '2017-01-05']
      }],
      order: [{
        id: 'visitors_view.createdAt'
      }],
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(JSON.stringify(preAggregationsDescription, null, 2));

    expect((<any>preAggregationsDescription)[0].loadSql[0]).toMatch(/visitors_partitioned/);

    const queries = dbRunner.tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [
          {
            visitors_view__source: 'some',
            visitors_view__created_at_day: '2017-01-02T00:00:00.000Z',
            visitors_view__checkins_total: '3'
          },
          {
            visitors_view__source: 'some',
            visitors_view__created_at_day: '2017-01-04T00:00:00.000Z',
            visitors_view__checkins_total: '2'
          },
          {
            visitors_view__source: 'google',
            visitors_view__created_at_day: '2017-01-05T00:00:00.000Z',
            visitors_view__checkins_total: '1'
          }
        ]
      );
    });
  });

  it('simple view non matching time-dimension granularity', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors_view.checkinsTotal'
      ],
      dimensions: [
        'visitors_view.source'
      ],
      timezone: 'America/Los_Angeles',
      preAggregationsSchema: '',
      timeDimensions: [{
        dimension: 'visitors_view.createdAt',
        granularity: 'month',
        dateRange: ['2016-12-30', '2017-01-05']
      }],
      order: [{
        id: 'visitors_view.createdAt'
      }],
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(JSON.stringify(preAggregationsDescription, null, 2));

    expect((<any>preAggregationsDescription)[0].loadSql[0]).toMatch(/visitors_partitioned/);

    const queries = dbRunner.tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [
          {
            visitors_view__source: 'google',
            visitors_view__created_at_month: '2017-01-01T00:00:00.000Z',
            visitors_view__checkins_total: '1'
          },
          {
            visitors_view__source: 'some',
            visitors_view__created_at_month: '2017-01-01T00:00:00.000Z',
            visitors_view__checkins_total: '5'
          }
        ]
      );
    });
  });

  it('lambda cross data source refresh key and ungrouped', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'LambdaVisitors.count'
      ],
      dimensions: [
        'LambdaVisitors.source'
      ],
      timeDimensions: [{
        dimension: 'LambdaVisitors.createdAt',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-25']
      }],
      timezone: 'America/Los_Angeles',
      order: [{
        id: 'LambdaVisitors.createdAt'
      }],
      preAggregationsSchema: '',
      queryFactory: {
        createQuery: (cube, compilers, options) => {
          if (cube === 'RealTimeLambdaVisitors') {
            // eslint-disable-next-line global-require
            const { KsqlQuery } = require('../../../../../cubejs-ksql-driver');
            return new KsqlQuery(compilers, options);
          } else {
            return new PostgresQuery(compilers, options);
          }
        }
      }
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    console.log(JSON.stringify(preAggregationsDescription, null, 2));
    const { loadSql } = preAggregationsDescription.find(p => p.preAggregationId === 'RealTimeLambdaVisitors.partitioned');

    expect(loadSql[0]).not.toMatch(/GROUP BY/);
    expect(loadSql[0]).toMatch(/THEN 1 END `real_time_lambda_visitors__count`/);
  });

  it('rollupJoin pre-aggregation', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      dimensions: ['cube_1.dim_1', 'cube_2.dim_2'],
      timezone: 'America/Los_Angeles',
      preAggregationsSchema: ''
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);
    expect(preAggregationsDescription.length).toBe(2);
    const aaa = preAggregationsDescription.find(p => p.preAggregationId === 'cube_1.aaa');
    const bbb = preAggregationsDescription.find(p => p.preAggregationId === 'cube_2.bbb');
    expect(aaa).toBeDefined();
    expect(bbb).toBeDefined();

    expect(query.preAggregations?.preAggregationForQuery?.canUsePreAggregation).toEqual(true);
    expect(query.preAggregations?.preAggregationForQuery?.preAggregationName).toEqual('rollupJoin');

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      expect(res).toEqual(
        [{
          cube_1__dim_1: 'dim_1',
          cube_2__dim_2: 'dim_2',
        }]
      );
    });
  });

  it('rollupJoin pre-aggregation with three cubes', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      dimensions: ['cube_x.dim_x', 'cube_y.dim_y', 'cube_z.dim_z'],
      timezone: 'America/Los_Angeles',
      preAggregationsSchema: ''
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);
    expect(preAggregationsDescription.length).toBe(3);
    const xxx = preAggregationsDescription.find(p => p.preAggregationId === 'cube_x.xxx');
    const yyy = preAggregationsDescription.find(p => p.preAggregationId === 'cube_y.yyy');
    const zzz = preAggregationsDescription.find(p => p.preAggregationId === 'cube_z.zzz');
    expect(xxx).toBeDefined();
    expect(yyy).toBeDefined();
    expect(zzz).toBeDefined();

    expect(query.preAggregations?.preAggregationForQuery?.canUsePreAggregation).toEqual(true);
    expect(query.preAggregations?.preAggregationForQuery?.preAggregationName).toEqual('rollupJoinThreeCubes');

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      expect(res).toEqual(
        [{
          cube_x__dim_x: 'dim_x',
          cube_y__dim_y: 'dim_y',
          cube_z__dim_z: 'dim_z',
        }]
      );
    });
  });

  it('rollupJoin pre-aggregation with nested joins via view (A->B->C)', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      dimensions: ['view_abc.dim_a', 'view_abc.dim_b', 'view_abc.dim_c'],
      timezone: 'America/Los_Angeles',
      preAggregationsSchema: ''
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);
    expect(preAggregationsDescription.length).toBe(2);
    const aaa = preAggregationsDescription.find(p => p.preAggregationId === 'cube_a.aaa_rollup');
    const bbb = preAggregationsDescription.find(p => p.preAggregationId === 'cube_b.bbb_rollup');
    expect(aaa).toBeDefined();
    expect(bbb).toBeDefined();

    expect(query.preAggregations?.preAggregationForQuery?.canUsePreAggregation).toEqual(true);
    expect(query.preAggregations?.preAggregationForQuery?.preAggregationName).toEqual('rollupJoinAB');

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      expect(res).toEqual(
        [{
          view_abc__dim_a: 'dim_a',
          view_abc__dim_b: 'dim_b',
          view_abc__dim_c: 'dim_c',
        }]
      );
    });
  });

  if (getEnv('nativeSqlPlanner')) {
    it.skip('FIXME(tesseract): rollupJoin pre-aggregation with nested joins via cube (A->B->C)', () => {
      // Need to investigate tesseract internals of how pre-aggs members are resolved and how
      // rollups are used to construct rollupJoins.
    });
  } else {
    it('rollupJoin pre-aggregation with nested joins via cube (A->B->C)', async () => {
      await compiler.compile();

      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        dimensions: ['cube_a.dim_a', 'cube_b.dim_b', 'cube_c.dim_c'],
        timezone: 'America/Los_Angeles',
        preAggregationsSchema: ''
      });

      const queryAndParams = query.buildSqlAndParams();
      console.log(queryAndParams);
      const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
      console.log(preAggregationsDescription);
      expect(preAggregationsDescription.length).toBe(0);

      expect(query.preAggregations?.preAggregationForQuery).toBeUndefined();

      return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
        expect(res).toEqual(
          [{
            cube_a__dim_a: 'dim_a',
            cube_b__dim_b: 'dim_b',
            cube_c__dim_c: 'dim_c',
          }]
        );
      });
    });
  }

  it('rollupJoin pre-aggregation matching with transitive joins', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      dimensions: [
        'test_facts.merchant_sk',
        'test_facts.product_sk',
        'merchant_and_product_dims.status',
        'other_facts.fact'
      ],
      timezone: 'America/Los_Angeles',
      preAggregationsSchema: ''
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    console.log(JSON.stringify(preAggregationsDescription, null, 2));

    // Verify that both rollups are included in the description
    expect(preAggregationsDescription.length).toBe(2);
    const factsRollup = preAggregationsDescription.find(p => p.preAggregationId === 'test_facts.facts_rollup');
    const bridgeRollup = preAggregationsDescription.find(p => p.preAggregationId === 'other_facts.bridge_rollup');
    expect(factsRollup).toBeDefined();
    expect(bridgeRollup).toBeDefined();

    // Verify that the rollupJoin pre-aggregation can be used for the query
    expect(query.preAggregations?.preAggregationForQuery?.canUsePreAggregation).toEqual(true);
    expect(query.preAggregations?.preAggregationForQuery?.preAggregationName).toEqual('rollupJoinTransitive');

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      expect(res).toEqual([
        {
          merchant_and_product_dims__status: 'SOLD',
          other_facts__fact: 'OF1',
          test_facts__merchant_sk: 101,
          test_facts__product_sk: 201,
        },
        {
          merchant_and_product_dims__status: 'PAID',
          other_facts__fact: 'OF2',
          test_facts__merchant_sk: 101,
          test_facts__product_sk: 202,
        },
        {
          merchant_and_product_dims__status: 'RETURNED',
          other_facts__fact: 'OF3',
          test_facts__merchant_sk: 102,
          test_facts__product_sk: 201,
        },
      ]);
    });
  });

  if (getEnv('nativeSqlPlanner')) {
    it.skip('FIXME(tesseract): rollupJoin pre-aggregation with not-full paths should fail', () => {
      // Need to investigate tesseract internals of how pre-aggs members are resolved and how
      // rollups are used to construct rollupJoins.
    });
  } else {
    it('rollupJoin pre-aggregation with not-full paths should fail', async () => {
      await compiler.compile();

      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        dimensions: ['cube_a_to_fail_pre_agg.dim_a', 'cube_b.dim_b', 'cube_c.dim_c'],
        timezone: 'America/Los_Angeles',
        preAggregationsSchema: ''
      });

      expect(() => query.buildSqlAndParams()).toThrow('No rollups found that can be used for a rollup join');
    });
  }
});
