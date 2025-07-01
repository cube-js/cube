cube('validate_preaggs', {
  sql_table: 'public.orders',

  dimensions: {
    id: {
      sql: 'id',
      type: 'number',
      primary_key: true,
    },

    status: {
      sql: 'status',
      type: 'string',
    },

    created_at: {
      sql: 'created_at',
      type: 'time',
    },

    completed_at: {
      sql: 'completed_at',
      type: 'time',
    },
  },

  measures: {
    count: {
      type: 'count',
    },
  },

  hierarchies: {
    hello: {
      title: 'World',
      levels: [status],
    },
  },

  preAggregations: {
    autoRollupFail: {
      type: 'autoRollup',
      maxPreAggregations: 'string_instead_of_number',
    },

    originalSqlFail: {
      type: 'originalSql',
      partitionGranularity: 'invalid_partition_granularity',
    },

    originalSqlFail2: {
      type: 'originalSql',
      partitionGranularity: 'day',
      uniqueKeyColumns: 'not_an_array',
    },

    rollupJoinFail: {
      type: 'rollupJoin',
      partitionGranularity: 'day',
      // no rollups
    },

    rollupLambdaFail: {
      type: 'rollupLambda',
      partitionGranularity: 'day',
      granularity: 'day',
      time_dimension: created_at,
      rollups: not_a_func,
    },

    rollupFail: {
      type: 'rollup',
      measures: [CUBE.count],
      timeDimension: [CUBE.created_at],
      granularity: 'day',
      partitionGranularity: 'month',
    },

    rollupFail2: {
        type: 'rollup',
        granularity: `day`,
        partitionGranularity: `month`,
        dimensions: [CUBE.created_at],
        measures: [CUBE.count],
        timeDimensions: 'created_at',
    },

    // TODO: implement check for strings in dimensions/measures/timeDimension
    // rollupFail3: {
    //     type: 'rollup',
    //     granularity: `day`,
    //     partitionGranularity: `month`,
    //     dimensions: ['created_at'],
    //     measures: ['count'],
    //     timeDimension: 'created_at',
    // },
  },
});
