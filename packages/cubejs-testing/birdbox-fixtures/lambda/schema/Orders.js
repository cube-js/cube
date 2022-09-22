cube(`Orders`, {
  sql: `SELECT * FROM public.orders`,

  preAggregations: {
    ordersByCompletedAtMonthLambda: {
      type: `rollupLambda`,
      rollups: [ordersByCompletedAtMonth],
      unionWithSourceData: true,
    },

    ordersByCompletedAtLambda: {
      type: `rollupLambda`,
      rollups: [ordersByCompletedAt],
      unionWithSourceData: true,
    },

    ordersByCompletedAtAndUserIdLambda: {
      type: `rollupLambda`,
      measures: [count],
      dimensions: [status, userId],
      timeDimension: completedAt,
      granularity: `day`,
      rollups: [ordersByCompletedAtAndUserId],
      unionWithSourceData: true,
    },

    ordersByCompletedAtMonth: {
      measures: [count],
      timeDimension: completedAt,
      granularity: `month`,
      partitionGranularity: `month`,
      buildRangeStart: {
        sql: `SELECT DATE('2020-02-7')`,
      },
      buildRangeEnd: {
        sql: `SELECT DATE('2020-05-7')`,
      },
      refreshKey: {
        every: '1 day'
      },
    },

    ordersByCompletedAt: {
      measures: [count],
      dimensions: [status],
      timeDimension: completedAt,
      granularity: `day`,
      partitionGranularity: `month`,
      buildRangeStart: {
        sql: `SELECT DATE('2020-02-7')`,
      },
      buildRangeEnd: {
        sql: `SELECT DATE('2020-05-7')`,
      },
      refreshKey: {
        every: '1 day'
      },
    },

    ordersByCompletedAtAndUserId: {
      measures: [count],
      dimensions: [status, userId],
      timeDimension: completedAt,
      granularity: `day`,
      partitionGranularity: `month`,
      buildRangeStart: {
        sql: `SELECT DATE('2020-02-7')`,
      },
      buildRangeEnd: {
        sql: `SELECT DATE('2020-05-7')`,
      },
      refreshKey: {
        every: '1 day'
      },
    },
  },

  refreshKey: {
    every: '1 second'
  },

  measures: {
    count: {
      type: `count`,
    },

    count2: {
      type: `count`,
    },
  },

  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true,
    },

    status: {
      sql: `status`,
      type: `string`,
    },

    userId: {
      sql: `user_id`,
      type: `number`,
    },

    completedAt: {
      sql: `completed_at`,
      type: `time`,
    },
  },
});
