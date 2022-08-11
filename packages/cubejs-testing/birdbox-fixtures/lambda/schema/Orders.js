cube(`Orders`, {
  sql: `SELECT * FROM public.orders`,

  preAggregations: {
    ordersByCompletedAt: {
      unionWithSourceData: true,
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
      unionWithSourceData: true,
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
