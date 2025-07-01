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
      rollups: [ordersByCompletedAt, ordersByCompletedByDay, RealTimeOrders.AOrdersByCompletedByHour],
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
        sql: `SELECT DATE('2021-01-1')`,
      },
      buildRangeEnd: {
        sql: `SELECT DATE('2021-12-1')`,
      },
      refreshKey: {
        every: '1 day'
      },
    },

    ordersByCompletedByDay: {
      measures: [count],
      dimensions: [status],
      timeDimension: completedAt,
      granularity: `day`,
      partitionGranularity: `day`,
      buildRangeStart: {
        sql: `SELECT DATE('2021-12-1')`,
      },
      buildRangeEnd: {
        sql: `SELECT DATE('2021-12-31')`,
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
        sql: `SELECT DATE('2020-12-1')`,
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

cube(`RealTimeOrders`, {
  sql: `SELECT * FROM public.orders`,

  measures: {
    count: {
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

    completedAt: {
      sql: `completed_at`,
      type: `time`,
    },
  },

  preAggregations: {
    AOrdersByCompletedByHour: {
      measures: [count],
      dimensions: [status],
      timeDimension: completedAt,
      granularity: `day`,
      partitionGranularity: `hour`,
      buildRangeStart: {
        sql: `SELECT DATE('2021-12-29')`,
      },
      buildRangeEnd: {
        sql: `SELECT DATE('2022-01-01')`,
      },
      refreshKey: {
        every: '1 day'
      },
    },
  }
});
