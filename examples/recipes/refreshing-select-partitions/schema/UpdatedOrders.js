// const updateStatus = require('../fetch').updateStatus;

// updateStatus();

cube(`UpdatedOrders`, {
  sql: `SELECT * FROM public.orders`,

  dimensions: {
    number: {
      sql: `number`,
      type: `number`,
    },

    status: {
      sql: `status`,
      type: `string`,
    },

    createdAt: {
      sql: `created_at`,
      type: `time`,
    },

    updatedAt: {
      sql: `updated_at`,
      type: `time`,
    },
  },

  preAggregations: {
    orders: {
      measures: [],
      dimensions: [CUBE.number, CUBE.status, CUBE.createdAt, CUBE.updatedAt],
      timeDimension: CUBE.createdAt,
      granularity: `day`,
      partitionGranularity: `month`,
      refreshKey: {
        sql: `SELECT max(updated_at) FROM public.orders`,
        every: `1 second`,
      },
    },
  },
});
