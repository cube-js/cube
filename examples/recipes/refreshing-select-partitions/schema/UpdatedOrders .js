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
    updatedOrders: {
      type: `rollup`,
      external: true,
      dimensions: [CUBE.number, CUBE.status, CUBE.createdAt, CUBE.updatedAt],
      timeDimension: CUBE.createdAt,
      granularity: `day`,
      partitionGranularity: `month`,
      refreshKey: {
        sql: `SELECT max(updated_at) FROM public.orders WHERE ${FILTER_PARAMS.UpdatedOrders.createdAt.filter('created_at')}`
      },
    },
  },
});
