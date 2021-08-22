cube(`Orders`, {
  sql: `SELECT * FROM public.orders`,
  
  dimensions: {
    status: {
      sql: `status`,
      type: `string`
    },

    completedAt: {
      sql: `completed_at`,
      type: `time`
    },
  },

  preAggregations: {
    statuses: {
      dimensions: [
        CUBE.status
      ],
      refreshKey: {
        every: `1 day`
      }
    }
  }
});
