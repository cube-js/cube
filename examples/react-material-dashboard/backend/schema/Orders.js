cube(`Orders`, {
  sql: `SELECT * FROM public.orders`,

  joins: {

  },

  measures: {
    count: {
      type: `count`,
      drillMembers: [id, createdAt]
    },

    number: {
      sql: `number`,
      type: `sum`
    },

    ordersCount: {
      sql: `id`,
      type: `count`,
      shown: false
    },

    ordersCompletedCount: {
      sql: `id`,
      type: `count`,
      filters: [
        { sql: `${CUBE}.status = 'completed'` }
      ]
    },
    //
    percentOfCompletedOrders: {
      sql: `${ordersCompletedCount} * 100 / ${ordersCount}`,
      type: `number`,
      format: `percent`
    }
  },

  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true
    },

    status: {
      sql: `status`,
      type: `string`
    },

    createdAt: {
      sql: `created_at`,
      type: `time`
    },

    completedAt: {
      sql: `completed_at`,
      type: `time`
    }
  }
});
