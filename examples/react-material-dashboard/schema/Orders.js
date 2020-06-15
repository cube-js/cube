cube(`Orders`, {
  sql: `SELECT * FROM public.orders`,

  joins: {
    LineItems: {
      relationship: `belongsTo`,
      sql: `${Orders}.id = ${LineItems}.order_id`
    }
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

    completedCount: {
      sql: `id`,
      type: `count`,
      filters: [
        { sql: `${CUBE}.status = 'completed'` }
      ]
    },
    //
    percentOfCompletedOrders: {
      sql: `${completedCount}*100.0/${count}`,
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
    },

    user_id: {
      sql: `user_id`,
      type: `number`
    },

    number_of_order: {
      sql: `number`,
      type: `number`
    },

    product_id: {
      sql: `product_id`,
      type: `number`
    },

    order_id: {
      sql: `id`,
      type: `number`
    }
  }
});
