cube(`Orders`, {
  sql: `SELECT * FROM public.orders`,

  joins: {
    Users: {
      sql: `${CUBE}.user_id = ${Users}.id`,
      relationship: `belongsTo`
    },

    Products: {
      sql: `${CUBE}.product_id = ${Products}.id`,
      relationship: `belongsTo`
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
      primaryKey: true,
      shown: true
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

    size: {
      sql: `${LineItems.count}`,
      subQuery: true,
      type: 'number'
    },

    price: {
      sql: `${LineItems.price}`,
      subQuery: true,
      type: 'number'
    }
  }
});
