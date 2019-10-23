cube(`LineItems`, {
  sql: `select * from public.line_items`,
  title: "Sold Items",

  joins: {
    Orders: {
      relationship: `belongsTo`,
      sql: `${Orders}.id = ${LineItems}.order_id`
    }
  },

  measures: {
    count: {
      sql: `id`,
      type: `count`
    },

    totalAmount: {
      sql: `price`,
      type: `runningTotal`,
      format: `currency`,
    },

    cumulativeTotalRevenue: {
      sql: `price`,
      type: `runningTotal`,
      format: `currency`,
    }
  },

  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true,
    },

    quantity: {
      sql: `quantity`,
      type: `number`
    },

    price: {
      sql: `price`,
      type: `number`,
      format: `currency`
    },

    createdAt: {
      sql: `created_at`,
      type: `time`
    }
  }
});
