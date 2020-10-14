cube(`AllSales`, {
  sql: `
  select id, created_at, 'OrdersOffline' row_type from ${OrdersOffline.sql()}
  UNION ALL
  select id, created_at, 'Orders' row_type from ${Orders.sql()}
  `,

  measures: {
    count: {
      sql: `id`,
      type: `count`,
    },

    onlineRevenue: {
      type: `count`,
      filters: [{ sql: `${CUBE}.row_type = 'Orders'` }],
    },

    offlineRevenue: {
      type: `count`,
      filters: [{ sql: `${CUBE}.row_type = 'OrdersOffline'` }],
    },

    onlineRevenuePercentage: {
      sql: `(${onlineRevenue} / NULLIF(${onlineRevenue} + ${offlineRevenue} + 0.0, 0))*100`,
      type: `number`,
    },

    offlineRevenuePercentage: {
      sql: `(${offlineRevenue} / NULLIF(${onlineRevenue} + ${offlineRevenue} + 0.0, 0))*100`,
      type: `number`,
    },

    commonPercentage: {
      sql: `${onlineRevenuePercentage} + ${offlineRevenuePercentage}`,
      type: `number`,
    },
  },

  dimensions: {
    createdAt: {
      sql: `created_at`,
      type: `time`,
    },

    revenueType: {
      sql: `row_type`,
      type: `string`,
    },
  },
});
