cube(`Statuses`, {
  sql: `
    SELECT product_id AS order_id, status, MIN(created_at) AS changed_at
    FROM public.orders
    GROUP BY 1, 2
  `,

  measures: {
    count: {
      sql: `order_id`,
      type: `countDistinct`
    },
  },
  
  dimensions: {
    orderId: {
      sql: `order_id`,
      type: `number`
    },

    status: {
      sql: `status`,
      type: `string`
    },
    
    changedAt: {
      sql: `changed_at`,
      type: `time`
    },
  }
});