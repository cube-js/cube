cube(`UsersStatuses_Joins`, {
  sql: `
    SELECT
      users.first_name,
      users.last_name,
      MIN(cOrders.created_at) AS cCreatedAt,
      MIN(pOrders.created_at) AS pCreatedAt,
      MIN(sOrders.created_at) AS sCreatedAt
    FROM public.users AS users
    LEFT JOIN public.orders AS cOrders
      ON users.id = cOrders.user_id AND cOrders.status = 'completed'
    LEFT JOIN public.orders AS pOrders
      ON users.id = pOrders.user_id AND pOrders.status = 'processing'
    LEFT JOIN public.orders AS sOrders
      ON users.id = sOrders.user_id AND sOrders.status = 'shipped'
    GROUP BY 1, 2
  `,
  
  dimensions: {
    name: {
      sql: `first_name || ' ' || last_name`,
      type: `string`
    },

    completedCreatedAt: {
      sql: `cCreatedAt`,
      type: `time`,
    },

    processingCreatedAt: {
      sql: `pCreatedAt`,
      type: `time`,
    },

    shippedCreatedAt: {
      sql: `sCreatedAt`,
      type: `time`,
    }
  }
});
