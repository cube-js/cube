cube(`Orders`, {
  sql: `SELECT * FROM public.orders`,
  
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
      type: `time`,
    },

    createdAtConverted: {
      sql: SQL_UTILS.convertTz(`created_at`),
      type: `time`,
    },
  },

  preAggregations: {
    main: {
      dimensions: [ Orders.status, Orders.createdAt, Orders.createdAtConverted ],
      timeDimension: Orders.createdAt,
      granularity: "day"
    }
  },
});