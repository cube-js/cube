const {
  securityContext: { env },
} = COMPILE_CONTEXT;

cube(`Products`, {
  sql: `SELECT * FROM ${env}.Orders`,
  measures: {
    amount: {
      sql: `amount`,
      type: `sum`,
    },
  },
  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true,
    },
    clientName: {
      sql: `client_name`,
      type: `string`,
    },
    createdAt: {
      sql: `created_at`,
      type: `time`,
    },
  },

  preAggregations: {
    amountByClientName: {
      measures: [Products.amount],
      dimensions: [Products.clientName],
      timeDimension: Products.createdAt,
      granularity: `day`,
      refreshKey: {
        every: `1 minute`,
      },
    },
  },
});
