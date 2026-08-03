cube(`SlowQuery`, {
  sql: `SELECT pg_sleep(90), 1 as id`,

  measures: {
    count: {
      type: `count`,
    },
  },

  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true,
    },
  },
});
