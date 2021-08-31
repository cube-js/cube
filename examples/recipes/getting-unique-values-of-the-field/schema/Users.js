cube(`Users`, {
  sql: `SELECT * FROM public.users`,

  dimensions: {
    city: {
      sql: `city`,
      type: `string`,
    },

    state: {
      sql: `state`,
      type: `string`,
    },
  }
});
