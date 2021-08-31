cube(`Users`, {
  sql: `SELECT * FROM public.users`,

  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true,
    },

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
