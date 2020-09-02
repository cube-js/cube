cube(`Users`, {
  sql: `SELECT * FROM public.users`,

  dimensions: {
    state: {
      sql: `state`,
      type: `string`
    },

    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true
    }
  }
});
