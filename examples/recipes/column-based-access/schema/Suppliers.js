cube(`Suppliers`, {
  sql: `SELECT * FROM public.suppliers`,

  dimensions: {
    id: {
      primaryKey: true,
      sql: `id`,
      type: `string`
    },

    email: {
      sql: `email`,
      type: `string`
    }
  }
});
