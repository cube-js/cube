cube(`Suppliers`, {
  sql: `SELECT * FROM public.suppliers`,

  dimensions: {
    email: {
      sql: `email`,
      type: `string`
    }
  }
});
