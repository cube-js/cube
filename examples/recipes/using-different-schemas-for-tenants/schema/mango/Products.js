cube(`Products`, {
  sql: `SELECT * FROM public.Products WHERE MOD (id, 2) = 0`,
  
  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true,
      shown: true
    },

    name: {
      sql: `name`,
      type: `string`
    }
  }
});
