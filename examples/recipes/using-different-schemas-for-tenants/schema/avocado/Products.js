cube(`Products`, {
  sql: `SELECT * FROM public.Products WHERE MOD (id, 2) = 1`,
  
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
