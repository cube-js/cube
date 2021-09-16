cube(`Products`, {
  sql: `SELECT * FROM public.Products WHERE MOD (id, 2) = 1`,
  
  measures: {
    count: {
      type: `count`,
    }
  },
  
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
