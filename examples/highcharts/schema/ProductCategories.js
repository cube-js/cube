cube(`ProductCategories`, {
  sql: `SELECT * FROM public.product_categories`,
  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true
    },
    
    name: {
      sql: `name`,
      type: `string`
    },
  }
});
