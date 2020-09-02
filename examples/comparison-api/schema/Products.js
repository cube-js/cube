cube(`Products`, {
  sql: `SELECT * FROM public.products`,
  
  joins: {
    ProductCategories: {
      sql: `${CUBE}.product_category_id = ${ProductCategories}.id`,
      relationship: `belongsTo`
    }
  },
  
  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true
    }
  }
});
