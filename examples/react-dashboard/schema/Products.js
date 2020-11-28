cube(`Products`, {
  sql: `select * from public.products`,
  
  joins: {
    ProductCategories: {
      relationship: `belongsTo`,
      sql: `${Products}.product_category_id = ${ProductCategories}.id`
    }
  },

  measures: {
    count: {
      sql: `count(*)`,
      type: `number`
    }
  },

  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true,
    },

    name: {
      sql: `name`,
      type: `string`
    }
  }
});
