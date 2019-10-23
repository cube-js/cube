cube(`ProductCategories`, {
  sql: `select * from public.product_categories`,

  joins: {
    Products: {
      relationship: `hasMany`,
      sql: `${Products}.category_id = ${ProductCategories}.id`
    }
  },

  measures: {
  },

  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true,
    },

    name: {
      sql: `${TABLE}.name`,
      type: `string`
    }
  }
});
