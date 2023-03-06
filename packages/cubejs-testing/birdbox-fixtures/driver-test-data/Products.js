import { sql } from './Products.sql';

cube(`Products`, {
  sql: sql('_type_'),
  dimensions: {
    category: {
      sql: 'category',
      type: 'string',
    },
    subCategory: {
      sql: 'sub_category',
      type: 'string',
    },
    productName: {
      sql: 'product_name',
      type: 'string',
    },
  },
});
