import { sql } from './Customers.sql';

cube(`CommonCustomers`, {
  sql: sql('_type_'),
  dimensions: {
    customerId: {
      sql: 'customer_id',
      type: 'string',
    },
  },
});