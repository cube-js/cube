
import { sql } from './Customers.sql';

cube(`Customers`, {
  sql: sql('_type_'),
  dimensions: {
    customerId: {
      sql: 'customer_id',
      type: 'string',
    },
    customerName: {
      sql: 'customer_name',
      type: 'string',
    },
  },
});
