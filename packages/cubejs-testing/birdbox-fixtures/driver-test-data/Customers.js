
import { sql } from './Customers.sql';

cube(`Customers`, {
  sql: sql('_type_'),
  measures: {
    count: {
      type: `count`,
    },
    runningTotal: {
      type: `count`,
      rollingWindow: {
        trailing: `unbounded`
      }
    },
  },

  dimensions: {
    customerId: {
      sql: 'customer_id',
      type: 'string',
      primaryKey: true,
      shown: true,
    },
    customerName: {
      sql: 'customer_name',
      type: 'string',
    },
  },

  preAggregations: {
    rolling: {
      measures: [
        CUBE.count,
        CUBE.runningTotal,
      ],
      refreshKey: {
        every: `1 hour`,
      },
      external: true
    },
  }
});
