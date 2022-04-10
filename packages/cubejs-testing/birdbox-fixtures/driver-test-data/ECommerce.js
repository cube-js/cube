import { sql } from './ECommerce.sql';
import { Products } from './Products';
import { Customers } from './Customers';

cube(`ECommerce`, {
  sql: sql('_type_'),
  extends: Products,
  preAggregations: {
    productAnalys: {
      dimensions: [
        CUBE.productName,
      ],
      measures: [
        CUBE.totalQuantity,
        CUBE.avgDiscount,
        CUBE.totalSales,
        CUBE.totalProfit,
      ],
      indexes: {
        productIndex: {
          columns: [
            CUBE.productName,
          ],
        },
      },
      refreshKey: {
        every: `1 hour`,
      },
    },
  },
  joins: {
    Customers: {
      relationship: 'belongsTo',
      sql: `${CUBE}.customer_id = ${Customers}.customer_id`,
    },
  },
  measures: {
    count: {
      type: `count`,
    },
    totalQuantity: {
      sql: 'quantity',
      type: 'sum',
    },
    avgDiscount: {
      sql: 'discount',
      type: 'avg',
    },
    totalSales: {
      sql: 'sales',
      type: 'sum',
    },
    totalProfit: {
      sql: 'profit',
      type: 'sum',
    },
  },
  dimensions: {
    rowId: {
      sql: 'row_id',
      type: 'number',
      primaryKey: true,
    },
    orderId: {
      sql: 'order_id',
      type: 'string',
    },
    orderDate: {
      sql: 'order_date',
      type: 'time',
    },
    customerId: {
      sql: 'customer_id',
      type: 'string',
    },
    customerName: {
      sql: `${Customers.customerName}`,
      type: 'string',
    },
    city: {
      sql: 'city',
      type: 'string',
    },
    sales: {
      sql: 'sales',
      type: 'number',
    },
    quantity: {
      sql: 'quantity',
      type: 'number',
    },
    discount: {
      sql: 'discount',
      type: 'number',
    },
    profit: {
      sql: 'profit',
      type: 'number',
    },
  },
});
