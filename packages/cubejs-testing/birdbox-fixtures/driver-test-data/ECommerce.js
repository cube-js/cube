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
      refreshKey: {
        every: `1 hour`,
      },
    },
    manual: {
      external: false,
      scheduledRefresh: false,
      timeDimension: CUBE.orderDate,
      granularity: `month`,
      partitionGranularity: `month`,
      dimensions: [
        CUBE.productName,
      ],
      measures: [
        CUBE.totalQuantity,
        CUBE.avgDiscount,
        CUBE.totalSales,
        CUBE.totalProfit,
      ],
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
      meta: {
        foo: `bar`
      }
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
    hiddenSum: {
      sql: 'profit',
      type: 'sum',
      shown: false,
    },
  },
  dimensions: {
    rowId: {
      sql: 'row_id',
      type: 'number',
      primaryKey: true,
      shown: true,
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

view(`ECommerceView`, {
  cubes: [{
    joinPath: ECommerce,
    includes: `*`,
    excludes: [`orderDate`]
  }]
});
