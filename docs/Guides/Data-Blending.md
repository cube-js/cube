---
title: Data Blending
permalink: /data-blending
scope: cubejs
category: Guides
subCategory: Tutorials
menuOrder: 24
---

In case you want to plot two measures from different cubes on one chart or
create a calculated measure based on it you need to create a join between these two cubes.
If there's no way to join two cubes other than by time dimension you need to use Data Blending approach.

Data Blending in Cube.js is a pattern that allows to create Data Blending cube based on two or more cubes.
This cube basically should contain union of underlying cubes in order to allow query this data together.

For example you have omnichannel shop and you have both online and offline sales which requires to calculate some summary metrics for revenue, customer count, etc.
In this case we have `Orders` cube for offline sales:

```javascript
cube(`Orders`, {
 sql: `select * from orders`,

 measures: {
   customerCount: {
     sql: `customer_id`,
     type: `countDistinct`
   },

   revenue: {
     sql: `amount`,
     type: `sum`
   }
 },

 dimensions: {
   createdAt: {
     sql: `created_at`,
     type: `time`
   }
 }
});
```

And `Transactions` cube for online sales

```javascript
cube(`Transactions`, {
 sql: `select * from transactions`,

 measures: {
   customerCount: {
     sql: `user_id`,
     type: `countDistinct`
   },

   revenue: {
     sql: `amount`,
     type: `sum`
   }
 },

 dimensions: {
   createdAt: {
     sql: `created_at`,
     type: `time`
   }
 }
});
```

Given that Data Blending cube can be introduced as simple as:

```javascript
cube(`AllSales`, {
 sql: `
 select amount, user_id as customer_id, created_at, 'Transactions' row_type from ${Transactions.sql()}
 UNION ALL
 select amount, customer_id, created_at, 'Orders' row_type from ${Orders.sql()}
 `,

 measures: {
   customerCount: {
     sql: `customer_id`,
     type: `countDistinct`
   },

   revenue: {
     sql: `amount`,
     type: `sum`
   },

   onlineRevenue: {
     sql: `amount`,
     type: `sum`,
     filters: [{ sql: `${CUBE}.row_type = 'Transactions'` }]
   },

   offlineRevenue: {
     sql: `amount`,
     type: `sum`,
     filters: [{ sql: `${CUBE}.row_type = 'Orders'` }]
   },

   onlineRevenuePercentage: {
     sql: `${onlineRevenue} / NULLIF(${onlineRevenue} + ${offlineRevenue}, 0)`,
     type: `number`,
     format: `percent`
   }
 },

 dimensions: {
   createdAt: {
     sql: `created_at`,
     type: `time`
   },

   revenueType: {
     sql: `row_type`,
     type: `string`
   }
 }
});
```

Another use case of the Data Blending approach would be when you want to chart some measures (business related) together and see how they correlate.

Provided we have the aforementioned tables `Transactions` and `Orders` let's assume that we want to chart those measures together and see how they correlate. You can simply pass the queries to the Cube.js client and it will merge the results which will let you easily display it on the chart.

```js
import cubejs from '@cubejs-client/core';

const API_URL = 'http://localhost:4000';
const CUBEJS_TOKEN = 'YOUR_TOKEN';

const cubejsApi = cubejs(CUBEJS_TOKEN, {
  apiUrl: `${API_URL}/cubejs-api/v1`
});

const queries = [
  {
    measures: ['Transactions.revenue'],
    timeDimensions: [
      {
        dimension: 'Transactions.createdAt',
        granularity: 'day',
        dateRange: ['2020-08-01', '2020-08-07']
      }
    ]
  },
  {
    measures: ['Orders.revenue'],
    timeDimensions: [
      {
        dimension: 'Orders.createdAt',
        granularity: 'day',
        dateRange: ['2020-08-01', '2020-08-07']
      }
    ]
  }
];

const resultSet = await cubejsApi.load(queries);
// ...
```
