---
title: Data Blending
permalink: /data-blending
scope: cubejs
category: Guides
---

In case you want to plot two measures from different cubes on one chart or
create calculated measure based on it you need to create join between these two cubes.
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
