---
title: Data Blending
permalink: /schema/advanced/data-blending
category: Data Schema
subCategory: Advanced
menuOrder: 3
redirect_from:
  - /data-blending
  - /recipes/data-blending
---

In case you want to plot two measures from different cubes on a single chart, or
create a calculated measure based on it, you need to create a join between these
two cubes. If there's no way to join two cubes other than by time dimension, you
need to use the data blending approach.

Data blending is a pattern that allows creating a cube based on two or more
existing cubes, and contains a union of the underlying cubes' date to query it
together.

For an example omnichannel store which has both online and offline sales, let's
calculate summary metrics for revenue, customer count, etc. We have
`RetailOrders` cube for offline sales:

```javascript
cube(`RetailOrders`, {
  sql: `SELECT * FROM retail_orders`,

  measures: {
    customerCount: {
      sql: `customer_id`,
      type: `countDistinct`,
    },

    revenue: {
      sql: `amount`,
      type: `sum`,
    },
  },

  dimensions: {
    createdAt: {
      sql: `created_at`,
      type: `time`,
    },
  },
});
```

An `OnlineOrders` cube for online sales:

```javascript
cube(`OnlineOrders`, {
  sql: `SELECT * FROM online_orders`,

  measures: {
    customerCount: {
      sql: `user_id`,
      type: `countDistinct`,
    },

    revenue: {
      sql: `amount`,
      type: `sum`,
    },
  },

  dimensions: {
    createdAt: {
      sql: `created_at`,
      type: `time`,
    },
  },
});
```

Given the above cubes, a data blending cube can be introduced as follows:

```javascript
cube(`AllSales`, {
  sql: `
SELECT
  amount,
  user_id AS customer_id,
  created_at,
  'OnlineOrders' AS row_type
FROM ${OnlineOrders.sql()}
UNION ALL
SELECT
  amount,
  customer_id,
  created_at,
  'Orders' AS row_type
FROM ${RetailOrders.sql()}
 `,

  measures: {
    customerCount: {
      sql: `customer_id`,
      type: `countDistinct`,
    },

    revenue: {
      sql: `amount`,
      type: `sum`,
    },

    onlineRevenue: {
      sql: `amount`,
      type: `sum`,
      filters: [{ sql: `${CUBE}.row_type = 'OnlineOrders'` }],
    },

    offlineRevenue: {
      sql: `amount`,
      type: `sum`,
      filters: [{ sql: `${CUBE}.row_type = 'RetailOrders'` }],
    },

    onlineRevenuePercentage: {
      sql: `${onlineRevenue} / NULLIF(${onlineRevenue} + ${offlineRevenue}, 0)`,
      type: `number`,
      format: `percent`,
    },
  },

  dimensions: {
    createdAt: {
      sql: `created_at`,
      type: `time`,
    },

    revenueType: {
      sql: `row_type`,
      type: `string`,
    },
  },
});
```

Another use case of the Data Blending approach would be when you want to chart
some measures (business related) together and see how they correlate.

Provided we have the aforementioned tables `OnlineOrders` and `RetailOrders`
let's assume that we want to chart those measures together and see how they
correlate. You can simply pass the queries to the Cube.js client, and it will
merge the results which will let you easily display it on the chart.

```javascript
import cubejs from '@cubejs-client/core';

const API_URL = 'http://localhost:4000';
const CUBEJS_TOKEN = 'YOUR_TOKEN';

const cubejsApi = cubejs(CUBEJS_TOKEN, {
  apiUrl: `${API_URL}/cubejs-api/v1`,
});

const queries = [
  {
    measures: ['OnlineOrders.revenue'],
    timeDimensions: [
      {
        dimension: 'OnlineOrders.createdAt',
        granularity: 'day',
        dateRange: ['2020-08-01', '2020-08-07'],
      },
    ],
  },
  {
    measures: ['RetailOrders.revenue'],
    timeDimensions: [
      {
        dimension: 'RetailOrders.createdAt',
        granularity: 'day',
        dateRange: ['2020-08-01', '2020-08-07'],
      },
    ],
  },
];

const resultSet = await cubejsApi.load(queries);
```
