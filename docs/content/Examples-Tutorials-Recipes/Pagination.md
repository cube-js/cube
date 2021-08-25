---
title: Pagination
permalink: /recipes/pagination
category: Examples & Tutorials
subCategory: Queries
menuOrder: 2
---

## Use case

We want to display a table of data with hundreds of rows. To make the table
easier to digest and to improve the performance of the query, we'll use
pagination. With the recipe below, we'll get the orders list sorted by the order
number. Every page will have 5 orders.

## Data schema

We have the following data schema.

```javascript
cube(`Orders`, {
  sql: `SELECT * FROM public.orders`,

  measures: {
    count: {
      type: `count`,
    },
  },

  dimensions: {
    number: {
      sql: `number`,
      type: `number`,
    },

    createdAt: {
      sql: `created_at`,
      type: `time`,
    },
  },
});
```

## Query

To select orders that belong to a particular page, we can use the `limit` and
`offset` query properties. First, we get the number of all orders that we have.
Then we should set the `limit` and `offset` properties for the queries that will
get the orders from the Cube API.

```bash
// Get count of the orders
curl cube:4000/cubejs-api/v1/load \
-H "Authorization: eeyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoib3BlcmF0b3IiLCJpYXQiOjE2Mjg3NDUwNDUsImV4cCI6MTgwMTU0NTA0NX0.VErb2t7Bc43ryRwaOiEgXuU5KiolCT-69eI_i2pRq4o" \
"query={"measures": ["Orders.count"]}"
```

```bash
// Get first five orders
curl cube:4000/cubejs-api/v1/load \
-H "Authorization: eeyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoib3BlcmF0b3IiLCJpYXQiOjE2Mjg3NDUwNDUsImV4cCI6MTgwMTU0NTA0NX0.VErb2t7Bc43ryRwaOiEgXuU5KiolCT-69eI_i2pRq4o" \
"query={"order": [["Orders.number", "asc"]], "dimensions": ["Orders.number"], "limit": 5}"
```

```bash
// Get next five orders
curl cube:4000/cubejs-api/v1/load \
-H "Authorization: eeyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoib3BlcmF0b3IiLCJpYXQiOjE2Mjg3NDUwNDUsImV4cCI6MTgwMTU0NTA0NX0.VErb2t7Bc43ryRwaOiEgXuU5KiolCT-69eI_i2pRq4o" \
"query={"order": [["Orders.number", "asc"]], "dimensions": ["Orders.number"], "limit": 5, "offset": 5}"
```

## Result

We have received five orders per query and can use them as we want.

```javascript
// Orders count:

[
  {
    'Orders.count': '10000',
  },
];
```

```javascript
// The first five orders:

[
  {
    'Orders.number': 1,
  },
  {
    'Orders.number': 2,
  },
  {
    'Orders.number': 3,
  },
  {
    'Orders.number': 4,
  },
  {
    'Orders.number': 5,
  },
];
```

```javascript
// The next five orders:

[
  {
    'Orders.number': 6,
  },
  {
    'Orders.number': 7,
  },
  {
    'Orders.number': 8,
  },
  {
    'Orders.number': 9,
  },
  {
    'Orders.number': 10,
  },
];
```

## Source code

Please feel free to check out the
[full source code](https://github.com/cube-js/cube.js/tree/master/examples/recipes/pagination)
or run it with the `docker-compose up` command. You'll see the result, including
queried data, in the console.
