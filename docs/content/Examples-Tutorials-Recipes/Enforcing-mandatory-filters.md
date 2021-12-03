---
title: Enforcing Mandatory Filters
permalink: /recipes/enforcing-mandatory-filters
category: Examples & Tutorials
subCategory: Queries
menuOrder: 5
---

## Use case

Let's imagine that on New Year's Eve, December 30th, 2019, we renamed our store,
changed the design, and started selling completely different products. At the
same time, we decided to reuse the database for the new store. So, we'd like to
only show orders created after December 30th, 2019. In the recipe below, we'll
learn how to add mandatory filters to all queries.

## Configuration

To enforce mandatory filters we'll use the
[`queryRewrite`](https://cube.dev/docs/security/context#using-query-rewrite)
parameter in the `cube.js` configuration file.

To solve this, we add a filter that will apply to all queries. This will make
sure we only show orders created after December 30th, 2019.

```javascript
module.exports = {
  queryRewrite: (query) => {
    query.filters.push({
      member: `Orders.createdAt`,
      operator: 'afterDate',
      values: ['2019-12-30'],
    });

    return query;
  },
};
```

## Query

To get the orders we will send two queries with filters by status:

```bash
// Completed orders

curl cube:4000/cubejs-api/v1/load \
-H "Authorization: eeyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoib3BlcmF0b3IiLCJpYXQiOjE2Mjg3NDUwNDUsImV4cCI6MTgwMTU0NTA0NX0.VErb2t7Bc43ryRwaOiEgXuU5KiolCT-69eI_i2pRq4o" \
"query={"measures": [], "order": [["Users.createdAt", "asc"]], "dimensions": ["Orders.number", "Orders.createdAt"],
  "filters": [
    {
      "member": "Orders.status",
      "operator": "equals",
      "values": ["completed"]
    }
  ],
  "limit": 5}"
```

```bash
// Shipped orders

curl cube:4000/cubejs-api/v1/load \
-H "Authorization: eeyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoib3BlcmF0b3IiLCJpYXQiOjE2Mjg3NDUwNDUsImV4cCI6MTgwMTU0NTA0NX0.VErb2t7Bc43ryRwaOiEgXuU5KiolCT-69eI_i2pRq4o" \
"query={"measures": [], "order": [["Orders.createdAt", "asc"]], "dimensions": ["Orders.number", "Orders.createdAt"],
  "filters": [
    {
      "member": "Orders.status",
      "operator": "equals",
      "values": ["shipped"]
    }
  ],
  "limit": 5}"
```

## Result

We have received orders created after December 30th, 2019:

```javascript
// Completed orders

[
  {
    'Orders.number': 78,
    'Orders.createdAt': '2020-01-01T00:00:00.000',
  },
  {
    'Orders.number': 43,
    'Orders.createdAt': '2020-01-02T00:00:00.000',
  },
  {
    'Orders.number': 87,
    'Orders.createdAt': '2020-01-04T00:00:00.000',
  },
  {
    'Orders.number': 45,
    'Orders.createdAt': '2020-01-04T00:00:00.000',
  },
  {
    'Orders.number': 28,
    'Orders.createdAt': '2020-01-05T00:00:00.000',
  },
];
```

```javascript
// Shipped orders

[
  {
    'Orders.number': 57,
    'Orders.createdAt': '2019-12-31T00:00:00.000',
  },
  {
    'Orders.number': 38,
    'Orders.createdAt': '2020-01-01T00:00:00.000',
  },
  {
    'Orders.number': 10,
    'Orders.createdAt': '2020-01-02T00:00:00.000',
  },
  {
    'Orders.number': 19,
    'Orders.createdAt': '2020-01-02T00:00:00.000',
  },
  {
    'Orders.number': 15,
    'Orders.createdAt': '2020-01-02T00:00:00.000',
  },
];
```

## Source code

Please feel free to check out the
[full source code](https://github.com/cube-js/cube.js/tree/master/examples/recipes/mandatory-filters)
or run it with the `docker-compose up` command. You'll see the result, including
queried data, in the console.
