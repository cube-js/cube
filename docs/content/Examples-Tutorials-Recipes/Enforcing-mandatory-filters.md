---
title: Enforcing Mandatory Filters in a Query
permalink: /recipes/enforcing-mandatory-filters
category: Examples & Tutorials
subCategory: queries
menuOrder: 1
---

## Use case

Let's imagine that on New Year's Eve, December 30th, 2019, we renamed our store,
changed the design, and started selling completely different products. At the
same time, we decided to reuse the database for the new store. So, we'd like to only show
orders and users created after December 30th, 2019. In the recipe below,
we'll learn how to add mandatory filters to all queries.

## Configuration

To enforce mandatory filters we'll use the
[`queryRewrite`](https://cube.dev/docs/security/context#using-query-rewrite)
parameter in the `cube.js` configuration file.

To solve this, first we need to get all measures and
dimensions from a query. Next, we add filters for the
measures and dimensions. This will make sure we only show users and orders created after December 30th, 2019.

```javascript
module.exports = {
  queryRewrite: (query) => {
    const dimensions = [
      ...new Set(
        Array.from(query.dimensions, (element) => element.split('.')[0])
      ),
    ];
    const measures = [
      ...new Set(
        Array.from(query.measures, (element) => element.split('.')[0])
      ),
    ];
    const filterItems = dimensions.concat(measures);

    filterItems.forEach((item) =>
      query.filters.push({
        member: `${item}.createdAt`,
        operator: 'afterDate',
        values: ['2019-12-30'],
      })
    );

    return query;
  },
};
```

## Query

To get the users and orders we will send two queries without filters:

```bash
// Users
curl cube:4000/cubejs-api/v1/load \
-G -s --data-urlencode "query={"measures": [], "order": [["Users.createdAt", "asc"]], "dimensions": ["Users.firstName", "Users.lastName", "Users.createdAt"], "limit": 5}"
```

```bash
// Orders
curl cube:4000/cubejs-api/v1/load \
-G -s --data-urlencode "query={"measures": [], "order": [["Orders.createdAt", "asc"]], "dimensions": ["Orders.status", "Orders.createdAt"], "limit": 5}"
```

## Result

We have received users and orders created after December 30th, 2019:

```javascript
// Manager
[
  {
    'Users.firstName': 'Adonis',
    'Users.lastName': 'Labadie',
    'Users.createdAt': '2019-12-30T14:21:42.000',
  },
  {
    'Users.firstName': 'Keegan',
    'Users.lastName': 'Hane',
    'Users.createdAt': '2019-12-30T14:21:42.000',
  },
  {
    'Users.firstName': 'Enrique',
    'Users.lastName': 'Gerhold',
    'Users.createdAt': '2020-01-01T04:30:57.000',
  },
  {
    'Users.firstName': 'Leonor',
    'Users.lastName': 'Rolfson',
    'Users.createdAt': '2020-01-01T04:30:57.000',
  },
];
```

```javascript
// Orders
[
  {
    'Orders.status': 'shipped',
    'Orders.createdAt': '2019-12-31T00:00:00.000',
  },
  {
    'Orders.status': 'processing',
    'Orders.createdAt': '2019-12-31T00:00:00.000',
  },
  {
    'Orders.status': 'shipped',
    'Orders.createdAt': '2020-01-01T00:00:00.000',
  },
  {
    'Orders.status': 'processing',
    'Orders.createdAt': '2020-01-01T00:00:00.000',
  },
  {
    'Orders.status': 'completed',
    'Orders.createdAt': '2020-01-01T00:00:00.000',
  },
];
```

## Source code

Please feel free to check out the
[full source code](https://github.com/cube-js/cube.js/tree/master/examples/recipes/mandatory-filters)
or run it with the `docker-compose up` command. You'll see the result, including
queried data, in the console.
