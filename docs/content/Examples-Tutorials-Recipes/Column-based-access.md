---
title: Column-Based Access
permalink: /recipes/column-based-access
category: Examples & Tutorials
subCategory: Access control
menuOrder: 2
---

## Use case

We want to manage user access to different data depending on a database
relationship. In the recipe below, we will manage supplier access to their
products. A supplier can't see other supplier's products.

## Data schema

To implement column-based access, we will use supplier's email from a
[JSON Web Token](https://cube.dev/docs/security), and the
[`queryRewrite`](https://cube.dev/docs/security/context#using-query-rewrite)
extension point to manage data access.

We have `Products` and `Suppliers` cubes with a `hasOne` relationship from
products to suppliers:

```javascript
cube(`Products`, {
  sql: `SELECT * FROM public.products`,

  joins: {
    Suppliers: {
      relationship: `belongsTo`,
      sql: `${CUBE}.supplier_id = ${Suppliers}.id`,
    },
  },

  dimensions: {
    name: {
      sql: `name`,
      type: `string`,
    },
  },
});
```

```javascript
cube(`Suppliers`, {
  sql: `SELECT * FROM public.suppliers`,

  dimensions: {
    email: {
      sql: `email`,
      type: `string`,
    },
  },
});
```

## Configuration

Let's add the supplier email filter if a query includes any dimensions or
measures from the `Products` cube:

```javascript
module.exports = {
  queryRewrite: (query, { securityContext }) => {
    const cubeNames = [
      ...Array.from(query.measures, (e) => e.split('.')[0]),
      ...Array.from(query.dimensions, (e) => e.split('.')[0]),
    ];

    if (cubeNames.includes('Products')) {
      if (!securityContext.email) {
        throw new Error('No email found in Security Context!');
      }

      query.filters.push({
        member: `Suppliers.email`,
        operator: 'equals',
        values: [securityContext.email],
      });
    }

    return query;
  },
};
```

## Query

To get the supplier's products, we will send two identical requests with
different emails inside JWTs.

```javascript
{
  "iat": 1000000000,
  "exp": 5000000000,
  "email": "purus.accumsan@Proin.org"
}
```

```javascript
{
  "iat": 1000000000,
  "exp": 5000000000,
  "email": "gravida.sit.amet@risus.net"
}
```

## Result

We have received different data depending on the supplier's email.

```javascript
// purus.accumsan@Proin.org
[
  {
    'Products.name': 'Awesome Soft Salad',
  },
  {
    'Products.name': 'Rustic Granite Gloves',
  },
];
```

```javascript
// gravida.sit.amet@risus.net
[
  {
    'Products.name': 'Incredible Granite Cheese',
  },
];
```

## Source code

Please feel free to check out the
[full source code](https://github.com/cube-js/cube.js/tree/master/examples/recipes/column-based-access)
or run it with the `docker-compose up` command. You'll see the result, including
queried data, in the console.
