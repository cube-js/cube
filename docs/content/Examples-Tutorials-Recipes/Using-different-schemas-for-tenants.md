---
title: Using Different Schemas for Tenants
permalink: /recipes/using-different-schemas-for-tenants
category: Examples & Tutorials
subCategory: Access control
menuOrder: 2
---

## Use case

We want to manage user access to different Cubes. In the recipe below, we'll
learn how to use multiple data schemas for various tenants.

## Configuration

We have two tenants and we created folders with the data schema for each one
inside the `schema` folder. The folders are named such as a tenants. Then we
have to tell Cube which data schema path to use for each tenant. We'll use the
[`repositoryFactory`](https://cube.dev/docs/config#repository-factory) option to
do it. We'll pass the tenant name into the `repositoryFactory` inside
[`securityContext`](https://cube.dev/docs/security/context#top). We also should
define the [`contextToAppId`](https://cube.dev/docs/config#context-to-app-id)
property for caching schema compilation result. Our `cube.js` file will look like
this:

```javascript
const FileRepository = require('@cubejs-backend/server-core/core/FileRepository');

module.exports = {
  contextToAppId: ({ securityContext }) =>
    `CUBEJS_APP_${securityContext.tenant}`,

  repositoryFactory: ({ securityContext }) =>
    new FileRepository(`schema/${securityContext.tenant}`),
};
```

## Data schema

In our case we'll get products with odd `id` values for the `Avocado` tenant and with
even `id` values the `Mango` tenant:

```javascript
// schema/avocado
cube(`Products`, {
  sql: `SELECT * FROM public.Products WHERE MOD (id, 2) = 1`,

  ...
});

// schema/mango
cube(`Products`, {
  sql: `SELECT * FROM public.Products WHERE MOD (id, 2) = 0`,

  ...
});
```

## Query

To get the products, we will send two identical queries with different JWTs:

```javascript
{
  "sub": "1234567890",
  "tenant": "Avocado",
  "iat": 1000000000,
  "exp": 5000000000
}
```

```javascript
{
  "sub": "1234567890",
  "tenant": "Mango",
  "iat": 1000000000,
  "exp": 5000000000
}
```

## Result

We have received different data from schemas corresponding to various tenants
and located in different folders:

```javascript
// Avocado products
[
  {
    'Products.id': 1,
    'Products.name': 'Generic Fresh Keyboard',
  },
  {
    'Products.id': 3,
    'Products.name': 'Practical Wooden Keyboard',
  },
  {
    'Products.id': 5,
    'Products.name': 'Handcrafted Rubber Chicken',
  }
]
```

```javascript
// Mango products:
[
  {
    'Products.id': 2,
    'Products.name': 'Gorgeous Cotton Sausages',
  },
  {
    'Products.id': 4,
    'Products.name': 'Handmade Wooden Soap',
  },
  {
    'Products.id': 6,
    'Products.name': 'Handcrafted Plastic Chair',
  }
]
```

## Source code

Please feel free to check out the
[full source code](https://github.com/cube-js/cube.js/tree/master/examples/recipes/using-different-schemas-for-tenants)
or run it with the `docker-compose up` command. You'll see the result, including
queried data, in the console.
