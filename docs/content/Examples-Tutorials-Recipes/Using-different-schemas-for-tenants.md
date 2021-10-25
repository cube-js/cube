---
title: Using Different Schemas for Tenants
permalink: /recipes/using-different-schemas-for-tenants
category: Examples & Tutorials
subCategory: Access control
menuOrder: 2
---

## Use case

We want to provide different data schemas to different tenants. In the recipe below, we'll
learn how to switch between multiple data schemas based on the tenant.

## Configuration

We have a folder structure as follows:

```bash
schema
├── avocado
│   └── Products.js
└── mango
    └── Products.js
```

Let's configure Cube to use a specific data schema path for each tenant. We'll
pass the tenant name as a part of [`securityContext`](https://cube.dev/docs/security/context#top)
into the [`repositoryFactory`](https://cube.dev/docs/config#repository-factory) function.

We'll also need to override the [`contextToAppId`](https://cube.dev/docs/config#context-to-app-id)
function to control how the schema compilation result is cached and provide the tenant names via the
[`scheduledRefreshContexts`](https://cube.dev/docs/config#scheduled-refresh-contexts) function
so a refresh worker can find all existing schemas and build pre-aggregations for them, if needed.

Our `cube.js` file will look like this:

```javascript
const FileRepository = require('@cubejs-backend/server-core/core/FileRepository');

module.exports = {
  contextToAppId: ({ securityContext }) =>
    `CUBEJS_APP_${securityContext.tenant}`,

  repositoryFactory: ({ securityContext }) =>
    new FileRepository(`schema/${securityContext.tenant}`),

  scheduledRefreshContexts: () => [
    { securityContext: { tenant: 'avocado' } },
    { securityContext: { tenant: 'mango' } },
  ]
};
```

## Data schema

In this example, we'd like to get products with odd `id` values for the `avocado` tenant and
with even `id` values the `mango` tenant:

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

To fetch the products, we will send two identical queries with different JWTs:

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

We will receive different data for each tenant, as expected:

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
  },
];
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
  },
];
```

## Source code

Please feel free to check out the
[full source code](https://github.com/cube-js/cube.js/tree/master/examples/recipes/using-different-schemas-for-tenants)
or run it with the `docker-compose up` command. You'll see the result, including
queried data, in the console.
