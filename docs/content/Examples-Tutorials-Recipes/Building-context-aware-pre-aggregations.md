---
title: Building Context-Aware Pre-aggregations
permalink: /recipes/building-context-aware-pre-aggregations
category: Examples & Tutorials
subCategory: Query acceleration
menuOrder: 6
---

## Use case

Using different environments to test a product is a good practice. Let's imagine
we have three environments: testing, staging, and production. We want to use
multiple database schemas depending on the current environment. In addition, we
want to use pre-aggregation for each schema. In the recipe below, we'll
learn how to dynamically select a database schema and create a pre-aggregation with
the scheduled update.

## Data schema

To select the database schema, we will use the
[`COMPILE_CONTEXT`](https://cube.dev/docs/schema/reference/cube#context-variables-compile-context)
global variable. We'll pass into the `COMPILE_CONTEXT` an `env` variable from
[`securityContext`](https://cube.dev/docs/security/context) with our environment
value.

```javascript
const {
  securityContext: { env },
} = COMPILE_CONTEXT;

cube(`Products`, {
  sql: `SELECT * FROM ${env}.Orders`,
  ...,
}
```

To significantly reduce response time, we will define a pre-aggregation.
We'll also specify the
[`refreshKey`](https://cube.dev/docs/schema/reference/pre-aggregations#parameters-refresh-key)
option for keeping pre-aggregations up to date:

```javascript
preAggregations: {
    amountByClientName: {
      measures: [Products.amount],
      dimensions: [Products.clientName],
      timeDimension: Products.createdAt,
      granularity: `day`,
      refreshKey: {
        every: `1 minute`,
      },
    },
  }
```

## Configuration

We'll use Cube Store as pre-aggregations storage with
[Refresh Worker](https://cube.dev/docs/deployment/production-checklist#set-up-refresh-worker),
updating our pre-aggregations in the background. The problem is Refresh Worker
does not have a `securityContext` when he started. Because of this, he will not
be able to figure out which scheme to use to build a pre-aggregation. To fix
this, we can use the
[`scheduledRefreshContexts`](https://cube.dev/docs/config#scheduled-refresh-contexts)
function, which allows to generate the security contexts:

```javascript
module.exports = {
  // Provides distinct identifiers for each tenant which are used as caching keys
  contextToAppId: ({ securityContext }) => `CUBEJS_APP_${securityContext.env}`,

  // Defines contexts for scheduled pre-aggregation update
  scheduledRefreshContexts: async () => [
    {
      securityContext: {
        env: 'testing',
      },
    },
    {
      securityContext: {
        env: 'staging',
      },
    },
    {
      securityContext: {
        env: 'production',
      },
    }
  ]
};
```

## Query

Now we can send the query to get the client name and amount for `orders` table
from multiple schemas using
[JWT](https://cube.dev/docs/security#generating-json-web-tokens-jwt):

```javascript
{
  "sub": "1234567890",
  "env": "testing",
  "iat": 1000000000,
  "exp": 5000000000
}
```

```javascript
{
  "measures": ["Products.amount"],
  "timeDimensions": [
    {
      "dimension": "Products.createdAt",
      "granularity": "day"
    }
  ],
  "order": {
    "Products.amount": "desc"
  },
  "dimensions": ["Products.clientName"],
  "limit": 2
}
```

## Result

We'll get the data from various pre-aggregations, which will update in the
background using the `scheduledRefreshContexts` function:

```javascript
// Response from query to testing:
[
  {
    "Products.clientName": "At Pede Cras Corporation",
    "Products.createdAt.day": "2021-09-22T00:00:00.000",
    "Products.createdAt": "2021-09-22T00:00:00.000",
    "Products.amount": "500"
  },
  {
    "Products.clientName": "Ipsum Leo Foundation",
    "Products.createdAt.day": "2021-09-22T00:00:00.000",
    "Products.createdAt": "2021-09-22T00:00:00.000",
    "Products.amount": "400"
  }
]
// Names of the used pre-aggregations:
{
  "dev_pre_aggregations.products_amount_by_client_name": {
    "targetTableName": "dev_pre_aggregations.products_amount_by_client_name_yhnblbst_ibkqy5r2_1gkonb8"
  }
}
// ---------
// Response from query to production:
[
  {
    "Products.clientName": "Quisque Purus Sapien Limited",
    "Products.createdAt.day": "2021-09-22T00:00:00.000",
    "Products.createdAt": "2021-09-22T00:00:00.000",
    "Products.amount": "8823"
  },
  {
    "Products.clientName": "Non Company",
    "Products.createdAt.day": "2021-08-22T00:00:00.000",
    "Products.createdAt": "2021-08-22T00:00:00.000",
    "Products.amount": "2284"
  }
]
// Names of the used pre-aggregations:
{
  "dev_pre_aggregations.products_amount_by_client_name": {
    "targetTableName": "dev_pre_aggregations.products_amount_by_client_name_jtjlvzlf_lvpyxxvh_1gkonb8"
  }
}
```

## Source code

Please feel free to check out the
[full source code](https://github.com/cube-js/cube.js/tree/master/examples/recipes/building-context-aware-pre-aggregations)
or run it with the `docker-compose up` command. You'll see the result, including
queried data, in the console.
