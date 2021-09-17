---
title: Using Different Schemas for Tenants
permalink: /recipes/using-different-schemas-for-tenants
category: Examples & Tutorials
subCategory: Access control
menuOrder: 2
---

## Use case

We want to manage user access to different Cubes. In the
recipe below, we'll learn how to use a different data schemas for various tenants.

## Configuration

We have two tenants and we created folders with the data schema for each one inside the `schema` folder. The folders are named such as a tenants. Then we have to tell Cube which data schema path to use for each tenant. We'll use the [`repositoryFactory`](https://cube.dev/docs/config#repository-factory) function to do it. We'll pass the tenant name into the `repositoryFactory` inside [`securityContext`](https://cube.dev/docs/security/context#top). We also should define the [`contextToAppId`](https://cube.dev/docs/config#context-to-app-id) property for caching schema compilation result.
Our cube.js file we'll look like this:
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

In our case we'll get the odd

## Query

To get the number of orders as a manager or operator, we will send two identical
requests with different JWTs:

```javascript
{
  "iat": 1000000000,
  "exp": 5000000000,
  "role": "manager"
}
```

```javascript
{
  "iat": 1000000000,
  "exp": 5000000000,
  "role": "operator"
}
```

## Result

We have received different data depending on the user's role.

```javascript
// Manager
[
  {
    'Orders.status': 'completed',
    'Orders.count': '3346',
  },
  {
    'Orders.status': 'shipped',
    'Orders.count': '3300',
  },
]
```

```javascript
// Operator
[
  {
    'Orders.status': 'processing',
    'Orders.count': '3354',
  },
]
```

## Source code

Please feel free to check out the
[full source code](https://github.com/cube-js/cube.js/tree/master/examples/recipes/role-based-access)
or run it with the `docker-compose up` command. You'll see the result, including
queried data, in the console.
