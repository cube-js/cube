---
title: Using Multiple Data Sources
permalink: /recipes/multiple-sources-same-schema
category: Examples & Tutorials
subCategory: Data sources
menuOrder: 3
---

## Use case

We need to access the data from different data sources for different tenants.
For example, we are the platform for the online website builder, and each client
can only view their data. The same data schema is used for all clients.

## Configuration

Each client has its own database. In this recipe, the `Mango Inc` tenant keeps
its data in the remote `ecom` database while the `Avocado Inc` tenant works with
the local database (bootstrapped in the `docker-compose.yml` file) which has the
same schema.

To enable multitenancy, use the
[`contextToAppId`](https://cube.dev/docs/config#options-reference-context-to-app-id)
function to provide distinct identifiers for each tenant. Also, implement the
[`driverFactory`](https://cube.dev/docs/config#options-reference-driver-factory)
function where you can select a data source based on the tenant name.
[JSON Web Token](https://cube.dev/docs/security) includes information about the
tenant name in the `tenant` property of the `securityContext`.

```javascript
const PostgresDriver = require('@cubejs-backend/postgres-driver');

module.exports = {
  contextToAppId: ({ securityContext }) =>
    `CUBEJS_APP_${securityContext.tenant}`,

  driverFactory: ({ securityContext } = {}) => {
    if (securityContext.tenant === 'cubeDev') {
      return new PostgresDriver({
        database: 'cubeDev',
        host: 'postgres',
        user: 'postgres',
        password: 'example',
        port: '5432',
      });
    } else {
      return new PostgresDriver({
        database: 'ecom',
        host: 'demo-db.cube.dev',
        user: 'cube',
        password: '12345',
        port: '5432',
      });
    }
  },
};
```

## Query

To get users for different tenants, we will send two identical requests with
different JWTs:

```javascript
// JWT`s payload for "Avocado Inc"
{
  "sub": "1234567890",
  "tenant": "Avocado Inc",
  "iat": 1516239022,
  "exp": 1724995581
}
```

```javascript
// JWT`s payload for "Mango Inc"
{
  "sub": "1234567890",
  "tenant": "Mango Inc",
  "iat": 1516239022,
  "exp": 1724995581
}
```

## Result

We have received different data from different data sources depending on the
tenant`s name:

```javascript
// Cube Inc last users:
[
  {
    'Users.id': 700,
    'Users.name': 'Freddy Gulgowski',
  },
  {
    'Users.id': 699,
    'Users.name': 'Julie Crooks',
  },
  {
    'Users.id': 698,
    'Users.name': 'Macie Ryan',
  },
];
```

```javascript
// Cube Dev last users:
[
  {
    'Users.id': 705,
    'Users.name': 'Zora Vallery',
  },
  {
    'Users.id': 704,
    'Users.name': 'Fawn Danell',
  },
  {
    'Users.id': 703,
    'Users.name': 'Moyra Denney',
  },
];
```

## Source code

Please feel free to check out the
[full source code](https://github.com/cube-js/cube.js/tree/master/examples/recipes/multiple-sources-same-schema)
or run it with the `docker-compose up` command. You'll see the result, including
queried data, in the console.
