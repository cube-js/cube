---
title: Manage Access to Multiple Data Sources
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

Each client has own database. To manage access to the databases, we need to use
the
[`contextToAppId`](https://cube.dev/docs/config#options-reference-context-to-app-id)
function and the
[`driverFactory`](https://cube.dev/docs/config#options-reference-driver-factory)
property. [JSON Web Token](https://cube.dev/docs/security) includes information
about the tenant name in the `tenant` property.

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
{
  "order": [
    [
      "Users.id",
      "desc"
    ]
  ],
  "dimensions": [
    "Users.id",
    "Users.name"
  ],
  "limit": 5
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
  {
    'Users.id': 697,
    'Users.name': 'Hailie Mosciski',
  },
  {
    'Users.id': 696,
    'Users.name': 'Gia Abbott',
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
  {
    'Users.id': 702,
    'Users.name': 'Bondy Davidman',
  },
  {
    'Users.id': 701,
    'Users.name': 'Caritta Hiley',
  },
];
```

## Source code

Please feel free to check out the
[full source code](https://github.com/cube-js/cube.js/tree/master/examples/recipes/multiple-sources-same-schema)
or run it with the `docker-compose up` command. You'll see the result, including
queried data, in the console.
