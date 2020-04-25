---
title: Multitenancy Setup
permalink: /multitenancy-setup
category: Cube.js Backend
menuOrder: 5
---

Cube.js supports multitenancy out of the box, both on database and data schema levels.
Multiple drivers are also supported, meaning that you can have one customer’s data in MongoDB and others in Postgres with one Cube.js instance.

There are 7 [configuration options](@cubejs-backend-server-core#options-reference) you can leverage to make your multitenancy setup.
You can use all of them or just a couple, depending on your specific case.
The options are:

- `contextToAppId`
- `dbType`
- `externalDbType`
- `driverFactory`
- `repositoryFactory`
- `preAggregationsSchema`
- `queryTransformer`

Please refer to [@cubejs-backend-server-core](@cubejs-backend-server-core) and [@cubejs-backend-server](@cubejs-backend-server) docs to see examples on how `CubejsServerCore` and `CubejsServer` can be used.

All of the above options are functions, which you provide on Cube.js server instance creation. The
functions accept one argument - context object, which has a nested object -
`authInfo`, which acts as a container, where you can provide all the necessary data to identify user, organization, app, etc.
You put data into `authInfo` when creating a Cube.js API Token.

There're several multitenancy setup scenarios that can be achieved by using combinations of these configuration options.

### Multitenancy vs Multiple Data Sources

In cases where your Cube.js schema is spread across multiple different databases you may consider using [dataSource cube parameter](cube#parameters-data-source) instead of multitenancy.
Multitenancy designed for cases where you need to serve different datasets for multiple users or tenants which aren't related to each other.
On other hand multiple data sources can be used for a scenario where users need to access same data but from different databases.
Multitenancy and multiple data sources features aren't mutually exclusive and can be used together.

Typical multiple data sources configuration looks like:

**index.js:**

```javascript
const CubejsServer = require('@cubejs-backend/server');
const PostgresDriver = require("@cubejs-backend/postgres-driver");
const AthenaDriver = require('@cubejs-backend/athena-driver');
const BigQueryDriver = require('@cubejs-backend/bigquery-driver');

const server = new CubejsServer({
  dbType: ({ dataSource } = {}) => {
    if (dataSource === 'web') {
      return 'athena';
    } else if (dataSource === 'googleAnalytics') {
      return 'bigquery';
    } else {
      return 'postgres';
    }
  },
  driverFactory: ({ dataSource } = {}) => {
    if (dataSource === 'web') {
      return new AthenaDriver();
    } else if (dataSource === 'googleAnalytics') {
      return new BigQueryDriver();
    } else if (dataSource === 'financials'){
      return new PostgresDriver({
        database: 'financials',
        host: 'financials-db.acme.com',
        user: process.env.FINANCIALS_DB_USER,
        password: process.env.FINANCIALS_DB_PASS
      });
    } else {
      return new PostgresDriver();
    }
  }
});

server.listen().then(({ version, port }) => {
  console.log(`🚀 Cube.js server (${version}) is listening on ${port}`);
});
```


## Same DB Instance with per Tenant Row Level Security

Per tenant row level security can be achieved by providing [queryTransformer](@cubejs-backend-server-core#query-transformer) which adds tenant identifier filter to the original query.
This way in fact every tenant starts to see it's own data however all the resources like query queue and pre-aggregations are shared between all the tenants.

**index.js:**
```javascript
const CubejsServer = require('@cubejs-backend/server');

const server = new CubejsServer({
  queryTransformer: (query, { authInfo }) => {
    const user = authInfo.u;
    if (user.id) {
      query.filters.push({
        dimension: 'Users.id',
        operator: 'equals',
        values: [user.id]
      })
    }
    return query;
  }
});

server.listen().then(({ version, port }) => {
  console.log(`🚀 Cube.js server (${version}) is listening on ${port}`);
});
```

## Multiple DB Instances with Same Schema

Let's consider the following example:

We store data for different users in different databases, but on the same Postgres host. The database name is `my_app_1_2`, where `1`
is **Application ID** and `2` is **User ID**.

To make it work with Cube.js,
first we need to pass the `appId` and `userId` as context to every query. We
should include that into our token generation code.

```javascript
const jwt = require('jsonwebtoken');
const CUBE_API_SECRET='secret';

const cubejsToken = jwt.sign(
  { appId: appId, userId: userId },
  CUBE_API_SECRET,
  { expiresIn: '30d' }
);
```

Now, we can access them as `authInfo` object inside the context object. Let's
first use `contextToAppId` to create a dynamic Cube.js App ID for every combination of
`appId` and `userId`. Cube.js App ID is used as caching key for various in-memory structures like schema compilation results, connection pool, etc.

**index.js:**
```javascript
const CubejsServer = require('@cubejs-backend/server');

const server = new CubejsServer({
  contextToAppId: ({ authInfo }) => `CUBEJS_APP_${authInfo.appId}_${authInfo.userId}`
});

server.listen().then(({ version, port }) => {
  console.log(`🚀 Cube.js server (${version}) is listening on ${port}`);
});
```

Next, we can use `driverFactory` to dynamically select database, based on
`appId` and `userId`.

**index.js:**
```javascript
const PostgresDriver = require("@cubejs-backend/postgres-driver");
const CubejsServer = require('@cubejs-backend/server');

const server = new CubejsServer({
  contextToAppId: ({ authInfo }) => `CUBEJS_APP_${authInfo.appId}_${authInfo.userId}`,
  driverFactory: ({ authInfo }) =>
    new PostgresDriver({
      database: `my_app_${authInfo.appId}_${authInfo.userId}`
    })
});

server.listen().then(({ version, port }) => {
  console.log(`🚀 Cube.js server (${version}) is listening on ${port}`);
});
```

## Same DB Instance with per Tenant Pre-Aggregations

To support per tenant pre-aggregation of data within same database instance you should provide `preAggregationsSchema` option.

**index.js:**
```javascript
const PostgresDriver = require("@cubejs-backend/postgres-driver");
const CubejsServer = require('@cubejs-backend/server');

const server = new CubejsServer({
  contextToAppId: ({ authInfo }) => `CUBEJS_APP_${authInfo.userId}`,
  preAggregationsSchema: ({ authInfo }) => `pre_aggregations_${authInfo.userId}`
});

server.listen().then(({ version, port }) => {
  console.log(`🚀 Cube.js server (${version}) is listening on ${port}`);
});
```

## Multiple Schema and Drivers

What if for application with ID 3 data is stored not in Postgres, but in MongoDB?

We can instruct Cube.js to connect to MongoDB in that case, instead of
Postgres. For that purpose we'll use `dbType` option to dynamically set database
type. We also need to modify our `driverFactory` option.

**index.js:**
```javascript
const PostgresDriver = require("@cubejs-backend/postgres-driver");
const MongoBIDriver = require('@cubejs-backend/mongobi-driver');
const CubejsServer = require('@cubejs-backend/server');

const server = new CubejsServer({
  contextToAppId: ({ authInfo }) => `CUBEJS_APP_${authInfo.appId}_${authInfo.userId}`,
  dbType: ({ authInfo }) => {
    if (authInfo.appId === 3) {
      return 'mongobi';
    } else {
      return 'postgres';
    }
  },
  driverFactory: ({ authInfo }) => {
    if (authInfo.appId === 3) {
      return new MongoBIDriver({
        database: `my_app_${authInfo.appId}_${authInfo.userId}`
        port: 3307
      })
    } else {
      return new PostgresDriver({
        database: `my_app_${authInfo.appId}_${authInfo.userId}`
      })
    }
  }
});

server.listen().then(({ version, port }) => {
  console.log(`🚀 Cube.js server (${version}) is listening on ${port}`);
});
```

Lastly, we want to have separate data schemas for every application. In this case we can
use `repositoryFactory` option to dynamically set a repository with schema files depending on the `appId`.

Below you can find final setup with `repositoryFactory` option.

**index.js:**
```javascript
const PostgresDriver = require("@cubejs-backend/postgres-driver");
const MongoBIDriver = require('@cubejs-backend/mongobi-driver');
const FileRepository = require('@cubejs-backend/server-core/core/FileRepository');
const CubejsServer = require('@cubejs-backend/server');

const server = new CubejsServer({
  contextToAppId: ({ authInfo }) => `CUBEJS_APP_${authInfo.appId}_${authInfo.userId}`,
  dbType: ({ authInfo }) => {
    if (authInfo.appId === 3) {
      return 'mongobi';
    } else {
      return 'postgres';
    }
  },
  driverFactory: ({ authInfo }) => {
    if (authInfo.appId === 3) {
      return new MongoBIDriver({
        database: `my_app_${authInfo.appId}_${authInfo.userId}`
        port: 3307
      })
    } else {
      return new PostgresDriver({
        database: `my_app_${authInfo.appId}_${authInfo.userId}`
      })
    }
  },
  repositoryFactory: ({ authInfo }) => new FileRepository(`schema/${authInfo.appId}`)
});

server.listen().then(({ version, port }) => {
  console.log(`🚀 Cube.js server (${version}) is listening on ${port}`);
});
```

## Serverless Deployment

If you are deploying Cube.js to AWS Lambda with [serverless template](deployment#serverless) you need to use `AWSHandlers` from `@cubejs-backend/serverless-aws` package.

Add the following code to your `cube.js` file for the serverless multitenancy setup.

**cube.js:**
```javascript
const AWSHandlers = require('@cubejs-backend/serverless-aws');
const PostgresDriver = require("@cubejs-backend/postgres-driver");

module.exports = new AWSHandlers({
  contextToAppId: ({ authInfo }) => `CUBEJS_APP_${authInfo.appId}`,
  driverFactory: ({ authInfo }) =>
    new PostgresDriver({
      database: `my_app_${authInfo.appId}`
    })
});
```
