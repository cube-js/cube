---
title: Multitenancy
permalink: /multitenancy-setup
category: Configuration
menuOrder: 5
---

Cube.js supports multitenancy out of the box, both on database and data schema levels.
Multiple drivers are also supported, meaning that you can have one customer’s data in MongoDB and others in Postgres with one Cube.js instance.

There are 7 [configuration options](config#options-reference) you can leverage to make your multitenancy setup.
You can use all of them or just a couple, depending on your specific case.
The options are:

- `contextToAppId`
- `dbType`
- `externalDbType`
- `driverFactory`
- `repositoryFactory`
- `preAggregationsSchema`
- `queryTransformer`

All of the above options are functions, which you provide to Cube.js in [cube.js
config file](config). The
functions accept one argument - context object, which has a nested object -
[authInfo](config#request-context-auth-info), which acts as a container, where you can provide all the necessary data to identify user, organization, app, etc.
By default [authInfo](config#request-context-auth-info) is defined by [Cube.js API Token](security).

There're several multitenancy setup scenarios that can be achieved by using combinations of these configuration options.

### Multitenancy vs Multiple Data Sources

In cases where your Cube.js schema is spread across multiple different databases you may consider using [dataSource cube parameter](cube#parameters-data-source) instead of multitenancy.
Multitenancy designed for cases where you need to serve different datasets for multiple users or tenants which aren't related to each other.
On other hand multiple data sources can be used for a scenario where users need to access same data but from different databases.
Multitenancy and multiple data sources features aren't mutually exclusive and can be used together.

Typical multiple data sources configuration looks like:

[[warning | Note]]
| Existence of handling route for `default` data source is mandatory.
| It's used to resolve target query data source for now.
| This behavior will be changed in future releases.

**cube.js:**

```javascript
const PostgresDriver = require("@cubejs-backend/postgres-driver");
const AthenaDriver = require('@cubejs-backend/athena-driver');
const BigQueryDriver = require('@cubejs-backend/bigquery-driver');

module.exports = {
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
};
```

### User Context vs Multitenant Compile Context

As a rule of thumb [USER_CONTEXT](cube#context-variables-user-context) should be used in scenarios when you want to define row level security within the same database for different users of such database.
For example to separate access of two ecommerce administrators who work on different product categories within same ecommerce store.

```javascript
cube(`Products`, {
  sql: `select * from products where ${USER_CONTEXT.categoryId.filter('categoryId')}`
})
```

On other hand Multitenant [COMPILE_CONTEXT](cube#context-variables-compile-context) should be used when users in fact access different databases.
For example if you provide SaaS ecommerce hosting and each of your customers has separate database then each ecommerce store should be modelled as a separate tenant.

```javascript
const { authInfo: { tenantId } } = COMPILE_CONTEXT;

cube(`Products`, {
  sql: `select * from ${tenantId}.products`
})
```

### User Context vs queryTransformer

[USER_CONTEXT](cube#context-variables-user-context) great for use cases where you want to get explicit control over filtering of underlying data seen by users.
However for use cases where you want to reuse pre-aggregation tables for different users or even tenants [queryTransformer](config#options-reference-query-transformer) is much better choice.
[queryTransformer](config#options-reference-query-transformer) is also very convenient way of enforcing row level security by means of join logic defined in your cubes instead of embedding [USER_CONTEXT](cube#context-variables-user-context) filtering boiler plate into each cube.
Together with [contextToDataSourceId](config#options-reference-context-to-data-source-id) it allows to define both row level security filtering as well as reuse the same pre-aggregation set for each tenant.

## Same DB Instance with per Tenant Row Level Security

Per tenant row level security can be achieved by providing [queryTransformer](config#options-reference-query-transformer) which adds tenant identifier filter to the original query.
It uses [authInfo](config#request-context-auth-info) to determine which tenant is requesting the data.
This way in fact every tenant starts to see it's own data however all the resources like query queue and pre-aggregations are shared between all the tenants.

**cube.js:**
```javascript
module.exports = {
  queryTransformer: (query, { authInfo }) => {
    const user = authInfo.u;
    if (user.id) {
      query.filters.push({
        member: 'Users.id',
        operator: 'equals',
        values: [user.id]
      })
    }
    return query;
  }
};
```

## Multiple DB Instances with Same Schema

Let's consider the following example:

We store data for different users in different databases, but on the same Postgres host.
The database name is `my_app_1_2`, where `1` is **Application ID** and `2` is **User ID**.

To make it work with Cube.js, first we need to pass the `appId` and `userId` as context to every query.
We should include that into our token generation code.

```javascript
const jwt = require('jsonwebtoken');
const CUBE_API_SECRET='secret';

const cubejsToken = jwt.sign(
  { appId: appId, userId: userId },
  CUBE_API_SECRET,
  { expiresIn: '30d' }
);
```

Now, we can access them as [authInfo](config#request-context-auth-info) object inside the context object.
Let's first use [contextToAppId](config#options-reference-context-to-app-id) to create a dynamic Cube.js App ID for every combination of `appId` and `userId`.

[[warning | Note]]
| Cube.js App ID (result of [contextToAppId](config#options-reference-context-to-app-id)) is used as caching key for various in-memory structures like schema compilation results, connection pool, etc.
| Missing [contextToAppId](config#options-reference-context-to-app-id) definition will result in unexpected caching issues such as schema of one tenant is used for another one.

**cube.js:**
```javascript
module.exports = {
  contextToAppId: ({ authInfo }) => `CUBEJS_APP_${authInfo.appId}_${authInfo.userId}`
};
```

Next, we can use [driverFactory](config#options-reference-driver-factory) to dynamically select database, based on `appId` and `userId`.

**cube.js:**
```javascript
const PostgresDriver = require("@cubejs-backend/postgres-driver");

module.exports = {
  contextToAppId: ({ authInfo }) => `CUBEJS_APP_${authInfo.appId}_${authInfo.userId}`,
  driverFactory: ({ authInfo }) =>
    new PostgresDriver({
      database: `my_app_${authInfo.appId}_${authInfo.userId}`
    })
};
```

## Same DB Instance with per Tenant Pre-Aggregations

To support per tenant pre-aggregation of data within same database instance you should provide [preAggregationsSchema](config#options-reference-pre-aggregations-schema) option.
You should use [authInfo](config#request-context-auth-info) to determine tenant which requesting the data.

**cube.js:**
```javascript
module.exports = {
  contextToAppId: ({ authInfo }) => `CUBEJS_APP_${authInfo.userId}`,
  preAggregationsSchema: ({ authInfo }) => `pre_aggregations_${authInfo.userId}`
};
```

## Multiple Schema and Drivers

What if for application with ID 3 data is stored not in Postgres, but in MongoDB?

We can instruct Cube.js to connect to MongoDB in that case, instead of
Postgres. For that purpose we'll use [dbType](config#options-reference-db-type) option to dynamically set database
type. We also need to modify our [driverFactory](config#options-reference-driver-factory) option.
You should use [authInfo](config#request-context-auth-info) to determine tenant which requesting the data.

**cube.js:**
```javascript
const PostgresDriver = require("@cubejs-backend/postgres-driver");
const MongoBIDriver = require('@cubejs-backend/mongobi-driver');

module.exports = {
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
};
```

Lastly, we want to have separate data schemas for every application. In this case we can
use `repositoryFactory` option to dynamically set a repository with schema files depending on the `appId`.

Below you can find final setup with `repositoryFactory` option.

**cube.js:**
```javascript
const PostgresDriver = require("@cubejs-backend/postgres-driver");
const MongoBIDriver = require('@cubejs-backend/mongobi-driver');
const FileRepository = require('@cubejs-backend/server-core/core/FileRepository');

module.exports = {
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
};
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
