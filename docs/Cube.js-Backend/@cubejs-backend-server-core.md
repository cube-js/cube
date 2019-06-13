---
title: cubejs-backend-server-core
permalink: /@cubejs-backend-server-core
category: Cube.js Backend
subCategory: Reference
menuOrder: 6
---

`@cubejs-backend/server-core` could be used to embed Cube.js Backend into your
[Express](https://expressjs.com/) application.

## create(options)

`CubejsServerCore.create` is an entry point for a Cube.js server application. It creates an instance of `CubejsServerCore`, which could be embedded for example into Express application.

```javascript
import * as CubejsServerCore from "@cubejs-backend/server-core";
import * as express from 'express';
import * as path from 'path';

const express = express();

const dbType = 'mysql';
const options = {
  dbType,
  devServer: false,
  logger: (msg, params) => {
    console.log(`${msg}: ${JSON.stringify(params)}`);
  },
  schemaPath: path.join('assets', 'schema')
};

const core = CubejsServerCore.create(options);
await core.initApp(express);
```

## Options Reference

Both [CubejsServerCore](@cubejs-backend-server-core) and [CubejsServer](@cubejs-backend-server) `create` methods accept an object with the following configuration options for Cube.js.

```javascript
{
  dbType: String | Function,
  schemaPath: String,
  basePath: String,
  devServer: Boolean,
  logger: Function,
  driverFactory: Function,
  contextToAppId: Function,
  repositoryFactory: Function,
  checkAuthMiddleware: Function,
  telemetry: Boolean,
  orchestratorOptions: {
    redisPrefix: String,
    queryCacheOptions: {
      refreshKeyRenewalThreshold: number,
      queueOptions: QueueOptions
    }
    preAggregationsOptions: {
      queueOptions: QueueOptions
    }
  }
}

// QueueOptions
{
  concurrency: number
  continueWaitTimeout: number,
  executionTimeout: number,
  orphanedTimeout: number,
  heartBeatInterval: number
}
```

### dbType

Either `String` or `Function` could be passed. Providing a `Function` allows to dynamically select a database type depending on the user's context. It is usually used in [Multitenancy Setup](multitenancy-setup).

If no option is passed, Cube.js will lookup for environment variable
`CUBEJS_DB_TYPE` to resolve `dbType`.

### schemaPath

Path to schema files. The default value is `/schema`.

### basePath

[REST API](/rest-api) base path. The default value is `/cubejs-api`.

### devServer

Boolean to enable or disable a development server mode. The default value is based on `NODE_ENV` environment variable value. If the value of `NODE_ENV` is `production` it is `false`, otherwise it is `true`.

### logger

A function to setup a custom logger. It accepts the following arguments:
  * `message`: Cube.js Backend event message
  * `params`: Parameters of the call

```javascript
CubejsServerCore.create({
  logger: (msg, params) => {
    console.log(`${msg}: ${JSON.stringify(params)}`);
  }
})
```

### driverFactory

Set a custom database driver. The function accepts context object as an argument
to let dynamically load database drivers, which is usually used
in [Multitenancy Applications](multitenancy-setup).

```javascript
const PostgresDriver = require('@cubejs-backend/postgres-driver');

CubejsServerCore.create({
  driverFactory: () => new PostgresDriver();
})
```


### contextToAppId

It is a [Multitenancy Setup](multitenancy-setup) option.

`contextToAppId` is a  function to determine an App ID which is used as caching key for various in-memory structures like schema compilation results, connection pool, etc.

```javascript
CubejsServerCore.create({
  contextToAppId: ({ authInfo }) => `CUBEJS_APP_${authInfo.user_id}`
})
```

### repositoryFactory

This option allows to customize the repository for Cube.js data schema files. It
is a function, which accepts a context object and can dynamically select
repositories with schema files. Learn more about it in [Multitenancy Setup](multitenancy-setup) guide.

```javascript
const FileRepository = require('@cubejs-backend/server-core/core/FileRepository');

CubejsServerCore.create({
  repositoryFactory: ({ authInfo }) => new FileRepository(`schema/${authInfo.appId}`)
});
```

### checkAuthMiddleware

This is an [Express Middleware](https://expressjs.com/en/guide/using-middleware.html) for authentication.

You can set `req.authInfo = { u: { ...userContextObj } }` inside the middleware if you want to customize [USER_CONTEXT](cube#context-variables-user-context).

Also, you can use `checkAuthMiddleware` to disable built-in security. See an example below.

```javascript
CubejsServerCore.create({
  checkAuthMiddleware: (req, res, next) => {
    return next && next();
  }
});
```

### telemetry

Cube.js collects anonymous events to better understand our
users. You can opt out of event tracking any time by setting `telemetry` option to
`false` or, alternatively, by setting `CUBEJS_TELEMETRY` environment variable to
`false`.

```javascript
CubejsServerCore.create({
  telemetry: false
});
```

### orchestratorOptions

You can pass this object to set advanced options for Cube.js Query Orchestrator.

_Please note that this is advanced configuration._

| Option | Description | Default Value |
| ------ | ----------- | ------------- |
| redisPrefix | Prefix to be set an all Redis keys | `STANDALONE` |
| queryCacheOptions | Query cache options for DB queries | `{}`
| preAggregationsOptions | Query cache options for pre-aggregations | `{}`

To set options for `queryCache` and `preAggregations`, set an object with key queueOptions. `queryCacheOptions` are used while querying database tables, while `preAggregationsOptions` settings are used to query pre-aggregated tables.

`queryCacheOptions` also has `refreshKeyRenewalThreshold` option to set time in seconds to cache the result of [refreshKey](cube#parameters-refresh-key) check. The default value is `120`.

```javascript
const queueOptions = {
  concurrency: 3
};

CubejsServerCore.create({
  orchestratorOptions: {
    queryCacheOptions: {
      refreshKeyRenewalThreshold: 30,
      queueOptions
    },
    preAggregationsOptions: { queueOptions }
  }
});
```

#### QueueOptions

Timeout and interval options' values are in seconds.

| Option | Description | Default Value |
| ------ | ----------- | ------------- |
| concurrency | Maximum number of queries to be processed simultaneosly | `2` |
| continueWaitTimeout | Long polling interval | `5` |
| executionTimeout | Total timeout of single query | `600` |
| orphanedTimeout | Query will be marked for cancellation if not requested during this period. | `120` |
| heartBeatInterval | Worker heartbeat interval. If `4*heartBeatInterval` time passes without reporting, the query gets cancelled. | `30` |

