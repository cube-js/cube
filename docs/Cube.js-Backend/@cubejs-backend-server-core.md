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
const CubejsServerCore = require('@cubejs-backend/server-core');
const express = require('express');
const path = require('path');

const expressApp = express();

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
core.initApp(expressApp);
```

## Options Reference

Both [CubejsServerCore](@cubejs-backend-server-core) `create` method and [CubejsServer](@cubejs-backend-server) constructor accept an object with the following configuration options for Cube.js.

```javascript
{
  dbType: String | (context: RequestContext) => String,
  externalDbType: String | (context: RequestContext) => String,
  schemaPath: String,
  basePath: String,
  webSocketsBasePath: String,
  devServer: Boolean,
  logger: (msg: String, params: Object) => any,
  driverFactory: (context: DriverContext) => BaseDriver | Promise<BaseDriver>,
  externalDriverFactory: (context: RequestContext) => BaseDriver | Promise<BaseDriver>,
  contextToAppId: (context: RequestContext) => String,
  contextToDataSourceId: (context: RequestContext) => String,
  repositoryFactory: (context: RequestContext) => SchemaFileRepository,
  checkAuth: (req: ExpressRequest, authorization: String) => any,
  checkAuthMiddleware: (req: ExpressRequest, res: ExpressResponse, next: ExpressMiddleware) => any,
  queryTransformer: (query: Object, context: RequestContext) => Object,
  preAggregationsSchema: String | (context: RequestContext) => String,
  schemaVersion: (context: RequestContext) => String,
  extendContext: (req: ExpressRequest) => any,
  scheduledRefreshTimer: Boolean | Number,
  compilerCacheSize: Number,
  maxCompilerCacheKeepAlive: Number,
  updateCompilerCacheKeepAlive: Boolean,
  telemetry: Boolean,
  allowUngroupedWithoutPrimaryKey: Boolean,
  orchestratorOptions: {
    redisPrefix: String,
    queryCacheOptions: {
      refreshKeyRenewalThreshold: number,
      backgroundRenew: Boolean,
      queueOptions: QueueOptions
    }
    preAggregationsOptions: {
      queueOptions: QueueOptions
    }
  },
  allowJsDuplicatePropsInSchema: Boolean
}

QueueOptions {
  concurrency: number
  continueWaitTimeout: number,
  executionTimeout: number,
  orphanedTimeout: number,
  heartBeatInterval: number
}

RequestContext {
  authInfo: Object,
  requestId: String
}

DriverContext extends RequestContext {
  dataSource: String
}

SchemaFileRepository {
  dataSchemaFiles(): Promise<FileContent[]>
}

FileContent {
  fileName: String,
  content: String
}
```

### dbType

Either `String` or `Function` could be passed. Providing a `Function` allows to dynamically select a database type depending on the user's context. It is usually used in [Multitenancy Setup](multitenancy-setup).

If no option is passed, Cube.js will lookup for environment variable
`CUBEJS_DB_TYPE` to resolve `dbType`.

Called only once per [appId](#options-reference-context-to-app-id).

### externalDbType

Should be used in conjunction with [externalDriverFactory](#external-driver-factory) option.
Either `String` or `Function` could be passed.
Providing a `Function` allows to dynamically select a database type depending on the user's context.
It is usually used in [Multitenancy Setup](multitenancy-setup).

Called only once per [appId](#options-reference-context-to-app-id).

### schemaPath

Path to schema files. The default value is `/schema`.

### basePath

[REST API](/rest-api) base path. The default value is `/cubejs-api`.

### webSocketsBasePath

base path for the websockets server. By default the websocket server will run on the root path.

### devServer

Boolean to enable or disable a development server mode. The default value is based on `NODE_ENV` environment variable value. If the value of `NODE_ENV` is `production` it is `false`, otherwise it is `true`.

**NOTE:** Using dev server in production is a security risk as there're unsecured routes which allows to get access to all data defined in Cube.js schema.

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

Called once per [dataSourceId](#options-reference-context-to-data-source-id). Can return a `Promise` for a driver.

```javascript
const PostgresDriver = require('@cubejs-backend/postgres-driver');

CubejsServerCore.create({
  driverFactory: ({ dataSource }) => new PostgresDriver({ database: dataSource })
})
```

### externalDriverFactory

Set database driver for external rollup database.
Please refer to [External Rollup](pre-aggregations#external-rollup) documentation for more info.
The function accepts context object as an argument
to let dynamically load database drivers, which is usually used
in [Multitenancy Applications](multitenancy-setup).

Called once per [appId](#options-reference-context-to-app-id). Can return a `Promise` for a driver.

```javascript
const MySQLDriver = require('@cubejs-backend/mysql-driver');

CubejsServerCore.create({
  externalDbType: 'mysql',
  externalDriverFactory: () => new MySQLDriver({
    host: process.env.CUBEJS_EXT_DB_HOST,
    database: process.env.CUBEJS_EXT_DB_NAME,
    port: process.env.CUBEJS_EXT_DB_PORT,
    user: process.env.CUBEJS_EXT_DB_USER,
    password: process.env.CUBEJS_EXT_DB_PASS,
  })
})
```

### contextToAppId

It is a [Multitenancy Setup](multitenancy-setup) option.

`contextToAppId` is a  function to determine an App ID which is used as caching key for various in-memory structures like schema compilation results, connection pool, etc.

Called on each request.

```javascript
CubejsServerCore.create({
  contextToAppId: ({ authInfo }) => `CUBEJS_APP_${authInfo.user_id}`
})
```

### contextToDataSourceId

`contextToDataSourceId` is a function to determine a DataSource Id which is used to override the `contextToAppId` caching key for managing connection pools.

Called on each request.

```javascript
CubejsServerCore.create({
  contextToAppId: ({ authInfo }) => `CUBEJS_APP_${authInfo.user_id}`,
  contextToDataSourceId: ({ authInfo }) => `CUBEJS_APP_${authInfo.tenantId}`
});
```

### repositoryFactory

This option allows to customize the repository for Cube.js data schema files. It
is a function, which accepts a context object and can dynamically select
repositories with schema files based on [SchemaFileRepository](#SchemaFileRepository) contract. Learn more about it in [Multitenancy Setup](multitenancy-setup) guide.

Called only once per [appId](#options-reference-context-to-app-id).

```javascript
const FileRepository = require('@cubejs-backend/server-core/core/FileRepository');

// using built-in SchemaFileRepository implementation and supplying the path to schema files
CubejsServerCore.create({
  repositoryFactory: ({ authInfo }) => new FileRepository(`schema/${authInfo.appId}`)
});

// supplying your own SchemaFileRepository implementation to return array of files
CubejsServerCore.create({
  repositoryFactory: ({ authInfo }) => {
    return {
      dataSchemaFiles: async () => await Promise.resolve([{ fileName: 'file.js', content: 'contents of file'}])
    }
  }
});
```

### checkAuth

Used in both REST and Websocket API.
Can be `async` functon.
Default implementation parses [JSON Web Tokens (JWT)](https://jwt.io/) in `Authorization` and sets payload to `req.authInfo` if it's verified.
More info on how to generate such tokens is [here](security#security-context).

You can set `req.authInfo = { u: { ...userContextObj } }` inside the middleware if you want to customize [USER_CONTEXT](cube#context-variables-user-context).

Called on each request.

Also, you can use empty `checkAuth` function to disable built-in security. See an example below.

```javascript
CubejsServerCore.create({
  checkAuth: (req, auth) => {}
});
```

### checkAuthMiddleware

This is an [Express Middleware](https://expressjs.com/en/guide/using-middleware.html) for authentication.
Default implementation calls [checkAuth](#options-reference-check-auth).

Called on each request.

### queryTransformer

This is a security hook to check your query just before it gets processed.
You can use this very generic API to implement any type of custom security checks your app needs and transform input query accordingly.

Called on each request.

For example you can use `queryTransformer` to add row level security filter where needed.

```javascript
CubejsServerCore.create({
  queryTransformer: (query, { authInfo }) => {
    const user = authInfo.u;
    if (user.filterByRegion) {
      query.filters.push({
        member: 'Regions.id',
        operator: 'equals',
        values: [user.regionId]
      })
    }
    return query;
  }
});
```

### preAggregationsSchema

Schema name to use for storing pre-aggregations.
For some drivers like MySQL it's name for pre-aggregation database as there's no database schema concept there.
Either `String` or `Function` could be passed.
Providing a `Function` allows to dynamically set the pre-aggregation schema name depending on the user's context.

Called once per [appId](#options-reference-context-to-app-id).

```javascript
CubejsServerCore.create({
  preAggregationsSchema: ({ authInfo }) => `pre_aggregations_${authInfo.tenantId}`
});
```

It is usually used in [Multitenancy Setup](multitenancy-setup).

### schemaVersion

Schema version can be used to tell Cube.js schema should be recompiled in case schema code depends on dynamic definitions fetched from some external database or API.
This method is called on each request however `RequestContext` parameter is reused per application id returned by [contextToAppId](#options-reference-context-to-app-id).
If returned string has been changed, schema will be recompiled.
It can be used in both multitenant and single tenant environments.

```javascript
const tenantIdToDbVersion = {};

CubejsServerCore.create({
  schemaVersion: ({ authInfo }) => tenantIdToDbVersion[authInfo.tenantId]
});
```

### scheduledRefreshTimer

Pass `true` to enable default every 5 second scheduled refresh timer with default querying options.
Can be also set using `CUBEJS_SCHEDULED_REFRESH_TIMER` env variable.
Each timer invocation will call `runScheduledRefresh()` method as in:

```javascript
setInterval(() => serverCore.runScheduledRefresh(), 5000);
```

Learn more about [scheduled refresh here](caching#keeping-cache-up-to-date)

You can also pass custom interval in seconds as value:

```javascript
CubejsServerCore.create({
  scheduledRefreshTimer: 10
});
```

You can pass comma separated list of timezones to refresh in `CUBEJS_SCHEDULED_REFRESH_TIMEZONES` env variable. For example:
```
CUBEJS_SCHEDULED_REFRESH_TIMEZONES=America/Los_Angeles,UTC
```

Best practice is to run `scheduledRefreshTimer` in a separate worker Cube.js instance.
For serverless deployments [REST API](rest-api#api-reference-v-1-run-scheduled-refresh) should be used instead of timer.

### extendContext

Option to extend the `RequestContext` with custom values. This method is called on each request.  Can be async.

### compilerCacheSize

Maximum number of compiled schemas to persist with in-memory cache.  Defaults to 250, but optimum value will depend on deployed environment. When the max is reached, will start dropping the least recently used schemas from the cache.

### maxCompilerCacheKeepAlive

Maximum length of time in ms to keep compiled schemas in memory.  Default keeps schemas in memory indefinitely.

### updateCompilerCacheKeepAlive

Providing `updateCompilerCacheKeepAlive: true` keeps frequently used schemas in memory by reseting their `maxCompilerCacheKeepAlive` every time they are accessed.

### allowUngroupedWithoutPrimaryKey

Providing `allowUngroupedWithoutPrimaryKey: true` disables primary key inclusion check for `ungrouped` queries.

### telemetry

Cube.js collects high-level anonymous usage statistics for servers started in development mode. It doesn't track any credentials, schema contents or queries issued. This statistics is used solely for the purpose of constant cube.js improvement.

You can opt out of it any time by setting `telemetry` option to
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
| rollupOnlyMode | When enabled, an error will be thrown if a query can't be served from a pre-aggregation (rollup) | `false`
| queryCacheOptions | Query cache options for DB queries | `{}`
| queryCacheOptions.refreshKeyRenewalThreshold | Time in seconds to cache the result of [refreshKey](cube#parameters-refresh-key) check | `defined by DB dialect`
| queryCacheOptions.backgroundRenew | Controls whether to wait in foreground for refreshed query data if `refreshKey` value has been changed. Refresh key queries or pre-aggregations are never awaited in foreground and always processed in background unless cache is empty. If `true` it immediately returns values from cache if available without [refreshKey](cube#parameters-refresh-key) check to renew in foreground. Default value before 0.15.0 was `true` | `false`
| queryCacheOptions.queueOptions | Query queue options for DB queries | `{}`
| preAggregationsOptions | Query cache options for pre-aggregations | `{}`
| preAggregationsOptions.queueOptions | Query queue options for pre-aggregations | `{}`
| preAggregationsOptions.externalRefresh | When running a separate instance of Cube.js to refresh pre-aggregations in the background, this option can be set on the API instance to prevent it from trying to check for rollup data being current - it won't try to create or refresh them when this option is `true` | `false`

To set options for `queryCache` and `preAggregations`, set an object with key queueOptions. `queryCacheOptions` are used while querying database tables, while `preAggregationsOptions` settings are used to query pre-aggregated tables.

```javascript
const queueOptions = {
  concurrency: 3
};

CubejsServerCore.create({
  orchestratorOptions: {
    queryCacheOptions: {
      refreshKeyRenewalThreshold: 30,
      backgroundRenew: true,
      queueOptions
    },
    preAggregationsOptions: { queueOptions }
  }
});
```

## QueueOptions

Timeout and interval options' values are in seconds.

| Option | Description | Default Value |
| ------ | ----------- | ------------- |
| concurrency | Maximum number of queries to be processed simultaneosly | `2` |
| continueWaitTimeout | Long polling interval | `5` |
| executionTimeout | Total timeout of single query | `600` |
| orphanedTimeout | Query will be marked for cancellation if not requested during this period. | `120` |
| heartBeatInterval | Worker heartbeat interval. If `4*heartBeatInterval` time passes without reporting, the query gets cancelled. | `30` |

## RequestContext

`RequestContext` object is filled by context data on a HTTP request level.

### authInfo

Defined as `req.authInfo` which should be set by [checkAuthMiddleware](#checkAuthMiddleware).
Default implementation of [checkAuthMiddleware](#checkAuthMiddleware) uses [JWT Security Token](security) payload and sets it to `req.authInfo`.

## SchemaFileRepository

The `SchemaFileRepository` contract defines an async `dataSchemaFiles` function which returns the files to compile for a schema. Returned by [repositoryFactory](#repositoryFactory). `@cubejs-backend/server-core/core/FileRepository` is the default implementation of the `SchemaFileRepository` contract which accepts [schemaPath](#schemaPath) in the constructor.

```javascript
class ApiFileRepository {
  async dataSchemaFiles() {
    const fileContents = await callExternalApiForFileContents();
    return [{ fileName: 'apiFile', content: fileContents }];
  }
}

CubejsServerCore.create({
  repositoryFactory: ({authInfo}) => new ApiFileRepository()
});
```

## Version

`CubejsServerCore.version` is a method that returns the semantic package version of `@cubejs-backend/server`.

```javascript
const CubejsServerCore = require('@cubejs-backend/server-core');

console.log(CubejsServerCore.version());
```

### allowJsDuplicatePropsInSchema

Boolean to enable or disable a check duplicate property names in all objects of a schema. The default value is `false`, and it is means the compiler would use the additional transpiler for check duplicates.