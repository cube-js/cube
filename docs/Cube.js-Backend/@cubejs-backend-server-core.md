---
title: cubejs-backend-server-core
permalink: /@cubejs-backend-server-core
category: Cube.js Backend
subCategory: Reference
menuOrder: 5
---

`@cubejs-backend/server-core` could be used to embed Cube.js Backend into your
Express application.

# API Reference

## CubejsServerCore.create(options)

Create an instance of `CubejsServerCore` to embed it in an [`Express`](https://expressjs.com/) application.

### Options object

| Option | Description | Required |
| ------ | ----------- | -------- |
| `dbType` | Type of your database | **required** |
| `schemaPath` | Path to schema files | optional, default: `/schema` |
| `basePath` | [REST API](/rest-api) base path.| optional, default: `/cubejs-api` |
| `devServer` | Enable development server | optional, default: `true` in development, `false` in production |
| `logger` | Pass function for your custom logger. [Learn more](#cubejs-server-core-create-options-logger-message-params) | optional |
| `driverFactory` | Pass function of the driver factory with your database type. [Learn more](#cubejs-server-core-create-options-driver-factory) | optional |
| `checkAuthMiddleware` | Express-style middleware to check authentication. [Learn more](#cubejs-server-core-create-options-check-auth-middleware-request-response-next) | optional |
| `orchestratorOptions` | Options object for Query Orchestrator [Learn more](#cubejs-server-core-create-options-orchestrator-options) | optional, default: `{}` |

### logger(message, params)

You can set custom logger using this option. 

  * `message` Cube.js Backend event message
  * `params` Parameters of the call

### driverFactory()

Set custom database driver. Example:

```javascript
driverFactory: () => new (require('@cubejs-backend/postgres-driver'));
```

### checkAuthMiddleware(request, response, next)

This is an [Express Middleware](https://expressjs.com/en/guide/using-middleware.html) for authentication.
Set `req.authInfo = { u: { ...userContextObj } }` inside middleware if you want to provide `USER_CONTEXT`. [Learn more](/cube#context-variables-user-context).
You can use checkAuthMiddleware to disable security:

```javascript
options = {
  checkAuthMiddleware: (req, res, next) => {
    return next && next();
  }
};
```

### orchestratorOptions

You can pass this object to set advanced options for Cube.js Query Orchestrator.

_Please note that this is advanced configuration._

| Option | Description | Default Value |
| ------ | ----------- | ------------- |
| redisPrefix | Prefix to be set an all Redis keys | `''` |
| queryCacheOptions | Query cache options for DB queries | `{}`
| preAggregationsOptions | Query cache options for pre-aggregations | `{}`

To set options for `queryCache` and `preAggregations`, set an object with key queueOptions. `queryCacheOptions` are used while querying database tables, while `preAggregationsOptions` settings are used to query pre-aggregated tables. Example:
```javascript
const queueOptions = {
  concurrency: 3
};
{ queryCacheOptions: { queueOptions }, preAggregationsOptions: { queueOptions } };
```

| Option | Description | Default Value |
| ------ | ----------- | ------------- |
| concurrency | Maximum number of queries to be processed simultaneosly | `2` |
| continueWaitTimeout | Polling timeout | `5` |
| executionTimeout | Total timeout of single query | `600` |
| orphanedTimeout | Inactivity timeout for query | `120` |
| heartBeatInterval | Heartbeat interval | `30` |

### Example

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
