---
title: cubejs-backend-server-core
permalink: /@cubejs-backend-server-core
category: Cube.js Backend
subCategory: Reference
menuOrder: 5
---

`@cubejs-backend/server-core` could be used to embed Cube.js Backend into your
Express application.

## API Reference

### CubejsServerCore.create(options)

Create an instance of `CubejsServerCore` to embed it in an `Express` application.

#### Options object

| Option | Description | Required |
| ------ | ----------- | -------- |
| `dbType` | Type of your database | **required** |
| `schemaPath` | Path to schema files | optional, default: `/schema` |
| `basePath` | [REST API](/rest-api) base path.| optional, default: `/cubejs-api` |
| `devServer` | Enable development server | optional, default: `true` in development, `false` in production |
| `logger(msg, params)` | Pass function for your custom logger. | optional |
| `driverFactory` | Pass function of the driver factory with your database type. | optional |
| `checkAuthMiddleware` | Express-style middleware to check authentication. Set `req.authInfo = { u: { ...userContextObj } }` inside middleware if you want to provide `USER_CONTEXT`. [Learn more](/cube#context-variables-user-context). | optional |

#### Example

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

#### Disable Security
Security can be disabled by passing an empty middleware in options:

```javascript
options = {
  checkAuthMiddleware: (req, res, next) => {
    return next && next();
  }
};
```
