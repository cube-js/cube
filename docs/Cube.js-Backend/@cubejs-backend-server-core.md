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

* `options` - Options object.
    * `dbType` - Type of your database.
    * `driverFactory()` - Pass function of the driver factory with your database type.
    * `logger(msg, params)` - Pass function for your custom logger.
    * `schemaPath` - Path to the `schema` location. By default, it is `/schema`.
    * `devServer` - Enable development server. By default, it is `true`.
    * `basePath` - Path where _Cube.js_ is mounted to. By default, it is `/cubejs-api`.
    * `checkAuthMiddleware` - Pass express-style middleware to check authentication. Set `req.authInfo = { u: { ...userContextObj } }` inside middleware if you want to provide `USER_CONTEXT`. [Learn more](/cube#context-variables-user-context).

```javascript
import * as CubejsServerCore from "@cubejs-backend/server-core";
import * as express from 'express';
import * as path from 'path';

const express = express();

const dbType = 'mysql';
const options = {
  dbType,
  devServer: false,
  driverFactory: () => CubejsServerCore.createDriver(dbType),
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
