---
title: cubejs-backend-server-core
permalink: /@cubejs-backend-server-core
category: Cube.js Backend
---

`@cubejs-backend/server-core` could be used to embed Cube.js Backend into your
Express application.

## API Reference

### CubejsServerCore.create(options)

Create an instance of `CubejsServerCore` to embed it in an `Express` application.

* `options` - options object.
    * `dbType` - Type of your database.
    * `driverFactory()` - pass function of the driver factory with your database type.
    * `logger(msg, params)` - pass function for your custom logger.
    * `schemaPath` - Path to the `schema` location. By default, it is `/schema`.
    * `devServer` - Enable development server. By default, it is `true`.
    * `checkAuthMiddleware` - Pass express-style middleware to check authentication. Set `req.authInfo = { u: { ...userContextObj } }` inside middleware if you want to provide `USER_CONTEXT`. [Learn more](/cube#context-variables-user-context).

```javascript
import * as CubejsServerCore from "@cubejs-backend/server-core";
import * as express from 'express';
import * as path from 'path';

const express = express();

const dbType = 'mysql';
const config = {
  dbType,
  devServer: false,
  driverFactory: () => CubejsServerCore.createDriver(dbType),
  logger: (msg, params) => {
    console.log(`${msg}: ${JSON.stringify(params)}`);
  },
  schemaPath: path.join('assets', 'schema')
};

const core = CubejsServerCore.create(config);
await core.initApp(express);
```
