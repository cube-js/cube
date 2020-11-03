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
  logger: (msg, params) => {
    console.log(`${msg}: ${JSON.stringify(params)}`);
  },
  schemaPath: path.join('assets', 'schema')
};

const core = CubejsServerCore.create(options);
core.initApp(expressApp);
```

## Options Reference

Both [CubejsServerCore](@cubejs-backend-server-core) `create` method and [CubejsServer](@cubejs-backend-server) constructor accept an object with the [Cube.js configuration options](/config).

## Version

`CubejsServerCore.version` is a method that returns the semantic package version of `@cubejs-backend/server`.
