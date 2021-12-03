---
title: cubejs-backend-server-core
permalink: /config/reference/@cubejs-backend-server-core
category: Configuration
subCategory: Reference
menuOrder: 6
redirect_from:
  - /@cubejs-backend-server-core
---

This package provides wiring of all essential Cube.js components and is used by
[@cubejs-backend/server][ref-config-ref-backend-server].

## API Reference

### <--{"id" : "API Reference"}--> CubejsServerCore.create(options)

`CubejsServerCore.create` is an entry point for a Cube.js server application. It
creates an instance of `CubejsServerCore`, which could be embedded for example
into Express application.

```javascript
const { CubejsServerCore } = require('@cubejs-backend/server-core');
const express = require('express');
const path = require('path');

const expressApp = express();

const dbType = 'mysql';
const options = {
  dbType,
  logger: (msg, params) => {
    console.log(`${msg}: ${JSON.stringify(params)}`);
  },
  schemaPath: path.join('assets', 'schema'),
};

const core = CubejsServerCore.create(options);
core.initApp(expressApp);
```

`CubejsServerCore.create` method accepts an object with the [Cube.js
configuration options][ref-config].

### <--{"id" : "API Reference"}--> CubejsServerCore.version()

`CubejsServerCore.version` is a method that returns the semantic package version
of `@cubejs-backend/server`.

[ref-config-ref-backend-server]: /config/reference/@cubejs-backend-server
[ref-config]: /config
