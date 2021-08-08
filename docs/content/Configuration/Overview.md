---
title: Overview
permalink: /configuration/overview
category: Configuration
menuOrder: 1
---

Cube.js is designed to work with different configuration sources. There are two
ways you can set configuration options; via [a configuration file][link-config],
commonly known as the `cube.js` file, and [environment
variables][link-env-vars].

<!-- prettier-ignore-start -->
[[info |]]
| When using Docker, ensure that the `cube.js` configuration file and your
| `schema/` folder are mounted to `/cube/conf` within the Docker container.
<!-- prettier-ignore-end -->

## Configuration Precedence

In Cube.js, values specified in `cube.js` take precedence over environment
variables.

## Development Mode

Cube.js can be run in an insecure, development mode by setting the
`CUBEJS_DEV_MODE` environment variable to `true`. Putting Cube.js in development
mode does the following:

- Disables authentication checks
- Enables Cube Store in single instance mode
- Enables background refresh for in-memory cache and [scheduled
  pre-aggregations][link-scheduled-refresh]
- Allows another log level to be set (`trace`)
- Enables [Developer Playground][link-dev-playground] on `http://localhost:4000`
- Uses `memory` instead of `redis` as the default cache/queue engine
- Logs incorrect/invalid configuration for `externalRefresh` /`waitForRenew`
  instead of throwing errors

## Configuring CORS

The Cube.js REST API supports Cross-Origin Resource Sharing (CORS) for all API
requests. By default, the middleware allows requests from any origin (`*`). To
change the allowed domain, you can do the following:

```javascript
module.exports = {
  http: {
    cors: {
      origin: 'https://myapp.com',
    },
  },
};
```

Please consult the Configuration Reference [for more
options][link-config-cors-opts].

## Migrating from Express to Docker

Since [`v0.23`][link-v-023-release], Cube.js CLI uses the `docker` template
instead of `express` as a default for project creation, and it is the
recommended route for production. To migrate you should move all Cube.js
dependencies in `package.json` to `devDependencies` and leave dependencies that
you use to configure Cube.js in `dependencies`.

For example, your existing `package.json` might look something like:

```json
{
  "name": "cubejs-app",
  "version": "0.0.1",
  "private": true,
  "scripts": {
    "dev": "node index.js"
  },
  "dependencies": {
    "@cubejs-backend/postgres-driver": "^0.20.0",
    "@cubejs-backend/server": "^0.20.0",
    "jwk-to-pem": "^2.0.4"
  }
}
```

It should become:

```json
{
  "name": "cubejs-app",
  "version": "0.0.1",
  "private": true,
  "scripts": {
    "dev": "./node_modules/.bin/cubejs-server server"
  },
  "dependencies": {
    "jwk-to-pem": "^2.0.4"
  },
  "devDependencies": {
    "@cubejs-backend/postgres-driver": "^0.23.6",
    "@cubejs-backend/server": "^0.23.7"
  }
}
```

You should also rename your `index.js` file to `cube.js` and replace the
`CubejsServer.create()` call with export of configuration using
`module.exports`.

For an `index.js` like the following:

```javascript
const CubejsServer = require('@cubejs-backend/server');
const fs = require('fs');
const jwt = require('jsonwebtoken');
const jwkToPem = require('jwk-to-pem');
const jwks = JSON.parse(fs.readFileSync('jwks.json'));
const _ = require('lodash');

const server = new CubejsServer({
  checkAuth: async (req, auth) => {
    const decoded = jwt.decode(auth, { complete: true });
    const jwk = _.find(jwks.keys, (x) => x.kid === decoded.header.kid);
    const pem = jwkToPem(jwk);
    req.securityContext = jwt.verify(auth, pem);
  },
  contextToAppId: ({ securityContext }) => `APP_${securityContext.userId}`,
  preAggregationsSchema: ({ securityContext }) =>
    `pre_aggregations_${securityContext.userId}`,
});

server
  .listen()
  .then(({ version, port }) => {
    console.log(`🚀 Cube.js server (${version}) is listening on ${port}`);
  })
  .catch((e) => {
    console.error('Fatal error during server start: ');
    console.error(e.stack || e);
  });
```

It should be renamed to `cube.js` and its' contents should look like:

```javascript
const fs = require('fs');
const jwt = require('jsonwebtoken');
const jwkToPem = require('jwk-to-pem');
const jwks = JSON.parse(fs.readFileSync('jwks.json'));
const _ = require('lodash');

module.exports = {
  checkAuth: async (req, auth) => {
    const decoded = jwt.decode(auth, { complete: true });
    const jwk = _.find(jwks.keys, (x) => x.kid === decoded.header.kid);
    const pem = jwkToPem(jwk);
    req.securityContext = jwt.verify(auth, pem);
  },
  contextToAppId: ({ securityContext }) => `APP_${securityContext.userId}`,
  preAggregationsSchema: ({ securityContext }) =>
    `pre_aggregations_${securityContext.userId}`,
};
```

[link-config]: /config
[link-config-cors-opts]: /config#options-reference-http
[link-dev-playground]: /dev-tools/dev-playground
[link-env-vars]: /reference/environment-variables
[link-scheduled-refresh]:
  /schema/reference/pre-aggregations#parameters-scheduled-refresh
[link-v-023-release]: https://github.com/cube-js/cube.js/releases/tag/v0.23.0
