---
title: Overview
permalink: /configuration/overview
category: Configuration
menuOrder: 1
---

Cube.js is designed to work with different configuration sources. There
are two main ways you can set configuration options; via [a configuration
file][link-config], commonly known as the `cube.js` file, and [environment
variables][link-env-vars].

[link-config]: /config
[link-env-vars]: /reference/environment-variables

## Configuration Precedence

In Cube.js, environment variables take precedence over values specified
in `cube.js`. In production scenarios, it allows changing configuration
by simply switching environment variables, which is generally safer,
faster and easier than re-deploying the codebase.

## Development Mode

Cube.js can be run in an insecure, development mode by setting the
`CUBEJS_DEV_MODE` environment variable to `true`. Putting Cube.js in
development mode does the following:

- Disables authentication checks
- Allows another log level to be set (`trace`)
- Enables the Playground on `http://localhost:4000`
- Uses `memory` instead of `redis` as the default cache/queue engine
- Logs incorrect/invalid configuration for `externalRefresh` /`waitForRenew` instead of throwing errors

## Migration from Express to Docker template

Since [`v0.23`][link-v-023-release], Cube.js CLI uses the `docker` template
instead of `express` as a default for project creation, and it is the
recommended route for production. To migrate you should move all of your
Cube.js dependencies in `package.json` to `devDependencies` and leave all of
your dependencies that you use to configure Cube.js in `dependencies`.

[link-v-023-release]: https://github.com/cube-js/cube.js/releases/tag/v0.23.0

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

You should also rename your `index.js` file to `cube.js` and replace
the `CubejsServer.create()` call with export of configuration using
`module.exports`.

For an `index.js` like the following:

```javascript
const CubejsServer = require("@cubejs-backend/server");
const fs = require("fs");
const jwt = require("jsonwebtoken");
const jwkToPem = require("jwk-to-pem");
const jwks = JSON.parse(fs.readFileSync("jwks.json"));
const _ = require("lodash");

const server = new CubejsServer({
  checkAuth: async (req, auth) => {
    const decoded = jwt.decode(auth, { complete: true });
    const jwk = _.find(jwks.keys, x => x.kid === decoded.header.kid);
    const pem = jwkToPem(jwk);
    req.authInfo = jwt.verify(auth, pem);
  },
  contextToAppId: ({ authInfo }) => `APP_${authInfo.userId}`,
  preAggregationsSchema: ({ authInfo }) => "pre_aggregations_${authInfo.userId}"
});

server
  .listen()
  .then(({ version, port }) => {
    console.log(`🚀 Cube.js server (${version}) is listening on ${port}`);
  })
  .catch((e) => {
    console.error("Fatal error during server start: ");
    console.error(e.stack || e);
  });
```

It should be renamed to `cube.js` and its' contents should look like:

```javascript
const fs = require("fs");
const jwt = require("jsonwebtoken");
const jwkToPem = require("jwk-to-pem");
const jwks = JSON.parse(fs.readFileSync("jwks.json"));
const _ = require("lodash");

module.exports = {
  checkAuth: async (req, auth) => {
    const decoded = jwt.decode(auth, { complete: true });
    const jwk = _.find(jwks.keys, x => x.kid === decoded.header.kid);
    const pem = jwkToPem(jwk);
    req.authInfo = jwt.verify(auth, pem);
  },
  contextToAppId: ({ authInfo }) => `APP_${authInfo.userId}`,
  preAggregationsSchema: ({ authInfo }) => "pre_aggregations_${authInfo.userId}"
};
```
