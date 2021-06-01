---
title: Extending Cube.js
permalink: /configuration/extending-cubejs
category: Configuration
menuOrder: 5
---

For some advanced use-cases like instrumentation, or custom authentication,
Cube.js can be extended by installing third-party Node.js packages and using
them in the [`cube.js` configuration file][ref-config].

[ref-config]: /config

## Example: sending Cube.js logs to Loggly

The example below shows how to use the Node.js Loggly client to collect and send
logs from Cube.js.

First, you'd need to install the third-party library with [NPM][link-npm]. In
our example, we're going to use [winston-loggly-bulk][link-loggly-client] to
collect and send logs to Loggly. You can install it with the following command:

[link-npm]: https://www.npmjs.com/
[link-loggly-client]: https://github.com/loggly/winston-loggly-bulk

```bash
$ npm install --save winston-loggly-bulk
```

<!-- prettier-ignore-start -->
[[info | Running Cube.js in Docker]]
| When installing custom Node.js packages for Cube.js running in Docker
| container, make sure you mount the project _including_ the
| `node_modules` subfolder:
|
| ```bash
| $ docker run -d \
|   -v ~/my-cubejs-project:/cube/conf
|   cubejs/cube
| ```
|
| If you need to use third-party Node.js packages with native extensions,
| you'll need to [build your own Docker image][ref-deployment-docker-extend].
<!-- prettier-ignore-end -->

[ref-deployment-docker-extend]:
  /deployment/platforms/docker#extend-the-docker-image

Now we can require and use `winston-loggly-bulk` library inside `cube.js`:

```javascript
const winston = require('winston');
const { loggly } = require('winston-loggly-bulk');

winston.add(
  new loggly({
    token: 'LOGGLY-TOKEN',
    subdomain: 'your-subdomain',
    tags: ['winston-nodejs'],
    json: true,
  })
);

module.exports = {
  logger: (msg, params) => {
    console.log(`${msg}: ${json.stringify(params)}`);
    winston.log('info', msg, params);
  },
};
```

## Using CubejsServer and CubejsServerCore

You can directly use `@cubejs-backend/server-core` or `@cubejs-backend/server`
Node.js packages to run Cube.js.

<!-- prettier-ignore-start -->
[[warning | ]]
| We do not recommend embedding Cube.js into existing Express application to
| share the runtime. Cube.js should be scaled very differently vs. other parts
| of Express, and embedding Cube.js **is not** the right approach in the long
| term, especially for larger deployments.
<!-- prettier-ignore-end -->

You can create an `index.js` file with the following content.

```javascript
const CubejsServer = require('@cubejs-backend/server');

const server = new CubejsServer();

server.listen().then(({ version, port }) => {
  console.log(`🚀 Cube.js server (${version}) is listening on ${port}`);
});
```

Then start Cube.js as a regular Node.js application:

```bash
node index.js
```
