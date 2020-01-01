---
title: cubejs-backend-server
permalink: /@cubejs-backend-server
category: Cube.js Backend
subCategory: Reference
menuOrder: 7
---

`@cubejs-backend/server` is an Express server for the [@cubejs-backend/server-core](/@cubejs-backend-server-core). You can generate an app using [Cube.js CLI](/using-the-cubejs-cli). There are also multiple options to run Cube.js Backend Server [in production](/deployment).

## API Reference

### CubejsServer.create(options)

Creates an instance of `CubejsServer`.

[Here you can find a full reference for a configuration options object](@cubejs-backend-server-core#options-reference).

You can set server port using `PORT` environment variable. Default port is `4000`.

### Example

```javascript
const CubejsServer = require('@cubejs-backend/server');

const server = new CubejsServer();

server.listen().then(({ port }) => {
  console.log(`🚀 Cube.js server is listening on ${port}`);
});
```

### this.listen([options])

Instantiates the Express.js App to listen to the specified `PORT`. Returns a promise that resolves with the following members:

* `port {number}` The port at which CubejsServer is listening for insecure connections for redirection to HTTPS, as specified by the environment variable `PORT`. Defaults to 4000.
* `app {Express.Application}` The express App powering CubejsServer
* `server {http.Server}` The `http` Server instance. If TLS is enabled, returns a `https.Server` instance instead.

Cube.js can also support TLS encryption. See the [Security page on how to enable tls](security#enabling-tls) for more information.

### this.testConnections()

Tests all existing open connections in the application.  Can be used for healthchecks by implementing custom methods, or extending the server with other packages such as [@godaddy/terminus](https://github.com/godaddy/terminus).

```javascript
const CubejsServer = require('@cubejs-backend/server');
const { createTerminus } = require('@godaddy/terminus');

const cubejsServer = new CubejsServer();

cubejsServer.listen().then(({ port, server }) => {
  console.log(`🚀 Cube.js server is listening on ${port}`);

    createTerminus(server, {
    healthChecks: {
      '/ready': () => cubejsServer.testConnections()
    },
    onSignal: () => cubejsServer.close()
  });
});
```

### this.close()

Shuts down the server and closes any open db connections.
