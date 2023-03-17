---
title: cubejs-backend-server
permalink: /config/reference/@cubejs-backend-server
category: Configuration
subCategory: Reference
menuOrder: 7
redirect_from:
  - /@cubejs-backend-server
---

`@cubejs-backend/server` is a web server for the
[@cubejs-backend/server-core][ref-config-ref-backend-server-core]. There are
also multiple options to run Cube.js Backend Server [in
production][ref-deployment].

## API Reference

### <--{"id" : "API Reference"}--> CubejsServer.create(options)

Creates an instance of `CubejsServer`.

You can set server port using `PORT` environment variable. Default port is
`4000`.

#### Example

```javascript
const CubejsServer = require('@cubejs-backend/server');

const server = new CubejsServer();

server.listen().then(({ version, port }) => {
  console.log(`🚀 Cube.js server (${version}) is listening on ${port}`);
});
```

#### Options Reference

The options for `CubejsServer` include the `CubejsServerCore`
[options][ref-config-ref-backend-server-core-opts] plus the following additional
ones specific to `CubejsServer`:

```typescript
type CubejsServerOptions = {
  webSockets?: boolean;
  initApp?(app: express.Application): void | Promise<void>;
};
```

##### webSockets

Boolean to enable or disable [web sockets][ref-frontend-real-time-ws] on the
backend. Can also be enabled using the `CUBEJS_WEB_SOCKETS` environment
variable.

##### initApp

A function to setup the instance of Express. It accepts the following argument:

- `app`: the instance of Express

This method is invoked prior to any routes having been added. Since routes can't
be overridden, this allows customization / overriding of the routes and other
aspects of the Express application early in its lifecycle.

An example usage is customizing the base route `/` in production mode to return
a 404:

`initApp.ts`

```typescript
import type { Application, Request, Response } from 'express';

export function initApp(app: Application) {
  app.get('/', (req: Request, res: Response) => {
    res.sendStatus(404);
  });
}
```

`index.ts`

```typescript
import { initApp } from './initApp';

const options = {};

// ...

if (process.env.NODE_ENV === 'production') {
  options.initApp = initApp;
}

const server = new CubejsServer(options);
```

### <--{"id" : "API Reference"}--> CubejsServer.version()

`CubejsServer.version` is a method that returns the semantic package version of
`@cubejs-backend/server`.

```javascript
const CubejsServer = require('@cubejs-backend/server');

console.log(CubejsServer.version());
```

### <--{"id" : "API Reference"}--> this.listen(options)

Instantiates the Express.js App to listen to the specified `PORT`. Returns a
promise that resolves with the following members:

- `port {number}` The port at which CubejsServer is listening for insecure
  connections for redirection to HTTPS, as specified by the environment variable
  `PORT`. Defaults to 4000.
- `app {Express.Application}` The express App powering CubejsServer
- `server {http.Server}` The `http` Server instance. If TLS is enabled, returns
  a `https.Server` instance instead.
- `version {string}` The semantic package version of `@cubejs-backend/server`

### <--{"id" : "API Reference"}--> this.testConnections()

Tests all existing open connections in the application.

### <--{"id" : "API Reference"}--> this.close()

Shuts down the server and closes any open db connections.

[ref-config-ref-backend-server-core]:
  /config/reference/@cubejs-backend-server-core
[ref-config-ref-backend-server-core-opts]:
  /config/reference/@cubejs-backend-server-core#options-reference
[ref-deployment]: /deployment/overview
[ref-frontend-real-time-ws]: /real-time-data-fetch#web-sockets
