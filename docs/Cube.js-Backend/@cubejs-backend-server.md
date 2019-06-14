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
