---
title: cubejs-backend-server
permalink: /@cubejs-backend-server
category: Cube.js Backend
subCategory: Reference
menuOrder: 6
---

`@cubejs-backend/server` is an Express server for the [@cubejs-backend/server-core](/@cubejs-backend-server-core). You can generate an app using [Cube.js CLI](/using-the-cubejs-cli). There are also multiple options to run Cube.js Backend Server [in production](/deployment).

## API Reference

### CubejsServer.create(options)

Create an instance of `CubejsServer`. `options` object is passed to [Cube.js Backend Server Core](@cubejs-backend-server-core#api-reference-cubejs-server-core-create-options).

### Example

```javascript
const CubejsServer = require('@cubejs-backend/server');

const server = new CubejsServer();

server.listen().then(({ port }) => {
  console.log(`🚀 Cube.js server is listening on ${port}`);
});
```
