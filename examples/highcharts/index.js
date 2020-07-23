const CubejsServerCore = require('@cubejs-backend/server-core');
const WebSocketServer = require('@cubejs-backend/server/WebSocketServer');
const express = require('express');
const bodyParser = require('body-parser');
const http = require('http');
const path = require('path');
const serveStatic = require('serve-static');
require('dotenv').config();

const app = express();

app.use(bodyParser.json({ limit: '50mb' }));

if (process.env.NODE_ENV === 'production') {
  app.use(serveStatic(path.join(__dirname, 'dashboard-app/build')));
}

const cubejsServer = CubejsServerCore.create();
cubejsServer.initApp(app);
const server = http.createServer({}, app);

const socketServer = new WebSocketServer(cubejsServer);
socketServer.initServer(server);

const port = process.env.PORT || 4000;
server.listen(port, () => {
  console.log(`🚀 Cube.js server (${CubejsServerCore.version()}) is listening on ${port}`);
});
