<<<<<<< HEAD
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
  console.log(
    `🚀 Cube.js server (${CubejsServerCore.version()}) is listening on ${port}`
  );
});
=======
const importSlackArchive = require('./import');
const CubejsServer = require('@cubejs-backend/server');

(async function() {
    await importSlackArchive(process.argv[2]);

    const server = new CubejsServer();

    server.listen().then(({ version, port }) => {
        console.log(`🚀 Cube.js server (${version}) is listening on ${port}`);
    }).catch(e => {
        console.error('Fatal error during server start: ');
        console.error(e.stack || e);
    });
})();
>>>>>>> a38e028fa6cdebc631523b577568e2ce2e6d869b
