const CubejsServer = require('@cubejs-backend/server');
const PostgresDriver = require('@cubejs-backend/postgres-driver');
const BigQueryDriver = require('@cubejs-backend/bigquery-driver');
const CubejsServerCore = require('@cubejs-backend/server-core');
const express = require('express');
const bodyParser = require("body-parser");
const path = require("path");
const http = require("http");
const serveStatic = require('serve-static');
require('dotenv').config();

const app = express();
app.use(bodyParser.json({ limit: "50mb" }));
app.use(require('cors')());

const cubejsServer = CubejsServerCore.create();

if (process.env.NODE_ENV === 'production') {
  app.use(serveStatic(path.join(__dirname, 'dashboard-app/build')));
}

app.get('/healthy', (req, res) => {
  res.json({ status: 'ok' });
});

cubejsServer.initApp(app);

const port = process.env.PORT || 4000;
const server = http.createServer({}, app);

server.listen(port, () => {
  console.log(`🚀 Cube.js server (${CubejsServerCore.version()}) is listening on ${port}`);
});
