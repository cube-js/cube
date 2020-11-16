const CubejsServerCore = require('@cubejs-backend/server-core');
const express = require('express');
const bodyParser = require('body-parser');
const path = require('path');
const http = require('http');
const serveStatic = require('serve-static');
const session = require('express-session');
const passport = require('passport');
require('dotenv').config();

const app = express();
app.use(bodyParser.json({ limit: '50mb' }));
app.use(require('cors')());

const cubejsServer = CubejsServerCore.create({
  preAggregationsSchema: 'stb_pre_aggregations',
});

app.get('/healthy', (req, res) => {
  res.json({ status: 'ok' });
});

app.use(session({ secret: process.env.CUBEJS_API_SECRET }));
app.use(passport.initialize());
app.use(passport.session());

cubejsServer.initApp(app);

const port = process.env.PORT || 4000;
const server = http.createServer({}, app);

server.listen(port, () => {
  console.log(`🚀 Cube.js server (${CubejsServerCore.version()}) is listening on ${port}`);
});
