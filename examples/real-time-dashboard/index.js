const CubejsServerCore = require('@cubejs-backend/server-core');
const WebSocketServer = require('@cubejs-backend/server/WebSocketServer');
const express = require('express');
const bodyParser = require('body-parser');
const http = require('http');
const path = require('path');
const { MongoClient } = require('mongodb');
const serveStatic = require('serve-static');
require('dotenv').config();

const app = express();

app.use(require('cors')());

app.use(bodyParser.json({ limit: '50mb' }));

const cubejsServer = CubejsServerCore.create({
  orchestratorOptions: {
    queryCacheOptions: {
      refreshKeyRenewalThreshold: 1,
    }
  }
});

if (process.env.NODE_ENV === 'production') {
  app.use(serveStatic(path.join(__dirname, 'dashboard-app/build')));
}

cubejsServer.initApp(app);
const server = http.createServer({}, app);

const socketServer = new WebSocketServer(cubejsServer, { processSubscriptionsInterval: 1 });
socketServer.initServer(server);

app.post('/collect', (req, res) => {
  console.log(req.body);
  const client = new MongoClient(process.env.MONGO_URL);
  client.connect((err) => {
    const db = client.db();
    const collection = db.collection('events');
    collection.insertOne({ timestamp: new Date(), ...req.body }, ((err, result) => {
      client.close();
      res.send('ok');
    }));
  });
});

const port = process.env.PORT || 4000;
server.listen(port, () => {
  console.log(`🚀 Cube.js server (${CubejsServerCore.version()}) is listening on ${port}`);
});
