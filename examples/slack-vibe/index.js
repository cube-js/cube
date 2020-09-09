require('dotenv').config();
const CubejsServerCore = require('@cubejs-backend/server-core');
const express = require('express');
const fileUpload = require('express-fileupload');
const bodyParser = require('body-parser');
const cors = require('cors');
const path = require('path');
const http = require('http');
const serveStatic = require('serve-static');
const { tryInitDatabase, tryImportSlackArchive } = require('./import');

(async function run() {
  await tryInitDatabase();

  const app = express();
  app.use(fileUpload({
    limits: { fileSize: 50 * 1024 * 1024 },
    useTempFiles: true,
    tempFileDir: '/tmp/',
  }));
  app.use(bodyParser.json({ limit: '50mb' }));
  app.use(cors());

  const cubejsServer = CubejsServerCore.create();

  if (process.env.NODE_ENV === 'production') {
    app.use(serveStatic(path.join(__dirname, './frontend/build')));
  }

  app.get('/healthy', (req, res) => {
    res.json({ status: 'ok' });
  });

  app.post('/upload', async (req, res) => {
    if (!req.files || Object.keys(req.files).length === 0) {
      return res.status(400).send('No files were uploaded.');
    }

    await tryImportSlackArchive(req.files.file.tempFilePath, () => {
      res.send('File uploaded!');
    });

    res.status(200);
  });

  cubejsServer.initApp(app);

  const port = process.env.PORT || 4000;
  const server = http.createServer({}, app);

  server.listen(port, () => {
    console.log(
      `🚀 Cube.js server (${CubejsServerCore.version()}) is listening on ${port}`
    );
  });
}());
