const importSlackArchive = require('./import');
const CubejsServer = require('@cubejs-backend/server');

(async function () {
  await importSlackArchive(process.argv[2]);

  const server = new CubejsServer();

  server
    .listen()
    .then(({ version, port }) => {
      console.log(`🚀 Cube.js server (${version}) is listening on ${port}`);
    })
    .catch((e) => {
      console.error('Fatal error during server start: ');
      console.error(e.stack || e);
    });
})();
