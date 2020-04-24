#!/usr/bin/env node
const CubejsServer = require('../');
const cubeVersion = require('../package.json').version;

const server = new CubejsServer();

server.listen().then(({ port }) => {
  console.log(`🚀 Cube.js server (${cubeVersion}) is listening on ${port}`);
}).catch(e => {
  console.error('Fatal error during server start: ');
  console.error(e.stack || e);
});
