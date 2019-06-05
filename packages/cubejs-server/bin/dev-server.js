#!/usr/bin/env node
const CubejsServer = require('../');

const server = new CubejsServer();

server.listen().then(({ port }) => {
  console.log(`🚀 Cube.js server is listening on ${port}`);
});
