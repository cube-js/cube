import cubejs from '@cubejs-client/core';
import cubeQueries from './cube-queries.js';
const { generate } = cubeQueries;

const cubejsApi = cubejs.default(
  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE2MzE2Mjc5OTAsImV4cCI6MTYzNDIxOTk5MH0.-lzwkP76khbyq31M2fKI9YwYYkQBR0obcS4TRwuk7Tc',
  { apiUrl: 'https://forward-wrightstown.gcp-us-central1.cubecloudapp.dev/cubejs-api/v1' }
);

async function queryCube () {
  const generatedData = generate.data();
  const generatedQuery = generate.query(generatedData);
  await cubejsApi.load(generatedQuery);
}

import * as http from 'http';
const requestListener = async (req, res) => {
  await queryCube();
  res.writeHead(200);
  res.end('Query Done!');
};
const server = http.createServer(requestListener);
server.listen(9090);
