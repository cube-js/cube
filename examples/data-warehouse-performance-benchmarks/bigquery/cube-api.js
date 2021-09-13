import cubejs from '@cubejs-client/core';
import cubeQueries from './cube-queries.js';
const { generate } = cubeQueries;

const cubejsApi = cubejs.default(
  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE2Mjc5ODkxODh9.toFTRcl7fdfZN-4fm9XSNu4qfpCZ2X8423Jbju8WyYY',
  { apiUrl: 'https://irish-idalia.gcp-us-central1.cubecloudapp.dev/cubejs-api/v1' }
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
