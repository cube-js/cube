import cubejs from '@cubejs-client/core';
import cubeQueries from './cube-queries.js';
const { generate } = cubeQueries;

const cubejsApi = cubejs.default(
  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE2NTQwODcyOTd9.KeVshv2ZyKZPPZ02f0Q10AcbAjScmi_JSSIrvC06_YI', 
  { apiUrl: 'https://unsightly-bovid.gcp-us-central1.cubecloudapp.dev/cubejs-api/v1' } 
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

