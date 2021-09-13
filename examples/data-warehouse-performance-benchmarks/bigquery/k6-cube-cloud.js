import http from 'k6/http';
import { sleep } from 'k6';
import cubeQueries from './cube-queries.js';
const { generate } = cubeQueries;

export default function () {
  const cubeUrl = 'https://irish-idalia.gcp-us-central1.cubecloudapp.dev/cubejs-api/v1/load'
  const params = {
    headers: {
      'Content-Type': 'application/json',
      'Authorization': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE2Mjc5ODk5NTd9.IQoJWnqtscvFe8r-dELM0ev2Rds_Rxe2h0F7-rUpES0',
    },
  };

  const generatedData = generate.data()
  const generatedQuery = generate.query(generatedData)

  const payload = `{"query": ${generatedQuery} }`
  http.post(cubeUrl, payload, params);
  sleep(1);
}
