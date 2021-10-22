import { BigQuery } from '@google-cloud/bigquery';
import http from 'http';
import bqQueries from './bq-queries.js';
const { generate } = bqQueries;

async function queryBQ() {
  const bigqueryClient = new BigQuery();

  const generatedData = generate.data()
  const generatedQuery = generate.query(generatedData)

  const options = {
    query: generatedQuery,
    location: 'US',
  };

  await bigqueryClient.query(options);
}

const requestListener = async (req, res) => {
  await queryBQ();
  res.writeHead(200);
  res.end('Query Done!');
}
const server = http.createServer(requestListener);
server.listen(8080);
