import { BigQuery } from '@google-cloud/bigquery';
import http from 'http';

async function queryBQ() {
  const bigqueryClient = new BigQuery();
  const sqlQuery =
  `
  SELECT
    repository_name,
    type,
    count(*) as stars
  FROM
    \`cube-devrel-team.github.events\`
  WHERE
    type = 'WatchEvent'
  GROUP BY
    1,
    2
  ORDER BY
    3 DESC
  LIMIT
    10
  `

  const options = {
    query: sqlQuery,
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
