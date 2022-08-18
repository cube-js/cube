import pg from 'pg';
const { Pool } = pg;
import http from 'http';
import pgQueries from './pg-queries.js';
const { generate } = pgQueries;

const pool = new Pool({
  host: 'demo-db-examples.cube.dev',
  database: 'tpch',
  port: 5432,
  user: 'cube',
  password: '12345',
})

async function queryPg() {
  const generatedData = generate.data()
  const generatedQuery = generate.query(generatedData)

  await pool.query(generatedQuery);
}

const requestListener = async (req, res) => {
  await queryPg();
  res.writeHead(200);
  res.end('Query Done!');
}
const server = http.createServer(requestListener);
server.listen(8080);
