const { Pool } = require('pg');

const pool = new Pool({
  host: process.env.CUBEJS_DB_HOST,
  port: process.env.CUBEJS_DB_PORT,
  user: process.env.CUBEJS_DB_USER,
  password: process.env.CUBEJS_DB_PASS,
  database: process.env.CUBEJS_DB_NAME
});

const statusesQuery = `
  SELECT DISTINCT status
  FROM public.orders
`;

exports.fetchStatuses = async () => {
  const client = await pool.connect();
  const result = await client.query(statusesQuery);
  client.release();

  return result.rows.map(row => row.status);
};