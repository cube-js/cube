const { Pool } = require('pg');

const pool = new Pool({
  host: process.env.CUBEJS_DB_HOST,
  port: process.env.CUBEJS_DB_PORT,
  user: process.env.CUBEJS_DB_USER,
  password: process.env.CUBEJS_DB_PASS,
  database: process.env.CUBEJS_DB_NAME,
});

const updatestatusQuery = `
  UPDATE 
    orders 
  SET 
    status = (array ['shipped', 'processing', 'completed']) [floor(random() * 3 + 1)],
    updated_at = NOW() 
  WHERE 
    id = 1;
`;

pool.query(updatestatusQuery, () => {
  console.log('ok');
  pool.end();
});

// exports.updateStatus = async () => {
//   const client = await pool.connect();

//   await client.query(updatestatusQuery);
//   pool.end();
// };
