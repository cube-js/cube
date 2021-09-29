const { Pool } = require('pg');

const pool = new Pool({
  host: `postgres`,
  port: 5432,
  user: `postgres`,
  password: `example`,
  database: `localDB`,
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

pool.query(updatestatusQuery, (err) => {
  if (err) {
    console.log(err);
  } else {
    console.log('Order successfully updated');
  }

  pool.end();
});
