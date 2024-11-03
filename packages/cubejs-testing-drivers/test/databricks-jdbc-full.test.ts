import * as https from 'node:https';

import D from '@cubejs-backend/databricks-jdbc-driver';

import { testQueries } from '../src/tests/testQueries';

https.get(
  'https://ohio.cloud.databricks.com/sql/1.0/warehouses/',
  (res) => {
    console.log(
      'res status and headers',
      res.statusCode,
      res.statusMessage,
      res.headers
    );
    res.on('data', (chunk) => console.log('res data', chunk.toString()));
  }
);

https.get(
  'https://ohio.cloud.databricks.com/sql/1.0/warehouses/',
  {
    headers: {
      authorization: `Bearer ${process.env.DRIVERS_TESTS_CUBEJS_DB_DATABRICKS_TOKEN}`,
    }
  },
  (res) => {
    console.log(
      'res with token status and headers',
      res.statusCode,
      res.statusMessage,
      res.headers
    );
    res.on('data', (chunk) => console.log('res with token data', chunk.toString()));
  }
);

async function f() {
  const source = new D({
    url: 'jdbc:databricks://ohio.cloud.databricks.com',
    maxPoolSize: 1,
  });
  console.log('test driver constructed');
  const qRes = await source.query('SELECT 1 as foo;', []);
  console.log('qRes', qRes);
}

f()
  .then(
    () => {
      console.log('driver test query succ');
      process.exit(0);
    },
    e => {
      console.log('driver test query failed', e);
      process.exit(1);
    }
  );

// testQueries('databricks-jdbc', {
//   includeIncrementalSchemaSuite: true,
//   includeHLLSuite: true,
// });
