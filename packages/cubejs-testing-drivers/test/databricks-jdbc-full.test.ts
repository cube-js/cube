import * as https from "node:https";

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
      authorization: process.env.DRIVERS_TESTS_CUBEJS_DB_DATABRICKS_TOKEN,
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

testQueries('databricks-jdbc', {
  includeIncrementalSchemaSuite: true,
  includeHLLSuite: true,
});
