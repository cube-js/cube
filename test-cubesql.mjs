import { createRequire } from 'module';
const require = createRequire(import.meta.url);

const cubeModule = require('./packages/cubejs-client-core/dist/cubejs-client-core.cjs.js');
const cube = cubeModule.default || cubeModule;


const cubeApi = cube('eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3NTUyMTU0MzZ9.uSivm5MdO_hooPVTUw6uXVYua15EICC5125_pdTOKxY', {
  apiUrl: 'https://statistical-planarian.gcp-europe-west2-a.cubecloudapp.dev/cubejs-api/v1'
});

const sqlQuery = `SelECT   fake_view.boolean_col,   fake_view.number_col,   DATE_TRUNC('quarter', fake_view.datetime_col) FROM   fake_view GROUP BY   1,   2,   3 LIMIT   10000;`;

try {
  console.log('Executing CubeSQL query...');
  const result = await cubeApi.cubeSql(sqlQuery, {
    timeout: 10
  });

  console.log('Schema:');
  console.log(JSON.stringify(result.schema, null, 2));

  console.log('\nData (first 5 rows):');
  console.log(JSON.stringify(result.data.slice(0, 5), null, 2));

  console.log(`\nTotal rows: ${result.data.length}`);
} catch (error) {
  console.error('Error executing CubeSQL query:', error);
}
