import { testQueries } from '../src/tests/testQueries';

testQueries('databricks-jdbc', {
  includeHLLSuite: true,
  extendedEnv: 'export-bucket-gcs'
});
