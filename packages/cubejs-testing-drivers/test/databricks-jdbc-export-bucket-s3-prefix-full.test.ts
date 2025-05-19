import { testQueries } from '../src/tests/testQueries';

testQueries('databricks-jdbc', {
  // NOTICE: It's enough to turn on this flag only once for any one
  // cloud storage integration. Please do not turn it on for every integration test!
  includeIncrementalSchemaSuite: false,
  includeHLLSuite: false,
  extendedEnv: 'export-bucket-s3-prefix'
});
