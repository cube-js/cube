import { testQueries } from '../src/tests/testQueries';

testQueries('clickhouse', {
  // NOTICE: It's enough to turn on this flag only once for any one
  // cloud storage integration. Please do not turn it on for every integration test!
  includeIncrementalSchemaSuite: false,
  extendedEnv: 'export-bucket-s3-prefix'
});
