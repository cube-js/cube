import { testQueries } from '../src/tests/testQueries';

testQueries('bigquery', {
  includeIncrementalSchemaSuite: true,
  includeHLLSuite: true,
});
