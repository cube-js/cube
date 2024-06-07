import { testQueries } from '../src/tests/testQueries';

testQueries('databricks-jdbc', {
  includeIncrementalSchemaSuite: true,
  includeHLLSuite: true,
});
