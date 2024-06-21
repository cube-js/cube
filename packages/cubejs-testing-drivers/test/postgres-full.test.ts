import { testQueries } from '../src/tests/testQueries';

testQueries('postgres', {
  includeIncrementalSchemaSuite: true,
  includeHLLSuite: true,
});
