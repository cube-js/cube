import { testQueries } from '../src/tests/testQueries';

testQueries('snowflake', {
  includeIncrementalSchemaSuite: true,
  includeHLLSuite: true,
});
