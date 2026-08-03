import { testQueries } from '../src/tests/testQueries';

testQueries('snowflake', {
  includeIncrementalSchemaSuite: false,
  includeHLLSuite: false,
  extendedEnv: 'encrypted-pk',
});
