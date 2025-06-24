import { testQueries } from '../src/tests/testQueries';

testQueries('snowflake', {
  includeHLLSuite: false,
  extendedEnv: 'export-bucket-azure-prefix'
});
