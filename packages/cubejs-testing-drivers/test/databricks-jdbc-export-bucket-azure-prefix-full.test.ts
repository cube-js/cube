import { testQueries } from '../src/tests/testQueries';

testQueries('databricks-jdbc', {
  includeHLLSuite: false,
  extendedEnv: 'export-bucket-azure-prefix'
});
