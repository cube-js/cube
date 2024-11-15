import { testQueries } from '../src/tests/testQueries';

// TODO just to trigger tests
testQueries('databricks-jdbc', {
  includeHLLSuite: true,
  extendedEnv: 'export-bucket-azure'
});
