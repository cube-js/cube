import { testQueries } from '../src/tests/testQueries';

testQueries('databricks-jdbc', { includeIncrementalSchemaSuite: true });
testQueries('databricks-jdbc', { includeIncrementalSchemaSuite: true, extendedEnv: 'export-bucket' });
