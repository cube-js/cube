import { testQueries } from '../src/tests/testQueries';

testQueries('redshift', {
  includeIncrementalSchemaSuite: true,
  externalSchemaTests: true
});
