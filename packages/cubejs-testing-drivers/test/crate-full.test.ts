import { testQueries } from '../src/tests/testQueries';

// CrateDB reuses the Postgres dialect (CrateQuery extends PostgresQuery) but does
// not implement HLL (hllInit/hllMerge are stubbed), so the HLL suite is disabled.
testQueries('crate', {
  includeIncrementalSchemaSuite: true,
});
