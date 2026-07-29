import { testQueries } from '../src/tests/testQueries';

// NOTE: the incremental schema suite is intentionally omitted — the Oracle
// driver does not implement incremental schema loading (capabilities()
// .incrementalSchemaLoading is false).
testQueries('oracle');
