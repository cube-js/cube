import { testQueries } from '../src/tests/testQueries';
import { testIncrementalSchemaLoading } from '../src/tests/testIncrementalSchemaLoading';

// TODO fails in CI?
// testIncrementalSchemaLoading('mssql');
testQueries('mssql');
