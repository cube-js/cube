import { testQueries } from '../src/tests/testQueries';
import { testIncrementalSchemaLoading } from '../src/tests/testIncrementalSchemaLoading';

testIncrementalSchemaLoading('athena');
testQueries('athena');
