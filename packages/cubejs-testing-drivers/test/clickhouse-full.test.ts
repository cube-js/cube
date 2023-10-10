import { testQueries } from '../src/tests/testQueries';
import { testIncrementalSchemaLoading } from '../src/tests/testIncrementalSchemaLoading';

testIncrementalSchemaLoading('clickhouse');
testQueries('clickhouse');
