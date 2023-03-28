import { testConnection, testQueries, testSequence } from '../src/index';

testConnection('athena');
testSequence('athena');
testQueries('athena');
