import { testConnection, testQueries, testSequence } from '../src/index';

testConnection('databricks-jdbc');
testSequence('databricks-jdbc');
testQueries('databricks-jdbc');
