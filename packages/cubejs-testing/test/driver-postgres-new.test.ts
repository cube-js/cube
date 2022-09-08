import { mainTestSet } from './driverTests/testSets';
import { executeTestSuite } from './new-driver-test-suite';

executeTestSuite({ type: 'postgres',
  tests: mainTestSet,
  options: {
    schemaExtends: { Customers: { heritageCubeNameFilePath: 'postgresql/CommonCustomers', heritageCubeName: 'CommonCustomers' } }
  }
});
