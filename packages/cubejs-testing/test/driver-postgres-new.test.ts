import { customerDimensionsAndLimitTest } from './driverTests/tests';
import { executeTestSuite } from './new-driver-test-suite';

executeTestSuite({ type: 'postgres',
  tests: [customerDimensionsAndLimitTest],
  options: {
    // schemaExtends: { Customers: { heritageCubeNameFilePath: 'postgresql/CommonCustomers', heritageCubeName: 'CommonCustomers' } }
  }
});
