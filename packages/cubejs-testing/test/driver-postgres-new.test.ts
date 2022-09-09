import { mainTestSet } from './driverTests/testSets';
import { executeTestSuite } from './new-driver-test-suite';

executeTestSuite({
  type: 'postgres',
  tests: mainTestSet,
  
});

executeTestSuite({
  type: 'postgres',
  tests: mainTestSet,
  config: { CUBEJS_EXTERNAL_DEFAULT: 'true' }
});
