import { mainTestSet } from './driverTests/testSets';
import { executeTestSuite } from './driver-test-suite';

executeTestSuite({
  type: 'questdb',
  tests: mainTestSet,
  
});

executeTestSuite({
  type: 'questdb',
  tests: mainTestSet,
  config: { CUBEJS_EXTERNAL_DEFAULT: 'true' }
});
