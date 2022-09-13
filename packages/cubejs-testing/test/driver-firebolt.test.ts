import { mainTestSet } from './driverTests/testSets';
import { executeTestSuite } from './driver-test-suite';

executeTestSuite({
  type: 'firebolt',
  tests: mainTestSet,
  
});

executeTestSuite({
  type: 'firebolt',
  tests: mainTestSet,
  config: { CUBEJS_EXTERNAL_DEFAULT: 'true' }
});
