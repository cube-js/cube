import { mainTestSet, multiQueryTestSet, preAggsTestSet, productionTestSet } from './driverTests/testSets';
import { executeTestSuite } from './driver-test-suite';

executeTestSuite({
  type: 'doris',
  tests: mainTestSet,
});

executeTestSuite({
  type: 'doris',
  tests: multiQueryTestSet,
});

executeTestSuite({
  type: 'doris',
  tests: mainTestSet,
  config: { CUBEJS_EXTERNAL_DEFAULT: 'true' }
});

executeTestSuite({
  type: 'doris',
  tests: preAggsTestSet,
  config: { CUBEJS_EXTERNAL_DEFAULT: 'true' }
});

executeTestSuite({
  type: 'doris',
  tests: productionTestSet,
  config: { CUBEJS_DEV_MODE: 'false' },
});
