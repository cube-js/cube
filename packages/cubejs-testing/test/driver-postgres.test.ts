import { mainTestSet, multiQueryTestSet, preAggsTestSet, productionTestSet } from './driverTests/testSets';
import { executeTestSuite } from './driver-test-suite';

executeTestSuite({
  type: 'postgres',
  tests: mainTestSet,
});

executeTestSuite({
  type: 'postgres',
  tests: multiQueryTestSet,
});

executeTestSuite({
  type: 'postgres',
  tests: mainTestSet,
  config: { CUBEJS_EXTERNAL_DEFAULT: 'true' }
});

executeTestSuite({
  type: 'postgres',
  tests: preAggsTestSet,
  config: { CUBEJS_EXTERNAL_DEFAULT: 'true' }
});

executeTestSuite({
  type: 'postgres',
  tests: productionTestSet,
  config: { CUBEJS_DEV_MODE: 'false' },
});
