import { mainTestSet } from './driverTests/testSets';
import { executeTestSuite } from './driver-test-suite';

executeTestSuite({
  type: 'databricks-jdbc',
  tests: mainTestSet,
});

executeTestSuite({
  type: 'databricks-jdbc',
  tests: mainTestSet,
  config: { CUBEJS_EXTERNAL_DEFAULT: 'true' },
});
