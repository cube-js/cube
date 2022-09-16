import { mainTestSet, databricksTestSet } from './driverTests/testSets';
import { executeTestSuite } from './driver-test-suite';

executeTestSuite({
  type: 'databricks-jdbc',
  tests: databricksTestSet,
});

executeTestSuite({
  type: 'databricks-jdbc',
  tests: databricksTestSet,
  config: { CUBEJS_EXTERNAL_DEFAULT: 'true' },
});
