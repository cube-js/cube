import { mainTestSet } from './driverTests/testSets';
import { executeTestSuite } from './driver-test-suite';

executeTestSuite({
  type: 'bigquery',
  tests: mainTestSet,
  
});

executeTestSuite({
  type: 'bigquery',
  tests: mainTestSet,
  config: { CUBEJS_EXTERNAL_DEFAULT: 'true' }
});
