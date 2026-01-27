import { dbRunner } from './PostgresDBRunner';

// Miscellaneous feature tests
import './bucketing.suite';
import './calc-groups.suite';
import './calendars.suite';
import './custom-granularities.suite';
import './multi-fact-join.suite';
import './multi-stage.suite';
import './multiple-join-paths.suite';
import './sub-query-dimensions.suite';

afterAll(async () => {
  await dbRunner.tearDown();
});
