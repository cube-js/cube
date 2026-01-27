import { dbRunner } from './MySqlDbRunner';

import './custom-granularities.suite';
import './mysql-pre-aggregations.suite';

afterAll(async () => {
  await dbRunner.tearDown();
});
