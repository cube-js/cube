import { dbRunner } from './MSSqlDbRunner';

import './custom-granularities.suite';
import './mssql-cumulative-measures.suite';
import './mssql-pre-aggregations.suite';
import './mssql-ungrouped.suite';

afterAll(async () => {
  await dbRunner.tearDown();
});
