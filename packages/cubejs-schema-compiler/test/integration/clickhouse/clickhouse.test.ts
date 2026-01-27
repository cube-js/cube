import { dbRunner } from './ClickHouseDbRunner';

import './clickhouse-dataschema-compiler.suite';
import './clickhouse-graph-builder.suite';
import './complex-joins.suite';
import './custom-granularities.suite';

afterAll(async () => {
  await dbRunner.tearDown();
});
