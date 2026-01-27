import { dbRunner } from './PostgresDBRunner';

// Pre-aggregation tests
import './pre-agg-allow-non-strict.suite';
import './pre-aggregations-alias.suite';
import './pre-aggregations-multi-stage.suite';
import './pre-aggregations-time.suite';
import './pre-aggregations.suite';

afterAll(async () => {
  await dbRunner.tearDown();
});
