import { dbRunner } from './PostgresDBRunner';

// Views and join order tests
import './cube-views.suite';
import './view-deduplication.suite';
import './views-join-order-2.suite';
import './views-join-order-3.suite';
import './views-join-order-join-maps.suite';
import './views-join-order.suite';
import './views.suite';

afterAll(async () => {
  await dbRunner.tearDown();
});
