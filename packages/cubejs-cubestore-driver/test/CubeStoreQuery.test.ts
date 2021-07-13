import { CubeStoreDBRunner } from '@cubejs-backend/testing';
import { createQueryTestCase, QueryTestAbstract } from '@cubejs-backend/testing/dist/src/testing';
import { CubeStoreDriver, CubeStoreQuery } from '../src';

class CubeStoreQueryTest extends QueryTestAbstract<CubeStoreDriver> {
  public getQueryClass() {
    return CubeStoreQuery;
  }
}

createQueryTestCase(new CubeStoreQueryTest(), {
  name: 'CubeStore',
  connectionFactory: (container) => new CubeStoreDriver({
    host: container.getHost(),
    port: container.getMappedPort(3030)
  }),
  DbRunnerClass: CubeStoreDBRunner,
});
