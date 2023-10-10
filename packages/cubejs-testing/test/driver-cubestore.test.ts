import { createQueryTestCase, CubeStoreDBRunner, QueryTestAbstract } from '@cubejs-backend/testing-shared';
import { CubeStoreDriver } from '@cubejs-backend/cubestore-driver';
import { CubeStoreQuery } from '@cubejs-backend/schema-compiler';

class CubeStoreQueryTest extends QueryTestAbstract<CubeStoreDriver> {
  public getQueryClass(): any {
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
