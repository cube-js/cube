import { StartedTestContainer } from 'testcontainers';
import { CrateDBRunner, DriverTests } from '@cubejs-backend/testing-shared';
import { CrateDriver } from '../src';

describe('CrateDriver', () => {
  let db: StartedTestContainer;
  let tests: DriverTests;
  jest.setTimeout(2 * 60 * 1000);

  beforeAll(async () => {
    db = await CrateDBRunner.startContainer({ volumes: [] });
    tests = new DriverTests(
      new CrateDriver({
        host: db.getHost(),
        port: db.getMappedPort(5432),
        user: 'crate',
        password: '',
        database: 'crate',
      }),
      {}
    );
  });

  afterAll(async () => {
    await tests.release();
    await db.stop();
  });

  test('query', async () => {
    await tests.testQuery();
  });
});
