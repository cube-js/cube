import { CrateRunner } from '@cubejs-backend/testing-shared';

import { StartedTestContainer } from 'testcontainers';

import { CrateDriver } from '../src';

describe('CrateDriver', () => {
  let container: StartedTestContainer;
  let driver: CrateDriver;

  jest.setTimeout(2 * 60 * 1000);

  beforeAll(async () => {
    container = await CrateRunner.startContainer({ volumes: [] });
    driver = new CrateDriver({
      host: container.getHost(),
      port: container.getMappedPort(5432),
      user: 'crate',
      password: '',
      database: 'crate',
    });
  });

  afterAll(async () => {
    await driver.release();

    if (container) {
      await container.stop();
    }
  });

  test('query', async () => {
    await driver.uploadTable(
      'query_test',
      [
        { name: 'id', type: 'int' },
        { name: 'created', type: 'timestamp' },
        { name: 'price', type: 'real' }
      ],
      {
        rows: [
          { id: 1, created: '2020-01-01T00:00:00.000Z', price: 100.5 },
          { id: 2, created: '2020-01-02T00:00:00.000Z', price: 200.5 },
          { id: 3, created: '2020-01-03T00:00:00.000Z', price: 300.5 }
        ]
      }
    );

    const tableData = await driver.query('select * from query_test', []);

    expect(tableData).toEqual([
      { id: '1', created: '2020-01-01T00:00:00.000', price: 100.5 },
      { id: '2', created: '2020-01-02T00:00:00.000', price: 200.5 },
      { id: '3', created: '2020-01-03T00:00:00.000', price: 300.5 }
    ]);
  });
});
