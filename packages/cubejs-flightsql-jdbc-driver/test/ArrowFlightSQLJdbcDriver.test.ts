import { GenericContainer, StartedTestContainer, Wait } from 'testcontainers';
import { JDBCDriver } from '@cubejs-backend/jdbc-driver';
import { ArrowFlightSQLJdbcDriver } from '../src';

describe('ArrowFlightSQLJdbcDriver', () => {
  let container: StartedTestContainer;
  let driver: JDBCDriver;

  jest.setTimeout(2 * 60 * 1000);

  beforeAll(async () => {
    const containerImage = await GenericContainer
      .fromDockerfile('./test', 'Dockerfile.spiceai')
      .build();
    container = await containerImage
      .withCopyFilesToContainer([{
        source: './test/spicepod.yaml',
        target: '/app/spicepod.yaml'
      }, {
        source: './test/yellow_tripdata_2024-01_499.parquet',
        target: '/app/yellow_tripdata_2024-01_499.parquet'
      }])
      .withExposedPorts(50051)
      .withWaitStrategy(Wait.forLogMessage('All components are loaded. Spice runtime is ready!'))
      .withStartupTimeout(10 * 1000)
      .start();

    driver = new ArrowFlightSQLJdbcDriver({
      url: `jdbc:arrow-flight-sql://${container.getHost()}:${container.getMappedPort(50051)}`,
      properties: {
        password: ''
      }
    });
  });

  test('testConnection', async () => {
    await driver.testConnection();
  });

  test('query', async () => {
    const count = await driver.query('select count(*) from public.yellow_taxis;', []);
    expect(count).toEqual([{ 'count(*)': '499' }]);
  });

  afterAll(async () => {
    await driver.release();
    await container.stop();
  });
});
