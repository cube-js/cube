import { PostgresDBRunner } from '@cubejs-backend/testing-shared';
import { StartedTestContainer } from 'testcontainers';
import { PostgresDriver } from '../src';

// https://github.com/cube-js/cube/issues/8337
// Materialized views live in pg_matviews, not information_schema.columns, so tablesSchema()
// (used by the Playground's "Data Model > Tables" list and scaffolding) never returns them.
// Reproduced end to end on cubejs/cube v1.8.22 via /playground/db-schema.
describe('issue #8337: materialized views in tablesSchema()', () => {
  let container: StartedTestContainer;
  let driver: PostgresDriver;

  jest.setTimeout(2 * 60 * 1000);

  beforeAll(async () => {
    container = await PostgresDBRunner.startContainer({ volumes: [] });
    driver = new PostgresDriver({
      host: container.getHost(),
      port: container.getMappedPort(5432),
      user: 'test',
      password: 'test',
      database: 'test',
    });
    await driver.query('CREATE SCHEMA IF NOT EXISTS issue_8337', []);
    await driver.query('CREATE TABLE issue_8337.orders (id INT, status TEXT, amount INT)', []);
    await driver.query(
      'CREATE MATERIALIZED VIEW issue_8337.orders_by_status AS SELECT status, SUM(amount) AS amount FROM issue_8337.orders GROUP BY 1',
      []
    );
  });

  afterAll(async () => {
    await driver.release();
    await container.stop();
  });

  test('lists materialized views alongside tables', async () => {
    const schema = await driver.tablesSchema();

    expect(Object.keys(schema.issue_8337).sort()).toEqual(['orders', 'orders_by_status']);
    expect(schema.issue_8337.orders_by_status.map((c: { name: string }) => c.name).sort()).toEqual(['amount', 'status']);
  });
});
