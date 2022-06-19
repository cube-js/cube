// eslint-disable-next-line import/no-extraneous-dependencies
import { StartedDockerComposeEnvironment, DockerComposeEnvironment } from 'testcontainers';
import path from 'path';

import { CockroachDriver } from '../src';

describe('CockroachDBRunner', () => {
  let env: StartedDockerComposeEnvironment | null = null;
  let driver: CockroachDriver;

  jest.setTimeout(2 * 60 * 1000);

  beforeAll(async () => {
    const dc = new DockerComposeEnvironment(
      path.resolve(path.dirname(__filename), '../../'),
      'docker-compose.yml'
    ).withEnv('CONTAINER_NAME', 'crdb')
      .withEnv('HOST_PORT', '26257');

    env = await dc.up();

    const host = env.getContainer('crdb').getHost();
    const port = env.getContainer('crdb').getMappedPort(26257);
    driver = new CockroachDriver({
      host,
      port,
      database: 'defaultdb',
      user: 'root'
    });
    await driver.query('CREATE SCHEMA IF NOT EXISTS test;', []);
  });

  afterAll(async () => {
    await driver.release();

    if (env) {
      await env.down();
    }
  });

  test('type coercion', async () => {
    await driver.query('CREATE TYPE IF NOT EXISTS CUBEJS_TEST_ENUM AS ENUM (\'FOO\');', []);

    const data = await driver.query(
      `
        SELECT
          CAST('2020-01-01' as DATE) as date,
          CAST('2020-01-01 00:00:00' as TIMESTAMP) as timestamp,
          CAST('2020-01-01 00:00:00+02' as TIMESTAMPTZ) as timestamptz,
          CAST('1.0' as DECIMAL(10,2)) as decimal,
          CAST('FOO' as CUBEJS_TEST_ENUM) as enum
      `,
      []
    );

    expect(data).toEqual([
      {
        // Date in UTC
        date: '2020-01-01T00:00:00.000',
        timestamp: '2020-01-01T00:00:00.000',
        // converted to utc
        timestamptz: '2019-12-31T22:00:00.000',
        // Numerics as string
        decimal: '1.00',
        // Enum datatypes as string
        enum: 'FOO',
      }
    ]);
  });
});
