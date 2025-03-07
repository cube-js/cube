import { StartedTestContainer } from 'testcontainers';
import { MysqlDBRunner, PostgresDBRunner } from '@cubejs-backend/testing-shared';
import cubejs, { CubeApi } from '@cubejs-client/core';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, expect, jest } from '@jest/globals';
import { Client as PgClient } from 'pg';
import { BirdBox, getBirdbox } from '../src';
import {
  DEFAULT_API_TOKEN,
  DEFAULT_CONFIG,
  JEST_AFTER_ALL_DEFAULT_TIMEOUT,
  JEST_BEFORE_ALL_DEFAULT_TIMEOUT,
} from './smoke-tests';

// TODO: Random port?
const pgPort = 5656;
let connectionId = 0;

async function createPostgresClient(user: string, password: string) {
  connectionId++;
  const currentConnId = connectionId;

  console.debug(`[pg] new connection ${currentConnId}`);

  const conn = new PgClient({
    database: 'db',
    port: pgPort,
    host: '127.0.0.1',
    user,
    password,
    ssl: false,
  });
  conn.on('error', (err) => {
    console.log(err);
  });
  conn.on('end', () => {
    console.debug(`[pg] end ${currentConnId}`);
  });

  await conn.connect();

  return conn;
}

describe('multidb', () => {
  jest.setTimeout(60 * 5 * 1000);
  let db: StartedTestContainer;
  let db2: StartedTestContainer;
  let birdbox: BirdBox;
  let client: CubeApi;
  let connection: PgClient;

  beforeAll(async () => {
    db = await PostgresDBRunner.startContainer({});
    db2 = await MysqlDBRunner.startContainer({});

    birdbox = await getBirdbox(
      'multidb',
      {
        CUBEJS_DB_TYPE: 'postgres',

        CUBEJS_DB_HOST: db.getHost(),
        CUBEJS_DB_PORT: `${db.getMappedPort(5432)}`,
        CUBEJS_DB_NAME: 'test',
        CUBEJS_DB_USER: 'test',
        CUBEJS_DB_PASS: 'test',

        CUBEJS_DB_HOST2: db2.getHost(),
        CUBEJS_DB_PORT2: `${db2.getMappedPort(3306)}`,
        CUBEJS_DB_NAME2: 'mysql',
        CUBEJS_DB_USER2: 'root',
        CUBEJS_DB_PASS2: 'Test1test',

        CUBEJS_PG_SQL_PORT: `${pgPort}`,
        CUBESQL_SQL_PUSH_DOWN: 'true',

        ...DEFAULT_CONFIG,
      },
      {
        schemaDir: 'multidb/schema',
        cubejsConfig: 'multidb/cube.js',
      }
    );
    client = cubejs(async () => DEFAULT_API_TOKEN, {
      apiUrl: birdbox.configuration.apiUrl,
    });
    connection = await createPostgresClient('admin', 'admin_password');
  }, JEST_BEFORE_ALL_DEFAULT_TIMEOUT);

  afterAll(async () => {
    await birdbox.stop();
    await db.stop();
    await db2.stop();
  }, JEST_AFTER_ALL_DEFAULT_TIMEOUT);

  test('query', async () => {
    const response = await client.load({
      order: {
        'Products.name': 'asc'
      },
      dimensions: [
        'Products.name',
        'Suppliers.company',
      ],
    });
    expect(response.rawData()).toMatchSnapshot('query');
  });

  test('SQL pushdown queries to different data sources: Products', async () => {
    const res = await connection.query(`
  SELECT
    name
  FROM
    Products
  WHERE
    LOWER(name) = 'apples'
  GROUP BY
    1
    `);
    expect(res.rows).toMatchSnapshot();
  });

  test('SQL pushdown queries to different data sources: Suppliers', async () => {
    const res = await connection.query(`
  SELECT
    company
  FROM
    Suppliers
  WHERE
    LOWER(company) = 'fruits inc'
  GROUP BY
    1
    `);
    expect(res.rows).toMatchSnapshot();
  });
});
