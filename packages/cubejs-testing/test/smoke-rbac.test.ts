// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, jest, expect } from '@jest/globals';
import { sign } from 'jsonwebtoken';
import { Client as PgClient } from 'pg';
import cubejs, { CubeApi, Query } from '@cubejs-client/core';
import { PostgresDBRunner } from '@cubejs-backend/testing-shared';
import type { StartedTestContainer } from 'testcontainers';

import { BirdBox, getBirdbox } from '../src';
import {
  DEFAULT_CONFIG,
  JEST_AFTER_ALL_DEFAULT_TIMEOUT,
  JEST_BEFORE_ALL_DEFAULT_TIMEOUT,
} from './smoke-tests';

describe('Cube RBAC Engine', () => {
  jest.setTimeout(60 * 5 * 1000);

  let birdbox: BirdBox;
  let db: StartedTestContainer;

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

  beforeAll(async () => {
    db = await PostgresDBRunner.startContainer({});
    await PostgresDBRunner.loadEcom(db);
    birdbox = await getBirdbox(
      'postgres',
      {
        ...DEFAULT_CONFIG,
        //
        CUBESQL_LOG_LEVEL: 'trace',
        //
        CUBEJS_DB_TYPE: 'postgres',
        CUBEJS_DB_HOST: db.getHost(),
        CUBEJS_DB_PORT: `${db.getMappedPort(5432)}`,
        CUBEJS_DB_NAME: 'test',
        CUBEJS_DB_USER: 'test',
        CUBEJS_DB_PASS: 'test',
        //
        CUBEJS_PG_SQL_PORT: `${pgPort}`,
        CUBESQL_SQL_PUSH_DOWN: 'true',
        CUBESQL_STREAM_MODE: 'true',
      },
      {
        schemaDir: 'rbac/model',
        cubejsConfig: 'rbac/cube.js',
      }
    );
  }, JEST_BEFORE_ALL_DEFAULT_TIMEOUT);

  afterAll(async () => {
    await birdbox.stop();
    await db.stop();
  }, JEST_AFTER_ALL_DEFAULT_TIMEOUT);

  describe('RBAC via SQL API', () => {
    let connection: PgClient;

    beforeAll(async () => {
      connection = await createPostgresClient('admin', 'admin_password');
    });

    afterAll(async () => {
      await connection.end();
    }, JEST_AFTER_ALL_DEFAULT_TIMEOUT);

    test('SELECT * from line_items', async () => {
      const res = await connection.query('SELECT * FROM line_items limit 10');
      // This query should return all rows because of the `allow_all` statement
      // It should also exclude the `created_at` dimension as per memberLevel policy
      expect(res.rows).toMatchSnapshot('line_items');
    });

    // ???
    test('SELECT * from line_items_view_no_policy', async () => {
      const res = await connection.query('SELECT * FROM line_items_view_no_policy limit 10');
      // This should query the line_items cube through the view that should
      // allow for the ommitted `created_at` dimension to be included
      expect(res.rows).toMatchSnapshot('line_items_view_no_policy');
    });

    test('SELECT * from line_items_view_price_gt_200', async () => {
      const res = await connection.query('SELECT * FROM line_items_view_price_gt_200 limit 10');
      // This query should add an extra filter by `price_dim` defined at the view level
      expect(res.rows).toMatchSnapshot('line_items_view_price_gt_200');
    });

    test('SELECT * from orders', async () => {
      let failed = false;
      try {
        // Orders cube does not expose any members so, the query should fail
        await connection.query('SELECT * FROM orders');
      } catch (e) {
        failed = true;
      }
      expect(failed).toBe(true);

      const res = await connection.query('SELECT * FROM orders_open limit 10');
      // Open version of the orders cube should return everything
      expect(res.rows).toMatchSnapshot('orders_open');
    });

    test('SELECT * from orders_view', async () => {
      const res = await connection.query('SELECT * FROM orders_view limit 10');
      // Orders cube should be visible via the view
      expect(res.rows).toMatchSnapshot('orders_view');
    });

    test('SELECT * from line_items_view_joined_orders', async () => {
      const res = await connection.query('SELECT * FROM line_items_view_joined_orders limit 10');
      // Querying the line_items cube with joined orders should take into account
      // orders row level policy and return only a few rows with select ids
      expect(res.rows).toMatchSnapshot('orders_view');
    });

    test('SELECT * from users', async () => {
      const res = await connection.query('SELECT * FROM users limit 10');
      // Querying a cube with nested filters and mixed values should not cause any issues
      expect(res.rows).toMatchSnapshot('users');
    });
  });

  describe('RBAC via REST API', () => {
    let client: CubeApi;
    let defaultClient: CubeApi;

    const ADMIN_API_TOKEN = sign({
      auth: {
        username: 'admin',
        userAttributes: {
          region: 'CA',
          city: 'Fresno',
          canHaveAdmin: true,
          minDefaultId: 10000,
        },
        roles: ['admin', 'ownder', 'hr'],
      },
    }, DEFAULT_CONFIG.CUBEJS_API_SECRET, {
      expiresIn: '2 days'
    });

    const DEFAULT_API_TOKEN = sign({
      auth: {
        username: 'nobody',
        userAttributes: {},
        roles: [],
      },
    }, DEFAULT_CONFIG.CUBEJS_API_SECRET, {
      expiresIn: '2 days'
    });

    beforeAll(async () => {
      client = cubejs(async () => ADMIN_API_TOKEN, {
        apiUrl: birdbox.configuration.apiUrl,
      });
      defaultClient = cubejs(async () => DEFAULT_API_TOKEN, {
        apiUrl: birdbox.configuration.apiUrl,
      });
    });

    test('line_items hidden created_at', async () => {
      let query: Query = {
        measures: ['line_items.count'],
        dimensions: ['line_items.created_at'],
        order: {
          'line_items.created_at': 'asc',
        },
      };
      let error = '';
      try {
        await client.load(query, {});
      } catch (e: any) {
        error = e.toString();
      }
      expect(error).toContain('You requested hidden member');
      query = {
        measures: ['line_items_view_no_policy.count'],
        dimensions: ['line_items_view_no_policy.created_at'],
        order: {
          'line_items_view_no_policy.created_at': 'asc',
        },
        limit: 10,
      };
      const result = await client.load(query, {});
      expect(result.rawData()).toMatchSnapshot('line_items_view_no_policy_rest');
    });

    test('orders_view and cube with default policy', async () => {
      let error = '';
      try {
        await defaultClient.load({
          measures: ['orders.count'],
        });
      } catch (e: any) {
        error = e.toString();
      }
      expect(error).toContain('You requested hidden member');

      let result = await defaultClient.load({
        measures: ['orders_view.count'],
        dimensions: ['orders_view.created_at'],
        order: {
          'orders_view.created_at': 'asc',
        },
      });
      // It should only return one value allowed by the default policy
      expect(result.rawData()).toMatchSnapshot('orders_view_rest');

      result = await defaultClient.load({
        measures: ['orders_open.count'],
        dimensions: ['orders_open.created_at'],
        order: {
          'orders_open.created_at': 'asc',
        },
        limit: 10
      });
      // order_open should return all values since it has no access policy
      expect(result.rawData()).toMatchSnapshot('orders_open_rest');
    });
  });
});
