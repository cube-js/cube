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

const PG_PORT = 5656;
let connectionId = 0;

const DEFAULT_API_TOKEN = sign({
  auth: {
    username: 'nobody',
    userAttributes: {},
    roles: [],
  },
}, DEFAULT_CONFIG.CUBEJS_API_SECRET, {
  expiresIn: '2 days'
});

async function createPostgresClient(user: string, password: string) {
  connectionId++;
  const currentConnId = connectionId;

  console.debug(`[pg] new connection ${currentConnId}`);

  const conn = new PgClient({
    database: 'db',
    port: PG_PORT,
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

describe('Cube RBAC Engine', () => {
  jest.setTimeout(60 * 5 * 1000);
  let db: StartedTestContainer;
  let birdbox: BirdBox;

  beforeAll(async () => {
    db = await PostgresDBRunner.startContainer({});
    await PostgresDBRunner.loadEcom(db);
    birdbox = await getBirdbox(
      'postgres',
      {
        ...DEFAULT_CONFIG,
        CUBEJS_DEV_MODE: 'false',
        NODE_ENV: 'production',
        //
        CUBEJS_DB_TYPE: 'postgres',
        CUBEJS_DB_HOST: db.getHost(),
        CUBEJS_DB_PORT: `${db.getMappedPort(5432)}`,
        CUBEJS_DB_NAME: 'test',
        CUBEJS_DB_USER: 'test',
        CUBEJS_DB_PASS: 'test',
        //
        CUBEJS_PG_SQL_PORT: `${PG_PORT}`,
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

    test('SELECT * from users_view', async () => {
      const res = await connection.query('SELECT * FROM users_view limit 10');
      // Make sure view policies are evaluated correctly in yaml schemas
      expect(res.rows).toMatchSnapshot('users_view_js');
    });
  });

  describe('RBAC via SQL API manager', () => {
    let connection: PgClient;

    beforeAll(async () => {
      connection = await createPostgresClient('manager', 'manager_password');
    });

    afterAll(async () => {
      await connection.end();
    }, JEST_AFTER_ALL_DEFAULT_TIMEOUT);

    test('SELECT * from line_items', async () => {
      const res = await connection.query('SELECT * FROM line_items limit 10');
      // This query should return rows allowed by the default policy
      // because the manager security context has a wrong city and should not match
      // two conditions defined on the manager policy
      expect(res.rows).toMatchSnapshot('line_items_manager');
    });
  });

  describe('RBAC via SQL API default policy', () => {
    let connection: PgClient;

    beforeAll(async () => {
      connection = await createPostgresClient('default', 'default_password');
    });

    afterAll(async () => {
      await connection.end();
    }, JEST_AFTER_ALL_DEFAULT_TIMEOUT);

    test('SELECT with member expressions', async () => {
      const res = await connection.query('SELECT COUNT(city) as count from "users" HAVING (COUNT(1) > 0)');
      // Pushed SQL queries should not fail
      expect(res.rows).toMatchSnapshot('users_member_expression');
    });
  });

  describe('RBAC via SQL changing users', () => {
    let connection: PgClient;

    beforeAll(async () => {
      connection = await createPostgresClient('restricted', 'restricted_password');
    });

    afterAll(async () => {
      await connection.end();
    }, JEST_AFTER_ALL_DEFAULT_TIMEOUT);

    test('Switching user should allow more members to be visible', async () => {
      const resDefault = await connection.query('SELECT * FROM line_items limit 10');
      expect(resDefault.rows).toMatchSnapshot('line_items_default');

      await connection.query('SET USER=admin');

      const resAdmin = await connection.query('SELECT * FROM line_items limit 10');
      expect(resAdmin.rows).toMatchSnapshot('line_items');
    });
  });

  /**
   * Test case for overlapping access policies with member-level and row-level filters.
   *
   * This tests the scenario where:
   * - Policy 1: group "*" with memberLevel.includes: [] (no members)
   * - Policy 2: group "developer" with memberLevel.includes: "*" and row_level filters
   *
   * The row-level filter from the developer policy SHOULD be applied when a developer
   * queries for members, because:
   *
   *   Members
   *     ^
   *     |   ┌─────────────────┐
   *     |   │    Policy 1     │  (no members, no row filter)
   *     |   │   ┌─────────────┼───────────────┐
   *     |   │   │             │               │
   *     |   └───┼─────────────┘   Policy 2    │  (all members, with row filter)
   *     |       │                             │
   *     |       └─────────────────────────────┘
   *     └──────────────────────────────────────────> Rows
   *
   * Policy 1 covers no members (empty includes), so it should not affect row filtering.
   * Policy 2 covers all members with a row filter, so the filter MUST be applied.
   */
  describe('RBAC via SQL API developer (overlapping policies)', () => {
    let connection: PgClient;
  
    beforeAll(async () => {
      connection = await createPostgresClient('developer', 'developer_password');
    });
  
    afterAll(async () => {
      await connection.end();
    }, JEST_AFTER_ALL_DEFAULT_TIMEOUT);
  
    test('SELECT * from customers should apply row-level filter', async () => {
      const res = await connection.query('SELECT * FROM customers limit 10');
      // This query should return rows filtered by city (Los Angeles, New York) from Policy 2
      // even though Policy 1 (group "*") has empty member includes
      expect(res.rows).toMatchSnapshot('customers_developer');
      expect(res.rows.length).toBeGreaterThan(0);
    });
  
    test('SELECT count from customers should apply row-level filter', async () => {
      const res = await connection.query('SELECT count FROM customers');
      // Count should reflect the filtered rows
      expect(res.rows).toMatchSnapshot('customers_developer_count');
    });
  });
  
  describe('RBAC via SQL API admin (overlapping policies - allowAll)', () => {
    let connection: PgClient;
  
    beforeAll(async () => {
      // Admin is in 'leadership' group which has allowAll
      connection = await createPostgresClient('admin', 'admin_password');
    });
  
    afterAll(async () => {
      await connection.end();
    }, JEST_AFTER_ALL_DEFAULT_TIMEOUT);
  
    test('SELECT * from customers should not apply row-level filter for admin', async () => {
      const res = await connection.query('SELECT * FROM customers limit 10');
      // Admin should see all rows without any filter (allowAll)
      expect(res.rows).toMatchSnapshot('customers_admin');
    });
  });

  /**
   * Two-dimensional policy overlap test (matches diagram in CompilerApi.ts:559-647)
   *
   * Policy 1 (role "*"): covers members a, b, id with row filter R1 (id < 500)
   * Policy 2 (role "policy2_role"): covers members b, c, id with row filter R2 (id >= 500)
   *
   *   Members
   *     ^
   *     |       ┌─────────────────────────────┐
   *   c |       │          Policy 2           │
   *     |   ┌───┼─────────────┐               │
   *   b |   │   │  (overlap)  │               │
   *     |   │   └─────────────┼───────────────┘
   *   a |   │    Policy 1     │
   *     |   └─────────────────┘
   *     └──────────────────────────────────────────> Rows
   *              R1 (id<500)   R2 (id>=500)
   */
  describe('RBAC two-dimensional policy overlap (a,b,c diagram)', () => {
    let connection: PgClient;

    beforeAll(async () => {
      // User has policy2_role, so both Policy 1 (*) and Policy 2 apply
      connection = await createPostgresClient('policy_test', 'policy_test_password');
    });

    afterAll(async () => {
      await connection.end();
    }, JEST_AFTER_ALL_DEFAULT_TIMEOUT);

    /**
     * Case 1: Query members (a, b)
     * Only Policy 1 covers ALL queried members → R1 rows visible (id < 500)
     */
    test('Case 1: Query (a, b) → Only R1 rows (id < 500)', async () => {
      const res = await connection.query(
        'SELECT member_a, member_b FROM policy_overlap_test LIMIT 1000'
      );
      // R1 filter: id < 500, so count should be exactly 499
      expect(res.rows.length).toBe(499);
    });

    /**
     * Case 2: Query members (b, c)
     * Only Policy 2 covers ALL queried members → R2 rows visible (id >= 500)
     */
    test('Case 2: Query (b, c) → Only R2 rows (id >= 500)', async () => {
      const res = await connection.query(
        'SELECT member_b, member_c FROM policy_overlap_test LIMIT 60000'
      );
      // R2 filter: id >= 500, should get rows from the upper range
      // The exact count depends on total rows in line_items table
      expect(res.rows.length).toBeGreaterThan(0);
    });

    /**
     * Case 3: Query member (b) only
     * Both policies cover member b → Union of R1 ∪ R2 rows visible (all rows)
     */
    test('Case 3: Query (b) only → R1 ∪ R2 rows (union, all rows)', async () => {
      // First get count from R1 only (querying a, b)
      const r1Res = await connection.query(
        'SELECT member_a, member_b FROM policy_overlap_test LIMIT 1000'
      );
      const r1Count = r1Res.rows.length;

      // Now get count from union (querying just b - both policies apply)
      const unionRes = await connection.query(
        'SELECT member_b FROM policy_overlap_test LIMIT 60000'
      );
      const unionCount = unionRes.rows.length;

      // Union should return more rows than R1 alone
      expect(unionCount).toBeGreaterThan(r1Count);
    });

    /**
     * Case 4: Query members (a, b, c)
     * Neither policy covers ALL three → NO rows visible (denied)
     */
    test('Case 4: Query (a, b, c) → Access denied (empty result)', async () => {
      const res = await connection.query(
        'SELECT member_a, member_b, member_c FROM policy_overlap_test LIMIT 10'
      );
      // No policy covers all three members, so access is denied
      expect(res.rows.length).toBe(0);
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

    beforeAll(async () => {
      client = cubejs(async () => ADMIN_API_TOKEN, {
        apiUrl: birdbox.configuration.apiUrl,
      });
      defaultClient = cubejs(async () => DEFAULT_API_TOKEN, {
        apiUrl: birdbox.configuration.apiUrl,
      });
    });

    test('line_items hidden price_dim', async () => {
      // When querying hidden members, row-level security denies access
      // by filtering out all rows (returns empty result)
      // TODO we should evaluate member access before the query runs and bounce early with an error
      let query: Query = {
        measures: ['line_items.count'],
        dimensions: ['line_items.price_dim'],
        order: {
          'line_items.price_dim': 'asc',
        },
      };
      const hiddenMemberResult = await client.load(query, {});
      // Row-level security denies access by returning empty results
      expect(hiddenMemberResult.rawData()).toEqual([]);

      query = {
        measures: ['line_items_view_no_policy.count'],
        dimensions: ['line_items_view_no_policy.price_dim'],
        order: {
          'line_items_view_no_policy.price_dim': 'asc',
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

describe('Cube RBAC Engine [dev mode]', () => {
  jest.setTimeout(60 * 5 * 1000);
  let db: StartedTestContainer;
  let birdbox: BirdBox;
  let client: CubeApi;

  const pgPort = 5656;

  beforeAll(async () => {
    db = await PostgresDBRunner.startContainer({});
    await PostgresDBRunner.loadEcom(db);
    birdbox = await getBirdbox(
      'postgres',
      {
        ...DEFAULT_CONFIG,
        CUBEJS_DEV_MODE: 'true',
        NODE_ENV: 'dev',
        //
        CUBEJS_DB_TYPE: 'postgres',
        CUBEJS_DB_HOST: db.getHost(),
        CUBEJS_DB_PORT: `${db.getMappedPort(5432)}`,
        CUBEJS_DB_NAME: 'test',
        CUBEJS_DB_USER: 'test',
        CUBEJS_DB_PASS: 'test',
        //
        CUBEJS_PG_SQL_PORT: `${pgPort}`,
      },
      {
        schemaDir: 'rbac/model',
        cubejsConfig: 'rbac/cube.js',
      }
    );
    client = cubejs(async () => DEFAULT_API_TOKEN, {
      apiUrl: birdbox.configuration.apiUrl,
    });
  }, JEST_BEFORE_ALL_DEFAULT_TIMEOUT);

  afterAll(async () => {
    await birdbox.stop();
    await db.stop();
  }, JEST_AFTER_ALL_DEFAULT_TIMEOUT);

  test('line_items hidden created_at', async () => {
    const meta = await client.meta();
    const dimensions = meta.meta.cubes.find(c => c.name === 'orders')?.dimensions;
    expect(dimensions?.length).toBe(2);
    for (const dim of dimensions || []) {
      expect(dim.isVisible).toBe(false);
      expect(dim.public).toBe(false);
    }
  });

  test('products with no matching policy', async () => {
    const result = await client.load({
      measures: ['products.count'],
    });

    // Querying a cube with no matching access policy should return no data
    expect(result.rawData()).toMatchSnapshot('products_no_policy');
  });
});

describe('Cube RBAC Engine [Python config]', () => {
  jest.setTimeout(60 * 5 * 1000);
  let db: StartedTestContainer;
  let birdbox: BirdBox;

  beforeAll(async () => {
    db = await PostgresDBRunner.startContainer({});
    await PostgresDBRunner.loadEcom(db);
    birdbox = await getBirdbox(
      'postgres',
      {
        ...DEFAULT_CONFIG,
        CUBEJS_DEV_MODE: 'false',
        NODE_ENV: 'production',
        //
        CUBEJS_DB_TYPE: 'postgres',
        CUBEJS_DB_HOST: db.getHost(),
        CUBEJS_DB_PORT: `${db.getMappedPort(5432)}`,
        CUBEJS_DB_NAME: 'test',
        CUBEJS_DB_USER: 'test',
        CUBEJS_DB_PASS: 'test',
        //
        CUBEJS_PG_SQL_PORT: `${PG_PORT}`,
      },
      {
        schemaDir: 'rbac-python/model',
        cubejsConfig: 'rbac-python/cube.py',
      }
    );
  }, JEST_BEFORE_ALL_DEFAULT_TIMEOUT);

  afterAll(async () => {
    await birdbox.stop();
    await db.stop();
  }, JEST_AFTER_ALL_DEFAULT_TIMEOUT);

  describe('RBAC via SQL API [python config]', () => {
    let connection: PgClient;

    beforeAll(async () => {
      connection = await createPostgresClient('admin', 'admin_password');
    });

    afterAll(async () => {
      await connection.end();
    }, JEST_AFTER_ALL_DEFAULT_TIMEOUT);

    test('SELECT * from users', async () => {
      const res = await connection.query('SELECT COUNT(city) as count from "users" HAVING (COUNT(1) > 0)');
      // const res = await connection.query('SELECT * FROM users limit 10');
      // This query should return all rows because of the `allow_all` statement
      // It should also exclude the `created_at` dimension as per memberLevel policy
      expect(res.rows).toMatchSnapshot('users_python');
    });

    test('SELECT * from users_view', async () => {
      const res = await connection.query('SELECT * FROM users_view limit 10');
      // Make sure view policies are evaluated correctly in yaml schemas
      expect(res.rows).toMatchSnapshot('users_view_python');
    });
  });
});

describe('Cube RBAC Engine [Python config][dev mode]', () => {
  jest.setTimeout(60 * 5 * 1000);
  let db: StartedTestContainer;
  let birdbox: BirdBox;
  let client: CubeApi;

  beforeAll(async () => {
    db = await PostgresDBRunner.startContainer({});
    await PostgresDBRunner.loadEcom(db);
    birdbox = await getBirdbox(
      'postgres',
      {
        ...DEFAULT_CONFIG,
        CUBEJS_DEV_MODE: 'true',
        NODE_ENV: 'dev',
        //
        CUBEJS_DB_TYPE: 'postgres',
        CUBEJS_DB_HOST: db.getHost(),
        CUBEJS_DB_PORT: `${db.getMappedPort(5432)}`,
        CUBEJS_DB_NAME: 'test',
        CUBEJS_DB_USER: 'test',
        CUBEJS_DB_PASS: 'test',
        //
        CUBEJS_PG_SQL_PORT: `${PG_PORT}`,
      },
      {
        schemaDir: 'rbac-python/model',
        cubejsConfig: 'rbac-python/cube.py',
      }
    );
    client = cubejs(async () => DEFAULT_API_TOKEN, {
      apiUrl: birdbox.configuration.apiUrl,
    });
  }, JEST_BEFORE_ALL_DEFAULT_TIMEOUT);

  afterAll(async () => {
    await birdbox.stop();
    await db.stop();
  }, JEST_AFTER_ALL_DEFAULT_TIMEOUT);

  test('products with no matching policy', async () => {
    const result = await client.load({
      measures: ['products.count'],
    });

    // Querying a cube with no matching access policy should return no data
    expect(result.rawData()).toMatchSnapshot('products_no_policy_python');
  });
});
