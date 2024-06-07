// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, jest, expect } from '@jest/globals';
import { Client as PgClient } from 'pg';
import { PostgresDBRunner } from '@cubejs-backend/testing-shared';
import jwt from 'jsonwebtoken';
import type { StartedTestContainer } from 'testcontainers';

import fetch from 'node-fetch';
import { BirdBox, getBirdbox } from '../src';
import {
  DEFAULT_CONFIG,
  JEST_AFTER_ALL_DEFAULT_TIMEOUT,
  JEST_BEFORE_ALL_DEFAULT_TIMEOUT,
} from './smoke-tests';

describe('SQL API', () => {
  jest.setTimeout(60 * 5 * 1000);

  let connection: PgClient;
  let birdbox: BirdBox;
  let db: StartedTestContainer;

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
      host: 'localhost',
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
        schemaDir: 'postgresql/schema',
        cubejsConfig: 'postgresql/single/sqlapi.js',
      }
    );
    connection = await createPostgresClient('admin', 'admin_password');
  }, JEST_BEFORE_ALL_DEFAULT_TIMEOUT);

  afterAll(async () => {
    await connection.end();
    await birdbox.stop();
    await db.stop();
  }, JEST_AFTER_ALL_DEFAULT_TIMEOUT);

  describe('Cube SQL over HTTP', () => {
    const token = jwt.sign(
      {
        user: 'admin',
      },
      DEFAULT_CONFIG.CUBEJS_API_SECRET,
      {
        expiresIn: '1h',
      }
    );

    it('streams data', async () => {
      const ROWS_LIMIT = 40;
      const response = await fetch(`${birdbox.configuration.apiUrl}/cubesql`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: token,
        },
        body: JSON.stringify({
          query: `SELECT orderDate FROM ECommerce LIMIT ${ROWS_LIMIT};`,
        }),
      });

      const reader = response.body;
      let isFirstChunk = true;

      let data = '';
      const execute = () => new Promise<void>((resolve, reject) => {
        const onData = jest.fn((chunk: Buffer) => {
          if (isFirstChunk) {
            isFirstChunk = false;
            expect(JSON.parse(chunk.toString()).schema).toEqual([
              {
                name: 'orderDate',
                column_type: 'String',
              },
            ]);
          } else {
            data += chunk.toString('utf-8');
          }
        });
        reader.on('data', onData);

        const onError = jest.fn(() => reject(new Error('Stream error')));
        reader.on('error', onError);

        const onEnd = jest.fn(() => {
          resolve();
        });

        reader.on('end', onEnd);
      });

      await execute();
      const rows = data
        .split('\n')
        .filter((it) => it.trim())
        .map((it) => JSON.parse(it).data.length)
        .reduce((a, b) => a + b, 0);

      expect(rows).toBe(ROWS_LIMIT);
    });
  });

  describe('Postgres (Auth)', () => {
    test('Success Admin', async () => {
      const conn = await createPostgresClient('admin', 'admin_password');

      try {
        const res = await conn.query(
          'SELECT "user", "uid" FROM SecurityContextTest'
        );
        expect(res.rows).toEqual([
          {
            user: 'admin',
            uid: '1',
          },
        ]);
      } finally {
        await conn.end();
      }
    });

    test('Error Admin Password', async () => {
      try {
        await createPostgresClient('admin', 'wrong_password');

        throw new Error('Code must thrown auth error, something wrong...');
      } catch (e: any) {
        expect(e.message).toContain(
          'password authentication failed for user "admin"'
        );
      }
    });

    test('Security Context (Admin -> Moderator) - allowed superuser', async () => {
      const conn = await createPostgresClient('admin', 'admin_password');

      try {
        const res = await conn.query(
          'SELECT "user", "uid" FROM SecurityContextTest WHERE __user = \'moderator\''
        );
        expect(res.rows).toEqual([
          {
            user: 'moderator',
            uid: '2',
          },
        ]);
      } finally {
        await conn.end();
      }
    });

    test('Security Context (Moderator -> Usr1) - allowed sqlCanChangeUser', async () => {
      const conn = await createPostgresClient(
        'moderator',
        'moderator_password'
      );

      try {
        const res = await conn.query(
          'SELECT "user", "uid" FROM SecurityContextTest WHERE __user = \'usr1\''
        );
        expect(res.rows).toEqual([
          {
            user: 'usr1',
            uid: '3',
          },
        ]);
      } finally {
        await conn.end();
      }
    });

    test('Security Context (Moderator -> Usr2) - not allowed', async () => {
      const conn = await createPostgresClient(
        'moderator',
        'moderator_password'
      );

      try {
        await conn.query(
          'SELECT "user", "uid" FROM SecurityContextTest WHERE __user = \'usr2\''
        );

        throw new Error('Code must thrown auth error, something wrong...');
      } catch (e: any) {
        expect(e.message).toContain(
          'You cannot change security context via __user from moderator to usr2, because it\'s not allowed'
        );
      } finally {
        await conn.end();
      }
    });

    test('Security Context (Usr1 -> Moderator) - not allowed', async () => {
      const conn = await createPostgresClient('usr1', 'user1_password');

      try {
        await conn.query(
          'SELECT "user", "uid" FROM SecurityContextTest WHERE __user = \'moderator\''
        );

        throw new Error('Code must thrown auth error, something wrong...');
      } catch (e: any) {
        expect(e.message).toContain(
          'You cannot change security context via __user from usr1 to moderator, because it\'s not allowed'
        );
      } finally {
        await conn.end();
      }
    });
  });

  describe('Postgres (Data)', () => {
    test('SELECT COUNT(*) as cn, "status" FROM Orders GROUP BY 2 ORDER BY cn DESC', async () => {
      const res = await connection.query(
        'SELECT COUNT(*) as cn, "status" FROM Orders GROUP BY 2 ORDER BY cn DESC'
      );
      expect(res.rows).toMatchSnapshot('sql_orders');
    });

    test('powerbi min max push down', async () => {
      const res = await connection.query(`
        select
    max("rows"."createdAt") as "a0",
    min("rows"."createdAt") as "a1"
  from
    (
      select
        "createdAt"
      from
        "public"."Orders" "$Table"
    ) "rows"
    `);
      expect(res.rows).toMatchSnapshot('powerbi_min_max_push_down');
    });

    test('no limit for non matching count push down', async () => {
      const res = await connection.query(`
      select
        max("rows"."createdAt") as "a0",
        min("rows"."createdAt") as "a1",
        count(*) as "a2"
      from
        "public"."BigOrders" "rows"
    `);
      expect(res.rows).toMatchSnapshot(
        'no limit for non matching count push down'
      );
    });

    test('metabase max number', async () => {
      const res = await connection.query(`
  SELECT
    "source"."id" AS "id",
    "source"."status" AS "status",
    "source"."pivot-grouping" AS "pivot-grouping",
    MAX("source"."numberTotal") AS "numberTotal"
  FROM
    (
      SELECT
        "public"."Orders"."numberTotal" AS "numberTotal",
        "public"."Orders"."id" AS "id",
        "public"."Orders"."status" AS "status",
        ABS(0) AS "pivot-grouping"
      FROM
        "public"."Orders"
      WHERE
        "public"."Orders"."status" = 'new'
    ) AS "source"
  GROUP BY
    "source"."id",
    "source"."status",
    "source"."pivot-grouping"
  ORDER BY
    "source"."id" DESC,
    "source"."status" ASC,
    "source"."pivot-grouping" ASC
    `);
      expect(res.rows).toMatchSnapshot('metabase max number');
    });

    test('power bi post aggregate measure wrap', async () => {
      const res = await connection.query(`
  select
    "_"."createdAt",
    "_"."a0",
    "_"."a1"
  from
    (
      select
        "rows"."createdAt" as "createdAt",
        sum(cast("rows"."amountRankView" as decimal)) as "a0",
        max("rows"."amountRankDate") as "a1"
      from
        (
          select
            "_"."status",
            "_"."createdAt",
            "_"."amountRankView",
            "_"."amountRankDate"
          from
            "public"."Orders" "_"
          where
            "_"."status" = 'shipped'
        ) "rows"
      group by
        "createdAt"
    ) "_"
  where
    not "_"."a0" is null or
    not "_"."a1" is null
  limit
    1000001
    `);
      expect(res.rows).toMatchSnapshot('power bi post aggregate measure wrap');
    });

    test('percentage of total sum', async () => {
      const res = await connection.query(`
  select
    sum("OrdersView"."statusPercentageOfTotal") as "m0"
  from
    "OrdersView" as "OrdersView"
    `);
      expect(res.rows).toMatchSnapshot('percentage of total sum');
    });

    test('date/string measures in view', async () => {
      const queryCtor = (column: string) => `SELECT "${column}" AS val FROM "OrdersView" ORDER BY "id" LIMIT 10`;

      const resStr = await connection.query(queryCtor('countAndTotalAmount'));
      expect(resStr.rows).toMatchSnapshot('string case');

      const resDate = await connection.query(queryCtor('createdAtMaxProxy'));
      expect(resDate.rows).toMatchSnapshot('date case');
    });

    test('zero limited dimension aggregated queries', async () => {
      const query = 'SELECT MAX(createdAt) FROM Orders LIMIT 0';
      const res = await connection.query(query);
      expect(res.rows).toEqual([]);
    });

    test('select dimension agg where false', async () => {
      const query =
          'SELECT MAX("createdAt") AS "max" FROM "BigOrders" WHERE 1 = 0';
      const res = await connection.query(query);
      expect(res.rows).toEqual([{ max: null }]);
    });

    test('where segment is false', async () => {
      const query =
        'SELECT value AS val FROM "SegmentTest" WHERE segment_eq_1 IS FALSE ORDER BY value;';
      const res = await connection.query(query);
      expect(res.rows.map((x) => x.val)).toEqual([789, 987]);
    });
  });
});
