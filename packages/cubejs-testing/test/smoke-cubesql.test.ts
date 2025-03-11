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
                column_type: 'Timestamp',
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

    describe('sql4sql', () => {
      async function generateSql(query: string) {
        const response = await fetch(`${birdbox.configuration.apiUrl}/sql`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            Authorization: token,
          },
          body: JSON.stringify({
            query,
            format: 'sql',
          }),
        });
        const { status, statusText, headers } = response;
        const body = await response.json();

        // To stabilize responses
        delete body.requestId;
        headers.delete('date');
        headers.delete('etag');

        return {
          status,
          statusText,
          headers,
          body,
        };
      }

      it('regular query', async () => {
        expect(await generateSql(`SELECT SUM(totalAmount) AS total FROM Orders;`)).toMatchSnapshot();
      });

      it('regular query with missing column', async () => {
        expect(await generateSql(`SELECT SUM(foobar) AS total FROM Orders;`)).toMatchSnapshot();
      });

      it('regular query with parameters', async () => {
        expect(await generateSql(`SELECT SUM(totalAmount) AS total FROM Orders WHERE status = 'foo';`)).toMatchSnapshot();
      });

      it('strictly post-processing', async () => {
        expect(await generateSql(`SELECT version();`)).toMatchSnapshot();
      });

      it('double aggregation post-processing', async () => {
        expect(await generateSql(`
          SELECT AVG(total)
          FROM (
            SELECT
              status,
              SUM(totalAmount) AS total
            FROM Orders
            GROUP BY 1
          ) t
        `)).toMatchSnapshot();
      });

      it('wrapper', async () => {
        expect(await generateSql(`
          SELECT
            SUM(totalAmount) AS total
          FROM Orders
          WHERE LOWER(status) = UPPER(status)
        `)).toMatchSnapshot();
      });

      it('wrapper with parameters', async () => {
        expect(await generateSql(`
          SELECT
            SUM(totalAmount) AS total
          FROM Orders
          WHERE LOWER(status) = 'foo'
        `)).toMatchSnapshot();
      });

      it('set variable', async () => {
        expect(await generateSql(`
          SET MyVariable = 'Foo'
        `)).toMatchSnapshot();
      });
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

    test('power bi multi stage measure wrap', async () => {
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

    test('zero limited dimension aggregated queries through wrapper', async () => {
      // Attempts to trigger query generation from SQL templates, not from Cube
      const query = 'SELECT MIN(t.maxval) FROM (SELECT MAX(createdAt) as maxval FROM Orders LIMIT 10) t LIMIT 0';
      const res = await connection.query(query);
      expect(res.rows).toEqual([]);
    });

    test('select dimension agg where false', async () => {
      const query =
          'SELECT MAX("createdAt") AS "max" FROM "BigOrders" WHERE 1 = 0';
      const res = await connection.query(query);
      expect(res.rows).toEqual([{ max: null }]);
    });

    test('select __user and literal grouped', async () => {
      const query = `
        SELECT
          status AS my_status,
          date_trunc('month', createdAt) AS my_created_at,
          __user AS my_user,
          1 AS my_literal,
          -- Columns without aliases should also work
          id,
          date_trunc('day', createdAt),
          __cubeJoinField,
          2
        FROM
          Orders
        GROUP BY 1,2,3,4,5,6,7,8
        ORDER BY 1,2,3,4,5,6,7,8
      `;

      const res = await connection.query(query);
      expect(res.rows).toMatchSnapshot('select __user and literal');
    });

    test('select __user and literal grouped under wrapper', async () => {
      const query = `
        WITH
-- This subquery should be represented as CubeScan(ungrouped=false) inside CubeScanWrapper
cube_scan_subq AS (
  SELECT
    status AS my_status,
    date_trunc('month', createdAt) AS my_created_at,
    __user AS my_user,
    1 AS my_literal,
    -- Columns without aliases should also work
    id,
    date_trunc('day', createdAt),
    __cubeJoinField,
    2
  FROM Orders
  GROUP BY 1,2,3,4,5,6,7,8
),
filter_subq AS (
  SELECT
    status status_filter
  FROM Orders
  GROUP BY
    status_filter
)
        SELECT
          -- Should use SELECT * here to reference columns without aliases.
          -- But it's broken ATM in DF, initial plan contains \`Projection: ... #__subquery-0.logs_content_filter\` on top, but it should not be there
          -- TODO fix it
          my_created_at,
          my_status,
          my_user,
          my_literal
        FROM cube_scan_subq
        WHERE
          -- This subquery filter should trigger wrapping of whole query
          my_status IN (
            SELECT
              status_filter
            FROM filter_subq
          )
        GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4
        ;
        `;

      const res = await connection.query(query);
      expect(res.rows).toMatchSnapshot('select __user and literal in wrapper');
    });

    test('join with grouped query', async () => {
      const query = `
        SELECT
          "Orders".status AS status,
          COUNT(*) AS count
        FROM
          "Orders"
          INNER JOIN
          (
            SELECT
              status,
              SUM(totalAmount)
            FROM
              "Orders"
            GROUP BY 1
            ORDER BY 2 DESC
            LIMIT 2
          ) top_orders
        ON
          "Orders".status = top_orders.status
        GROUP BY 1
        ORDER BY 1
        `;

      const res = await connection.query(query);
      // Expect only top statuses 2 by total amount: processed and shipped
      expect(res.rows).toMatchSnapshot('join grouped');
    });

    test('join with filtered grouped query', async () => {
      const query = `
        SELECT
          "Orders".status AS status,
          COUNT(*) AS count
        FROM
          "Orders"
          INNER JOIN
          (
            SELECT
              status,
              SUM(totalAmount)
            FROM
              "Orders"
            WHERE
              status NOT IN ('shipped')
            GROUP BY 1
            ORDER BY 2 DESC
            LIMIT 2
          ) top_orders
        ON
          "Orders".status = top_orders.status
        GROUP BY 1
        `;

      const res = await connection.query(query);
      // Expect only top statuses 2 by total amount, with shipped filtered out: processed and new
      expect(res.rows).toMatchSnapshot('join grouped with filter');
    });

    test('join with grouped query on coalesce', async () => {
      const query = `
        SELECT
          "Orders".status AS status,
          COUNT(*) AS count
        FROM
          "Orders"
          INNER JOIN
          (
            SELECT
              status,
              SUM(totalAmount)
            FROM
              "Orders"
            GROUP BY 1
            ORDER BY 2 DESC
            LIMIT 2
          ) top_orders
        ON
          (COALESCE("Orders".status, '') = COALESCE(top_orders.status, '')) AND
          (("Orders".status IS NOT NULL) = (top_orders.status IS NOT NULL))
        GROUP BY 1
        ORDER BY 1
        `;

      const res = await connection.query(query);
      // Expect only top statuses 2 by total amount: processed and shipped
      expect(res.rows).toMatchSnapshot('join grouped on coalesce');
    });

    test('where segment is false', async () => {
      const query =
        'SELECT value AS val, * FROM "SegmentTest" WHERE segment_eq_1 IS FALSE ORDER BY value;';
      const res = await connection.query(query);
      expect(res.rows.map((x) => x.val)).toEqual([789, 987]);
    });

    test('select null in subquery with streaming', async () => {
      const query = `
      SELECT * FROM (
        SELECT NULL AS "usr",
        value AS val
        FROM "SegmentTest" WHERE segment_eq_1 IS FALSE
        ORDER BY value
      ) "y";`;
      const res = await connection.query(query);
      expect(res.rows).toMatchSnapshot();
    });

    test('tableau bi fiscal year query', async () => {
      const query = `
      SELECT
        CAST("orders"."status" AS TEXT) AS "status",
        CAST(TRUNC(EXTRACT(YEAR FROM ("orders"."createdAt" + 11 * INTERVAL '1 MONTH'))) AS INT) AS "yr:created_at:ok"
      FROM
        "public"."Orders" AS "orders"
      GROUP BY 1, 2 ORDER BY status`;

      const res = await connection.query(query);
      expect(res.rows).toMatchSnapshot('result');
    });

    test('query with intervals', async () => {
      const query = `
      SELECT
        "orders"."createdAt" AS "timestamp",
        "orders"."createdAt" + 11 * INTERVAL '1 YEAR' AS "c0",
        "orders"."createdAt" + 11 * INTERVAL '2 MONTH' AS "c1",
        "orders"."createdAt" + 11 * INTERVAL '321 DAYS' AS "c2",
        "orders"."createdAt" + 11 * INTERVAL '43210 SECONDS' AS "c3",
        "orders"."createdAt" + 11 * INTERVAL '1 MON 12345 MS' + 10 * INTERVAL '1 MON 12345 MS' AS "c4"
      FROM
        "public"."Orders" AS "orders" ORDER BY createdAt`;

      const res = await connection.query(query);
      expect(res.rows).toMatchSnapshot('timestamps');
    });

    test('query with intervals (SQL PUSH DOWN)', async () => {
      const query = `
      SELECT
        CONCAT(DATE(createdAt), ' :') AS d,
        "orders"."createdAt" + 11 * INTERVAL '1 YEAR' AS "c0",
        "orders"."createdAt" + 11 * INTERVAL '2 MONTH' AS "c1",
        "orders"."createdAt" + 11 * INTERVAL '321 DAYS' AS "c2",
        "orders"."createdAt" + 11 * INTERVAL '43210 SECONDS' AS "c3",
        "orders"."createdAt" + 11 * INTERVAL '32 DAYS 20 HOURS' AS "c4",
        "orders"."createdAt" + 11 * INTERVAL '1 MON 12345 MS' + 10 * INTERVAL '1 MON 12345 MS' AS "c5",
        "orders"."createdAt" + 11 * INTERVAL '12345 MS' AS "c6",
        "orders"."createdAt" + 11 * INTERVAL '2 DAY 12345 MS' AS "c7",
        "orders"."createdAt" + 11 * INTERVAL '3 MON 2 DAY 12345 MS' AS "c8"
      FROM
        "public"."Orders" AS "orders" ORDER BY createdAt`;

      const res = await connection.query(query);
      expect(res.rows).toMatchSnapshot('timestamps');
    });

    test('query views with deep joins', async () => {
      const query = `
      SELECT
        CAST(
          DATE_TRUNC(
            'MONTH',
            CAST(
              CAST("OrdersItemsPrefixView"."Orders_createdAt" AS DATE) AS TIMESTAMP
            )
          ) AS DATE
        ) AS "Calculation_1055547778125863",
        SUM("OrdersItemsPrefixView"."Orders_arpu") AS "Orders_arpu",
        SUM("OrdersItemsPrefixView"."Orders_refundRate") AS "Orders_refundRate",
        SUM("OrdersItemsPrefixView"."Orders_netCollectionCompleted") AS "Orders_netCollectionCompleted"
      FROM
        OrdersItemsPrefixView
      WHERE
        OrdersItemsPrefixView.Orders_createdAt >= '2024-01-01T00:00:00.000'
        AND OrdersItemsPrefixView.Orders_createdAt <= '2024-12-31T23:59:59.999'
        AND (OrdersItemsPrefixView.Orders_status IN ('shipped', 'processed'))
        AND (OrdersItemsPrefixView.OrderItems_type IN ('Electronics', 'Home'))
      GROUP BY 1
      `;

      const res = await connection.query(query);
      expect(res.rows).toMatchSnapshot('query-view-deep-joins');
    });

    test('wrapper with duplicated members', async () => {
      const query = `
        SELECT
          "foo",
          "bar",
          CASE
            WHEN "bar" = 'new'
            THEN 1
            ELSE 0
            END
            AS "bar_expr"
        FROM (
          SELECT
            "rows"."foo" AS "foo",
            "rows"."bar" AS "bar"
          FROM (
            SELECT
              "status" AS "foo",
              "status" AS "bar"
            FROM Orders
          ) "rows"
          GROUP BY
            "foo",
            "bar"
        ) "_"
        ORDER BY
          "bar_expr"
          LIMIT 1
        ;
      `;

      const res = await connection.query(query);
      expect(res.rows).toMatchSnapshot('wrapper-duplicated-members');
    });
  });
});
