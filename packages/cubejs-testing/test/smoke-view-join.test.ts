// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, describe, expect, jest, test } from '@jest/globals';
import { Client as PgClient } from 'pg';
import { BirdBox, getBirdbox } from '../src';
import {
  DEFAULT_CONFIG,
  JEST_AFTER_ALL_DEFAULT_TIMEOUT,
  JEST_BEFORE_ALL_DEFAULT_TIMEOUT,
  stopIfStarted,
} from './smoke-tests';

const PG_PORT = 5656;
const SQL_USER = 'admin';
const SQL_PASSWORD = 'admin_password';

// SQL API joins of views on a shared cube member, end to end: CubeSQL merges
// `customers_view JOIN orders_view ON o.customer_city = c.customer_city` into a
// single multi-fact Cube query (both columns resolve to customers.city), and
// Tesseract stitches the facts with a FULL OUTER JOIN on the shared key. The
// requested join type is recovered with `set` filters, so each join type is
// checked against the rows plain SQL would return over the fixture data:
//
//   city     avg_age  revenue  refunds
//   Austin   30       150      10
//   Boston   50       70       -
//   Chicago  60       -        5
//   Denver   35       -        -
//
// The merge only happens with the Tesseract SQL planner, so it is pinned on in
// the birdbox env below (birdbox spreads `process.env` first, so the pin wins
// over whatever CUBEJS_TESSERACT_SQL_PLANNER the CI leg exports) and the suite
// runs on both legs.
describe('SQL API join of views on a shared member', () => {
  jest.setTimeout(60 * 5 * 1000);
  let birdbox: BirdBox;
  let connection: PgClient;

  beforeAll(async () => {
    birdbox = await getBirdbox(
      'duckdb',
      {
        CUBEJS_DB_TYPE: 'duckdb',
        ...DEFAULT_CONFIG,
        CUBEJS_TESSERACT_SQL_PLANNER: 'true',
        CUBEJS_PG_SQL_PORT: `${PG_PORT}`,
        CUBEJS_SQL_USER: SQL_USER,
        CUBEJS_SQL_PASSWORD: SQL_PASSWORD,
      },
      {
        schemaDir: 'view-join/schema',
      }
    );
    connection = new PgClient({
      database: 'db',
      port: PG_PORT,
      host: '127.0.0.1',
      user: SQL_USER,
      password: SQL_PASSWORD,
      ssl: false,
    });
    await connection.connect();
  }, JEST_BEFORE_ALL_DEFAULT_TIMEOUT);

  afterAll(async () => {
    await stopIfStarted('connection', connection && (() => connection.end()));
    await stopIfStarted('birdbox', birdbox);
  }, JEST_AFTER_ALL_DEFAULT_TIMEOUT);

  // Measures come back as numeric strings or numbers depending on the type;
  // normalize them (and keep NULL as null) so rows compare by value.
  async function query(sql: string): Promise<Record<string, unknown>[]> {
    const { rows } = await connection.query(sql);
    return rows.map((row) => Object.fromEntries(
      Object.entries(row).map(([key, value]) => [
        key,
        value === null || value instanceof Date || Number.isNaN(Number(value)) ? value : Number(value),
      ])
    ));
  }

  test('LEFT JOIN keeps every key of the left view', async () => {
    const rows = await query(`
      SELECT c.customer_city AS city, measure(o.revenue) AS revenue, measure(c.avg_age) AS avg_age
      FROM customers_view c
      LEFT JOIN orders_view o ON o.customer_city = c.customer_city
      GROUP BY 1
      ORDER BY 1
    `);

    expect(rows).toEqual([
      { city: 'Austin', revenue: 150, avg_age: 30 },
      { city: 'Boston', revenue: 70, avg_age: 50 },
      { city: 'Chicago', revenue: null, avg_age: 60 },
      { city: 'Denver', revenue: null, avg_age: 35 },
    ]);
  });

  test('INNER JOIN keeps only keys present in both views', async () => {
    const rows = await query(`
      SELECT c.customer_city AS city, measure(o.revenue) AS revenue, measure(c.avg_age) AS avg_age
      FROM customers_view c
      INNER JOIN orders_view o ON o.customer_city = c.customer_city
      GROUP BY 1
      ORDER BY 1
    `);

    expect(rows).toEqual([
      { city: 'Austin', revenue: 150, avg_age: 30 },
      { city: 'Boston', revenue: 70, avg_age: 50 },
    ]);
  });

  test('RIGHT JOIN keeps every key of the right view', async () => {
    const rows = await query(`
      SELECT o.customer_city AS city, measure(o.revenue) AS revenue, measure(r.refunds) AS refunds
      FROM returns_view r
      RIGHT JOIN orders_view o ON o.customer_city = r.customer_city
      GROUP BY 1
      ORDER BY 1
    `);

    expect(rows).toEqual([
      { city: 'Austin', revenue: 150, refunds: 10 },
      { city: 'Boston', revenue: 70, refunds: null },
    ]);
  });

  test('FULL JOIN keeps keys from either view', async () => {
    const rows = await query(`
      SELECT o.customer_city AS city, measure(o.revenue) AS revenue, measure(r.refunds) AS refunds
      FROM orders_view o
      FULL JOIN returns_view r ON r.customer_city = o.customer_city
      GROUP BY 1
      ORDER BY 1
    `);

    expect(rows).toEqual([
      { city: 'Austin', revenue: 150, refunds: 10 },
      { city: 'Boston', revenue: 70, refunds: null },
      { city: 'Chicago', revenue: null, refunds: 5 },
    ]);
  });

  test('three views are merged into one query', async () => {
    const rows = await query(`
      SELECT c.customer_city AS city,
        measure(c.avg_age) AS avg_age,
        measure(o.revenue) AS revenue,
        measure(r.refunds) AS refunds
      FROM customers_view c
      FULL JOIN orders_view o ON o.customer_city = c.customer_city
      FULL JOIN returns_view r ON r.customer_city = c.customer_city
      GROUP BY 1
      ORDER BY 1
    `);

    expect(rows).toEqual([
      { city: 'Austin', avg_age: 30, revenue: 150, refunds: 10 },
      { city: 'Boston', avg_age: 50, revenue: 70, refunds: null },
      { city: 'Chicago', avg_age: 60, revenue: null, refunds: 5 },
      { city: 'Denver', avg_age: 35, revenue: null, refunds: null },
    ]);
  });

  test('a WHERE filter is applied to the merged query', async () => {
    const rows = await query(`
      SELECT c.customer_city AS city, measure(o.revenue) AS revenue, measure(c.avg_age) AS avg_age
      FROM customers_view c
      LEFT JOIN orders_view o ON o.customer_city = c.customer_city
      WHERE c.customer_city IN ('Austin', 'Chicago')
      GROUP BY 1
      ORDER BY 1
    `);

    expect(rows).toEqual([
      { city: 'Austin', revenue: 150, avg_age: 30 },
      { city: 'Chicago', revenue: null, avg_age: 60 },
    ]);
  });

  test('views join on a shared time dimension truncated to the grouped grain', async () => {
    const rows = await query(`
      SELECT DATE_TRUNC('day', c.created_at) AS day, measure(o.revenue) AS revenue, measure(c.avg_age) AS avg_age
      FROM customers_view c
      INNER JOIN orders_view o ON DATE_TRUNC('day', o.created_at) = DATE_TRUNC('day', c.created_at)
      GROUP BY 1
      ORDER BY 1
    `);

    expect(rows.map((row) => ({ ...row, day: new Date(row.day as string | Date).toISOString() }))).toEqual([
      { day: '2024-01-01T00:00:00.000Z', revenue: 150, avg_age: 30 },
      { day: '2024-01-02T00:00:00.000Z', revenue: 70, avg_age: 50 },
    ]);
  });
});
