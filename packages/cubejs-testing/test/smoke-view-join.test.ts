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
// `orders_view JOIN returns_view ON r.customer_city = o.customer_city` into a
// single multi-fact Cube query (both columns resolve to customers.city), and
// Tesseract aggregates each fact on its own and stitches the results with a
// FULL OUTER JOIN on the shared key. The requested join type is recovered with
// `set` filters, so each join type is checked against the keys it must keep:
//
//   city     revenue  refunds  paid
//   Austin   150      10       120
//   Boston   70       -        70
//   Chicago  -        5        -
//   Denver   -        -        20
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
      SELECT o.customer_city AS city, measure(o.revenue) AS revenue, measure(r.refunds) AS refunds
      FROM orders_view o
      LEFT JOIN returns_view r ON r.customer_city = o.customer_city
      GROUP BY 1
      ORDER BY 1
    `);

    expect(rows).toEqual([
      { city: 'Austin', revenue: 150, refunds: 10 },
      { city: 'Boston', revenue: 70, refunds: null },
    ]);
  });

  test('INNER JOIN keeps only keys present in both views', async () => {
    const rows = await query(`
      SELECT o.customer_city AS city, measure(o.revenue) AS revenue, measure(r.refunds) AS refunds
      FROM orders_view o
      INNER JOIN returns_view r ON r.customer_city = o.customer_city
      GROUP BY 1
      ORDER BY 1
    `);

    expect(rows).toEqual([
      { city: 'Austin', revenue: 150, refunds: 10 },
    ]);
  });

  test('RIGHT JOIN keeps every key of the right view', async () => {
    const rows = await query(`
      SELECT r.customer_city AS city, measure(o.revenue) AS revenue, measure(r.refunds) AS refunds
      FROM orders_view o
      RIGHT JOIN returns_view r ON r.customer_city = o.customer_city
      GROUP BY 1
      ORDER BY 1
    `);

    expect(rows).toEqual([
      { city: 'Austin', revenue: 150, refunds: 10 },
      { city: 'Chicago', revenue: null, refunds: 5 },
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
      SELECT o.customer_city AS city,
        measure(o.revenue) AS revenue,
        measure(r.refunds) AS refunds,
        measure(p.paid) AS paid
      FROM orders_view o
      FULL JOIN returns_view r ON r.customer_city = o.customer_city
      FULL JOIN payments_view p ON p.customer_city = o.customer_city
      GROUP BY 1
      ORDER BY 1
    `);

    expect(rows).toEqual([
      { city: 'Austin', revenue: 150, refunds: 10, paid: 120 },
      { city: 'Boston', revenue: 70, refunds: null, paid: 70 },
      { city: 'Chicago', revenue: null, refunds: 5, paid: null },
      { city: 'Denver', revenue: null, refunds: null, paid: 20 },
    ]);
  });

  test('a WHERE filter is applied to the merged query', async () => {
    const rows = await query(`
      SELECT o.customer_city AS city, measure(o.revenue) AS revenue, measure(r.refunds) AS refunds
      FROM orders_view o
      FULL JOIN returns_view r ON r.customer_city = o.customer_city
      WHERE o.customer_city IN ('Boston', 'Chicago')
      GROUP BY 1
      ORDER BY 1
    `);

    expect(rows).toEqual([
      { city: 'Boston', revenue: 70, refunds: null },
      { city: 'Chicago', revenue: null, refunds: 5 },
    ]);
  });

  // Orders fall on 2024-01-01 and 2024-01-02, returns on 2024-01-01 and
  // 2024-01-03; a DATE_TRUNC join is an INNER join, so only the shared day stays.
  test('views join on a shared time dimension truncated to the grouped grain', async () => {
    const rows = await query(`
      SELECT DATE_TRUNC('day', o.created_at) AS day, measure(o.revenue) AS revenue, measure(r.refunds) AS refunds
      FROM orders_view o
      JOIN returns_view r ON DATE_TRUNC('day', r.created_at) = DATE_TRUNC('day', o.created_at)
      GROUP BY 1
      ORDER BY 1
    `);

    expect(rows.map((row) => ({ ...row, day: new Date(row.day as string | Date).toISOString() }))).toEqual([
      { day: '2024-01-01T00:00:00.000Z', revenue: 150, refunds: 10 },
    ]);
  });
});
