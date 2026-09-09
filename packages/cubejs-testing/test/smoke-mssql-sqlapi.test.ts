// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, expect, jest } from '@jest/globals';
import { Client as PgClient } from 'pg';
import { MssqlDbRunner } from '@cubejs-backend/testing-shared';
import type { StartedTestContainer } from 'testcontainers';
import { BirdBox, getBirdbox } from '../src';
import {
  DEFAULT_CONFIG,
  JEST_AFTER_ALL_DEFAULT_TIMEOUT,
  JEST_BEFORE_ALL_DEFAULT_TIMEOUT,
} from './smoke-tests';

// Regression tests for https://github.com/cube-js/cube/issues/11826:
// SQL API push down renders boolean literals and predicates without adapting
// them to the consuming context, which produces invalid T-SQL. SQL Server has
// no boolean value type: `TRUE`/`FALSE` are not literals, a `BIT` column is not
// a condition, and a predicate is not a value that can be selected or grouped.
describe('mssql SQL API push down', () => {
  jest.setTimeout(60 * 5 * 1000);

  let birdbox: BirdBox;
  let db: StartedTestContainer;
  let connection: PgClient;

  const pgPort = 5657;

  beforeAll(async () => {
    db = await MssqlDbRunner.startContainer({});
    birdbox = await getBirdbox(
      'mssql',
      {
        ...DEFAULT_CONFIG,

        CUBEJS_DB_TYPE: 'mssql',
        CUBEJS_DB_HOST: db.getHost(),
        CUBEJS_DB_PORT: `${db.getMappedPort(1433)}`,
        CUBEJS_DB_USER: 'sa',
        CUBEJS_DB_PASS: process.env.TEST_DB_PASSWORD || 'Test1test',

        CUBEJS_PG_SQL_PORT: `${pgPort}`,
        CUBEJS_SQL_USER: 'admin',
        CUBEJS_SQL_PASSWORD: 'admin_password',
        CUBESQL_SQL_PUSH_DOWN: 'true',
      },
      {
        schemaDir: 'mssql/schema',
      }
    );

    connection = new PgClient({
      database: 'db',
      port: pgPort,
      host: '127.0.0.1',
      user: 'admin',
      password: 'admin_password',
      ssl: false,
    });
    await connection.connect();
  }, JEST_BEFORE_ALL_DEFAULT_TIMEOUT);

  afterAll(async () => {
    await connection?.end();
    await birdbox?.stop();
    await db?.stop();
  }, JEST_AFTER_ALL_DEFAULT_TIMEOUT);

  // Rows come back in an unspecified order, and counts may arrive as strings
  // depending on the pg type parser, so normalize both sides before comparing.
  const normalize = (rows: any[], countColumn: string) => rows
    .map((row) => ({
      ...row,
      [countColumn]: row[countColumn] === null ? null : Number(row[countColumn]),
    }))
    .sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));

  const expectRows = (actual: any[], countColumn: string, expected: any[]) => {
    expect(normalize(actual, countColumn)).toEqual(normalize(expected, countColumn));
  };

  test('predicate-valued projection over an aggregate', async () => {
    const res = await connection.query(
      'SELECT COUNT(DISTINCT "companyCode") = 2 AS flag FROM "BooleanFixture"'
    );

    expect(res.rows).toEqual([{ flag: true }]);
  });

  test('boolean dimension compared to a literal in a filter', async () => {
    const res = await connection.query(
      'SELECT "companyCode", MEASURE("count") AS cnt FROM "BooleanFixture" WHERE "completed" = TRUE GROUP BY 1'
    );

    expectRows(res.rows, 'cnt', [
      { companyCode: 'a', cnt: 1 },
    ]);
  });

  test('negated boolean dimension in a filter', async () => {
    const res = await connection.query(
      'SELECT "companyCode", MEASURE("count") AS cnt FROM "BooleanFixture" WHERE NOT "completed" GROUP BY 1'
    );

    expectRows(res.rows, 'cnt', [
      { companyCode: 'b', cnt: 1 },
    ]);
  });

  test('negated conjunction over a boolean dimension in a filter', async () => {
    // NULL AND FALSE is FALSE, so the row with an unknown `completed` is kept.
    const res = await connection.query(
      'SELECT "companyCode", MEASURE("count") AS cnt FROM "BooleanFixture" WHERE NOT ("completed" AND "companyCode" = \'a\') GROUP BY 1'
    );

    expectRows(res.rows, 'cnt', [
      { companyCode: 'b', cnt: 2 },
    ]);
  });

  test('IS NULL predicate as a projection and grouping key', async () => {
    const res = await connection.query(
      'SELECT "completed" IS NULL AS missing, MEASURE("count") AS cnt FROM "BooleanFixture" GROUP BY 1'
    );

    expectRows(res.rows, 'cnt', [
      { missing: false, cnt: 2 },
      { missing: true, cnt: 1 },
    ]);
  });

  test('boolean literals inside a CASE expression', async () => {
    const res = await connection.query(
      'SELECT "companyCode", CASE WHEN "completed" THEN TRUE ELSE FALSE END AS f, MEASURE("count") AS cnt FROM "BooleanFixture" GROUP BY 1, 2'
    );

    expectRows(res.rows, 'cnt', [
      { companyCode: 'a', f: true, cnt: 1 },
      { companyCode: 'b', f: false, cnt: 2 },
    ]);
  });

  test('NOT over a boolean dimension as a projection preserves unknown', async () => {
    const res = await connection.query(
      'SELECT NOT "completed" AS n, MEASURE("count") AS cnt FROM "BooleanFixture" GROUP BY 1'
    );

    expectRows(res.rows, 'cnt', [
      { n: null, cnt: 1 },
      { n: false, cnt: 1 },
      { n: true, cnt: 1 },
    ]);
  });
});
