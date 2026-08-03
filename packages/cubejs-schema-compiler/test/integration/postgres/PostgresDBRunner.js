// eslint-disable-next-line import/no-extraneous-dependencies
import { PostgresQuery } from '../../../src';

const pgPromise = require('pg-promise');
// eslint-disable-next-line import/no-extraneous-dependencies
const { GenericContainer, Wait } = require('testcontainers');
const { BaseDbRunner } = require('../utils/BaseDbRunner');

process.env.TZ = 'GMT';

export class PostgresDBRunner extends BaseDbRunner {
  async connectionLazyInit(port) {
    const pgp = pgPromise();

    const db = pgp({
      host: 'localhost',
      port,
      password: this.password(),
      database: 'model_test',
      poolSize: 1,
      user: process.env.TEST_PG_USER || 'root',
    });

    const defaultFixture = this.prepareFixture.bind(this);
    return {
      testQueries(queries, prepareDataSet) {
        prepareDataSet = prepareDataSet || defaultFixture;
        return db.tx(async tx => {
          await tx.query('SET TIME ZONE \'UTC\'');
          await prepareDataSet(tx);
          let lastResult;
          for (const [query, params] of queries) {
            try {
              lastResult = await tx.query(query, params);
            } catch (e) {
              throw new Error(`Execution failed for '${query}', params: ${params}: ${e.stack || e}`);
            }
          }
          return JSON.parse(JSON.stringify(lastResult));
        });
      },
      close() {
        return pgp.end();
      }
    };
  }

  async prepareFixture(tx) {
    return tx.batch([
      tx.query('CREATE TEMPORARY TABLE visitors (id INT, amount INT, created_at TIMESTAMP, updated_at TIMESTAMP, status INT, source TEXT, latitude DECIMAL, longitude DECIMAL) ON COMMIT DROP'),
      tx.query('CREATE TEMPORARY TABLE visitor_checkins (id INT, visitor_id INT, created_at TIMESTAMP, source TEXT) ON COMMIT DROP'),
      tx.query('CREATE TEMPORARY TABLE cards (id INT, visitor_id INT, visitor_checkin_id INT) ON COMMIT DROP'),
      tx.query('CREATE TEMPORARY TABLE left_table (id INT, total DOUBLE PRECISION, description character varying) ON COMMIT DROP'),
      tx.query('CREATE TEMPORARY TABLE right_table (id INT, total DOUBLE PRECISION, description character varying) ON COMMIT DROP'),
      tx.query('CREATE TEMPORARY TABLE mid_table (id INT, left_id INT, right_id INT) ON COMMIT DROP'),
      tx.query('CREATE TEMPORARY TABLE compound_key_cards (id_a INT, id_b INT, visitor_id INT, visitor_checkin_id INT, visit_rank INT) ON COMMIT DROP'),
      tx.query(`
        INSERT INTO
        visitors
        (id, amount, created_at, updated_at, status, source, latitude, longitude) VALUES
        (1, 100, '2017-01-03', '2017-01-30', 1, 'some', 120.120, 40.60),
        (2, 200, '2017-01-05', '2017-01-15', 1, 'some', 120.120, 58.60),
        (3, 300, '2017-01-06', '2017-01-20', 2, 'google', 120.120, 70.60),
        (4, 400, '2017-01-07', '2017-01-25', 2, NULL, 120.120, 10.60),
        (5, 500, '2017-01-07', '2017-01-25', 2, NULL, 120.120, 58.10),
        (6, 500, '2016-09-07', '2016-09-07', 2, NULL, 120.120, 58.10)
      `),
      tx.query(`
        INSERT INTO
        visitor_checkins
        (id, visitor_id, created_at, source) VALUES
        (1, 1, '2017-01-03', NULL),
        (2, 1, '2017-01-04', NULL),
        (3, 1, '2017-01-05', 'google'),
        (4, 2, '2017-01-05', NULL),
        (5, 2, '2017-01-05', NULL),
        (6, 3, '2017-01-06', NULL)
      `),
      tx.query(`
        INSERT INTO
        cards
        (id, visitor_id, visitor_checkin_id) VALUES
        (1, 1, 1),
        (2, 1, 2),
        (3, 3, 6)
      `),
      tx.query(`
        INSERT INTO
        left_table
        (id, total, description) VALUES
        (1, 2.5, 'ABC'),
        (2, 2.5, 'BAC'),
        (3, 2.5, 'BCA'),
        (4, 2.5, 'CBA'),
        (5, 2.5, 'CAB'),
        (6, 2.5, 'ACB')
      `),
      tx.query(`
        INSERT INTO
        right_table
        (id, total, description) VALUES
        (1, 2.5, 'ABC'),
        (2, 2.5, 'BAC'),
        (3, 2.5, 'BCA'),
        (4, 2.5, 'CBA'),
        (5, 2.5, 'CAB'),
        (6, 2.5, 'ACB')
      `),
      tx.query(`
        INSERT INTO
        mid_table
        (id, left_id, right_id) VALUES
        (1, 1, 1),
        (2, 2, 2),
        (3, 3, 3),
        (4, 4, 4),
        (5, 5, 5),
        (6, 6, 6)
      `),
      tx.query(`
        INSERT INTO
        compound_key_cards
        (id_a, id_b, visitor_id, visitor_checkin_id, visit_rank) VALUES
        (1, 1, 1, 1, 10),
        (2, 1, 1, 2, 5),
        (2, 2, 3, 6, 7),
        (2, 2, 2, 4, 8),
        (2, 3, 4, 5, 2);
      `)
    ]);
  }

  async containerLazyInit() {
    const version = process.env.TEST_PGSQL_VERSION || '12.22';

    return new GenericContainer(`postgres:${version}`)
      .withEnvironment({
        POSTGRES_USER: 'root',
        POSTGRES_DB: 'model_test',
        POSTGRES_PASSWORD: this.password(),
      })
      .withExposedPorts(this.port())
      .withHealthCheck({
        test: [
          'CMD-SHELL',
          `pg_isready -h localhost -p ${this.port()} -U root -d model_test || exit 1`
        ],
        interval: 1000,
        timeout: 500,
        retries: 20,
        startPeriod: 5 * 1000,
      })
      .withWaitStrategy(Wait.forHealthCheck())
      .withStartupTimeout(15 * 1000)
      .start();
  }

  password() {
    return 'passwd';
  }

  port() {
    return 5432;
  }

  newTestQuery(compilers, query) {
    return new PostgresQuery(compilers, query);
  }
}

export const dbRunner = new PostgresDBRunner();

// eslint-disable-next-line no-undef
afterAll(async () => {
  await dbRunner.tearDown();
});
