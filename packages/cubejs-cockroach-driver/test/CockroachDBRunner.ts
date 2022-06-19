// eslint-disable-next-line import/no-extraneous-dependencies
import pgPromise from 'pg-promise';
// eslint-disable-next-line import/no-extraneous-dependencies
import { DockerComposeEnvironment, Wait } from 'testcontainers';
import path from 'path';
import { BaseDbRunner } from './BaseDBRunner';

process.env.TZ = 'GMT';

export class CockroachDBRunner extends BaseDbRunner {
  // @ts-ignore
  public async connectionLazyInit(port: number) {
    const pgp = pgPromise();

    const db = pgp({
      host: 'localhost',
      port,
      database: 'defaultdb',
      user: 'root',
      ssl: false
    });

    const defaultFixture = this.prepareFixture.bind(this);
    return {
      testQueries(queries: any, prepareDataSet: any) {
        prepareDataSet = prepareDataSet || defaultFixture;
        return db.tx((tx) => tx.query('SET TIME ZONE \'UTC\'').then(() => prepareDataSet(tx)
          .then(() => queries
            .map(
              ([query, params]: any) => () => tx.query(query, params).catch((e) => {
                throw new Error(
                  `Execution failed for '${query}', params: ${params}: ${
                    e.stack || e
                  }`
                );
              })
            )
            .reduce((a: any, b: any) => a.then(b), Promise.resolve()))
          .then((r: any) => JSON.parse(JSON.stringify(r)))));
      },
      async close() {
        return pgp.end();
      },
    };
  }

  public async prepareFixture(tx: any) {
    return tx.batch([
      tx.query('DROP TABLE IF EXISTS visitors'),
      tx.query('DROP TABLE IF EXISTS visitor_checkins'),
      tx.query('DROP TABLE IF EXISTS cards'),
      tx.query('DROP TABLE IF EXISTS left_table'),
      tx.query('DROP TABLE IF EXISTS right_table'),
      tx.query('DROP TABLE IF EXISTS mid_table'),
      tx.query('DROP TABLE IF EXISTS compound_key_cards'),
      tx.query(
        'CREATE TABLE visitors (id INT, amount INT, created_at TIMESTAMP, updated_at TIMESTAMP, status INT, source TEXT, latitude DECIMAL, longitude DECIMAL)'
      ),
      tx.query(
        'CREATE TABLE visitor_checkins (id INT, visitor_id INT, created_at TIMESTAMP, source TEXT)'
      ),
      tx.query(
        'CREATE TABLE cards (id INT, visitor_id INT, visitor_checkin_id INT)'
      ),
      tx.query(
        'CREATE TABLE left_table (id INT, total DOUBLE PRECISION, description character varying)'
      ),
      tx.query(
        'CREATE TABLE right_table (id INT, total DOUBLE PRECISION, description character varying)'
      ),
      tx.query(
        'CREATE TABLE mid_table (id INT, left_id INT, right_id INT)'
      ),
      tx.query(
        'CREATE TABLE compound_key_cards (id_a INT, id_b INT, visitor_id INT, visitor_checkin_id INT, visit_rank INT)'
      ),
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
      `),
    ]);
  }
  
  public async containerLazyInit() {
    const dc = new DockerComposeEnvironment(
      path.resolve(path.dirname(__filename), '../../'),
      'docker-compose.yml'
    ).withEnv('CONTAINER_NAME', 'crdb-runner')
      .withEnv('HOST_PORT', this.port().toString())
      .withWaitStrategy('crdb-runner', Wait.forHealthCheck());

    return dc.up();
  }

  public port() {
    return 26258;
  }
}

export const dbRunner = new CockroachDBRunner();

// eslint-disable-next-line no-undef
afterAll(async () => {
  await dbRunner.tearDown();
});
