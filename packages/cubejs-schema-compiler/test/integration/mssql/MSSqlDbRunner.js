// eslint-disable-next-line import/no-extraneous-dependencies
import { GenericContainer, Wait } from 'testcontainers';
import sql from 'mssql';

import { BaseDbRunner } from '../postgres/BaseDbRunner';

export class MSSqlDbRunner extends BaseDbRunner {
  async connectionLazyInit(port) {
    return {
      testQueries: async (queries, fixture) => {
        const pool = new sql.ConnectionPool({
          server: 'localhost',
          port,
          user: 'sa',
          password: this.password()
        });

        await pool.connect();

        try {
          const tx = new sql.Transaction(pool);
          await tx.begin();
          try {
            await this.prepareFixture(tx, fixture);
            const result = await queries.map(query => async () => {
              const request = new sql.Request(tx);
              (query[1] || []).forEach((v, i) => request.input(`_${i + 1}`, v));
              return (await request.query(query[0])).recordset;
            }).reduce((a, b) => a.then(b), Promise.resolve());
            await tx.commit();
            return result;
          } catch (e) {
            // console.log(e.stack);
            await tx.rollback();
            throw e;
          }
        } finally {
          await pool.close();
        }
      }
    };
  }

  async prepareFixture(tx) {
    const query = async (q) => {
      const request = new sql.Request(tx);
      await request.query(q);
    };
    await query('CREATE TABLE ##visitors (id INT, amount INT, created_at datetime, updated_at datetime, status INT, source VARCHAR(MAX), latitude DECIMAL, longitude DECIMAL)');
    await query('CREATE TABLE ##visitor_checkins (id INT, visitor_id INT, created_at datetime, source VARCHAR(MAX))');
    await query('CREATE TABLE ##cards (id INT, visitor_id INT, visitor_checkin_id INT)');
    await query(`
    INSERT INTO
    ##visitors
    (id, amount, created_at, updated_at, status, source, latitude, longitude) VALUES
    (1, 100, '2017-01-03', '2017-01-30', 1, 'some', 120.120, 40.60),
    (2, 200, '2017-01-05', '2017-01-15', 1, 'some', 120.120, 58.60),
    (3, 300, '2017-01-06', '2017-01-20', 2, 'google', 120.120, 70.60),
    (4, 400, '2017-01-07', '2017-01-25', 2, NULL, 120.120, 10.60),
    (5, 500, '2017-01-07', '2017-01-25', 2, NULL, 120.120, 58.10),
    (6, 500, '2016-09-07', '2016-09-07', 2, NULL, 120.120, 58.10)
    `);
    await query(`
    INSERT INTO
    ##visitor_checkins
    (id, visitor_id, created_at, source) VALUES
    (1, 1, '2017-01-03', NULL),
    (2, 1, '2017-01-04', NULL),
    (3, 1, '2017-01-05', 'google'),
    (4, 2, '2017-01-05', NULL),
    (5, 2, '2017-01-05', NULL),
    (6, 3, '2017-01-06', NULL)
    `);
    await query(`
    INSERT INTO
    ##cards
    (id, visitor_id, visitor_checkin_id) VALUES
    (1, 1, 1),
    (2, 1, 2),
    (3, 3, 6)
    `);
  }

  password() {
    return process.env.TEST_DB_PASSWORD || 'Test1test';
  }

  async containerLazyInit() {
    const version = process.env.TEST_MSSQL_VERSION || '2017-latest';

    return new GenericContainer('mcr.microsoft.com/mssql/server', version)
      .withEnv('ACCEPT_EULA', 'Y')
      .withEnv('MSSQL_PID', 'Developer')
      .withEnv('MSSQL_SA_PASSWORD', this.password())
      .withHealthCheck({
        test: `/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P ${this.password()} -Q "SELECT 1" || exit 1`,
        interval: 2 * 1000,
        timeout: 3 * 1000,
        retries: 5,
        startPeriod: 10 * 1000,
      })
      .withExposedPorts(this.port())
      .withWaitStrategy(Wait.forHealthCheck())
      .start();
  }

  port() {
    return 1433;
  }
}
