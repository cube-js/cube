// eslint-disable-next-line import/no-extraneous-dependencies
import { GenericContainer, Wait } from 'testcontainers';
import sql from 'mssql';

import { BaseDbRunner } from '../utils/BaseDbRunner';
import { MssqlQuery } from '../../../src';

export class MSSqlDbRunner extends BaseDbRunner {
  async connectionLazyInit(port) {
    return {
      testQueries: async (queries, fixture) => {
        const pool = new sql.ConnectionPool({
          server: 'localhost',
          port,
          user: 'sa',
          password: this.password(),
          options: {
            // local dev / self-signed certs
            trustServerCertificate: true,
          }
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
    await query('CREATE TABLE ##numbers (num INT);');
    await query(`
    INSERT INTO ##numbers (num) VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9),
                                  (10), (11), (12), (13), (14), (15), (16), (17), (18), (19),
                                  (20), (21), (22), (23), (24), (25), (26), (27), (28), (29),
                                  (30), (31), (32), (33), (34), (35), (36), (37), (38), (39),
                                  (40), (41), (42), (43), (44), (45), (46), (47), (48), (49),
                                  (50), (51), (52), (53), (54), (55), (56), (57), (58), (59);
    `);
  }

  password() {
    return process.env.TEST_DB_PASSWORD || 'Test1test';
  }

  sqlcmdPrefix(version) {
    if (version === '2017-latest') {
      return '/opt/mssql-tools/bin/';
    }

    // Thanks, Microsoft the last 2019 Version that has same path is "2019-CU27-ubuntu-20.04"
    // Starting with "2019-latest" published on 08/01/2024 - new path is "/opt/mssql-tools18/bin/"
    // @see https://learn.microsoft.com/en-us/troubleshoot/sql/releases/sqlserver-2019/cumulativeupdate28#3217207
    return '/opt/mssql-tools18/bin/';
  }

  async containerLazyInit() {
    const version = process.env.TEST_MSSQL_VERSION || '2019-latest';

    return new GenericContainer(`mcr.microsoft.com/mssql/server:${version}`)
      .withEnvironment({
        ACCEPT_EULA: 'Y',
        MSSQL_PID: 'Developer',
        MSSQL_SA_PASSWORD: this.password(),
      })
      .withExposedPorts(this.port())
      .withHealthCheck({
        test: [
          'CMD-SHELL',
          `${this.sqlcmdPrefix(version)}sqlcmd -C -l 1 -S localhost -U sa -P ${this.password()} -Q "SELECT 1" || exit 1`
        ],
        interval: 1000,
        timeout: 1100,
        retries: 20,
        startPeriod: 2 * 1000,
      })
      .withWaitStrategy(Wait.forHealthCheck())
      .withStartupTimeout(10 * 1000)
      .start();
  }

  port() {
    return 1433;
  }

  newTestQuery(compilers, query) {
    return new MssqlQuery(compilers, query);
  }
}
