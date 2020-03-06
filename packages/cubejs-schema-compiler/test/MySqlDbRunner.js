const { GenericContainer } = require("testcontainers");
const mysql = require('mysql');
const { promisify } = require('util');

const BaseDbRunner = require('./BaseDbRunner');

class MSSqlDbRunner extends BaseDbRunner {
  async connectionLazyInit(port) {
    return {
      testQueries: async (queries, fixture) => {
        const conn = mysql.createConnection({
          host: 'localhost',
          port,
          user: 'root',
          database: 'mysql',
          password: this.password()
        });
        const connect = promisify(conn.connect.bind(conn));

        conn.execute = promisify(conn.query.bind(conn));

        await connect();

        try {
          await this.prepareFixture(conn, fixture);
          return await queries
            .map(query => async () => JSON.parse(JSON.stringify(await conn.execute(query[0], query[1]))))
            .reduce((a, b) => a.then(b), Promise.resolve());
        } finally {
          await promisify(conn.end.bind(conn))();
        }
      }
    };
  }

  async prepareFixture(conn) {
    const query = conn.execute;
    await query('CREATE TEMPORARY TABLE visitors (id INT, amount INT, created_at datetime, updated_at datetime, status INT, source VARCHAR(255), latitude DECIMAL, longitude DECIMAL)');
    await query('CREATE TEMPORARY TABLE visitor_checkins (id INT, visitor_id INT, created_at datetime, source VARCHAR(255))');
    await query('CREATE TEMPORARY TABLE cards (id INT, visitor_id INT, visitor_checkin_id INT)');
    await query(`
    INSERT INTO
    visitors
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
    visitor_checkins
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
    cards
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
    return new GenericContainer("mysql", '5.7')
      .withEnv("MYSQL_ROOT_PASSWORD", this.password())
      .withExposedPorts(this.port())
      .start();
  }

  port() {
    return 3306;
  }
}

module.exports = new MSSqlDbRunner();
