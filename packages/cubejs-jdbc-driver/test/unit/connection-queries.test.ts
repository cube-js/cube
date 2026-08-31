// Keep the JVM out of the unit test: the connection-query replay is pure logic
// over a pooled connection.
jest.mock('@cubejs-backend/jdbc/lib/drivermanager', () => ({
  getConnection: () => { /* stub */ },
}), { virtual: true });
jest.mock('@cubejs-backend/jdbc/lib/connection', () => class Connection {
  public getMetaData() { /* stub */ }
}, { virtual: true });
jest.mock('@cubejs-backend/jdbc/lib/databasemetadata', () => class DatabaseMetaData {
  public getSchemas() { /* stub */ }

  public getTables() { /* stub */ }
}, { virtual: true });
jest.mock('@cubejs-backend/jdbc/lib/jinst', () => ({ isJvmCreated: () => true }), { virtual: true });
jest.mock('@cubejs-backend/node-java-maven', () => () => { /* stub */ }, { virtual: true });

// eslint-disable-next-line import/first
import { JDBCDriver } from '../../src/JDBCDriver';

const MYSQL_TIME_ZONE = 'SET time_zone = \'+00:00\'';

/**
 * The constructor needs a JVM-backed pool, while the replay only needs a
 * connection to hand out and a record of what was executed on it.
 *
 * `executed` collects every statement in order, so a test can assert both that
 * the connection queries ran and that they ran before the primary query.
 */
const driverFor = (config: Record<string, any>) => {
  const executed: string[] = [];
  const released: unknown[] = [];
  const conn: Record<string, any> = { id: 'conn' };

  const driver: any = Object.create(JDBCDriver.prototype);
  driver.config = config;
  driver.pool = {
    acquire: async () => conn,
    release: async (c: unknown) => { released.push(c); },
  };
  driver.executeStatement = async (_conn: unknown, sql: string) => {
    executed.push(sql);
    return [];
  };

  return { driver, executed, released, conn };
};

describe('JDBC connection queries', () => {
  describe('query()', () => {
    it('replays the dbType connection queries before the primary query', async () => {
      const { driver, executed } = driverFor({ dbType: 'mysql' });

      await driver.query('SELECT 1', []);

      expect(executed).toEqual([MYSQL_TIME_ZONE, 'SELECT 1']);
    });

    it('replays connection queries passed explicitly in the driver config', async () => {
      const { driver, executed } = driverFor({
        dbType: 'athena',
        prepareConnectionQueries: ['SET a = 1', 'SET b = 2'],
      });

      await driver.query('SELECT 1', []);

      expect(executed).toEqual(['SET a = 1', 'SET b = 2', 'SELECT 1']);
    });

    it('runs only the primary query for a dbType with no connection queries', async () => {
      const { driver, executed } = driverFor({ dbType: 'athena' });

      await driver.query('SELECT 1', []);

      expect(executed).toEqual(['SELECT 1']);
    });

    it('releases the connection when a connection query fails', async () => {
      const { driver, released, conn } = driverFor({ dbType: 'mysql' });
      driver.executeStatement = async () => { throw new Error('connection query failed'); };

      await expect(driver.query('SELECT 1', [])).rejects.toThrow('connection query failed');
      expect(released).toEqual([conn]);
    });
  });

  // `stream()` acquires its own connection, so the replay the query path does
  // buys it nothing — it has to run them itself.
  describe('stream()', () => {
    // The primary query runs through createStatement -> statement.execute, not
    // executeStatement, so it has to land in the same array — otherwise the
    // ordering assertions below would hold with the replay moved after it.
    const streamDriverFor = (config: Record<string, any>) => {
      const built = driverFor(config);
      const statement = {
        cancel: (cb: Function) => cb(null),
        execute: (sql: string, cb: Function) => {
          built.executed.push(sql);
          cb(null, {
            _types: { 12: 'string' },
            toObjectIter: (cb2: Function) => cb2(null, {
              labels: ['a'],
              types: [12],
              rows: { next: () => { /* no rows */ } },
            }),
          });
        },
      };
      built.conn.createStatement = (cb: Function) => cb(null, statement);
      return built;
    };

    it('replays the dbType connection queries before opening the stream', async () => {
      const { driver, executed } = streamDriverFor({ dbType: 'mysql' });

      await driver.stream('SELECT 1', [], { highWaterMark: 1 });

      expect(executed).toEqual([MYSQL_TIME_ZONE, 'SELECT 1']);
    });

    it('replays connection queries passed explicitly in the driver config', async () => {
      const { driver, executed } = streamDriverFor({
        dbType: 'athena',
        prepareConnectionQueries: ['SET a = 1', 'SET b = 2'],
      });

      await driver.stream('SELECT 1', [], { highWaterMark: 1 });

      expect(executed).toEqual(['SET a = 1', 'SET b = 2', 'SELECT 1']);
    });

    it('runs no connection queries for a dbType that declares none', async () => {
      const { driver, executed } = streamDriverFor({ dbType: 'athena' });

      await driver.stream('SELECT 1', [], { highWaterMark: 1 });

      expect(executed).toEqual(['SELECT 1']);
    });

    it('releases the connection when a connection query fails', async () => {
      const { driver, released, conn } = streamDriverFor({ dbType: 'mysql' });
      driver.executeStatement = async () => { throw new Error('connection query failed'); };

      await expect(driver.stream('SELECT 1', [], { highWaterMark: 1 }))
        .rejects.toThrow('connection query failed');
      expect(released).toEqual([conn]);
    });
  });
});
