// Keep the JVM out of the unit test: preamble resolution is pure logic.
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

const MYSQL_BUILT_IN = 'SET time_zone = \'+00:00\'';

// The constructor requires a JVM-backed pool, while preamble resolution only
// depends on the config.
const driverFor = (config: Record<string, any>): any => {
  const driver = Object.create(JDBCDriver.prototype);
  driver.config = config;
  driver.logger = jest.fn();
  return driver;
};

describe('JDBC sql preamble', () => {
  afterEach(() => {
    delete process.env.CUBEJS_DB_SQL_PREAMBLE;
  });

  it('keeps the per-dbType built-ins when nothing is configured', () => {
    expect(driverFor({ dbType: 'mysql' }).prepareConnectionQueries()).toEqual([MYSQL_BUILT_IN]);
    expect(driverFor({ dbType: 'athena' }).prepareConnectionQueries()).toEqual([]);
  });

  // Decision 8: replacing the built-in would silently drop the timezone
  // guarantee MySQL depends on to read timestamps.
  it('appends sqlPreamble after the built-ins', () => {
    expect(driverFor({ dbType: 'mysql', sqlPreamble: 'SET a = 1' }).prepareConnectionQueries())
      .toEqual([MYSQL_BUILT_IN, 'SET a = 1']);
  });

  it('splits a multi-statement sqlPreamble into separate statements', () => {
    expect(driverFor({ dbType: 'athena', sqlPreamble: 'SET a = 1; SET b = 2' }).prepareConnectionQueries())
      .toEqual(['SET a = 1', 'SET b = 2']);
  });

  it('does not split a semicolon inside a UDF body', () => {
    const preamble = 'CREATE FUNCTION f() AS $$ SELECT 1; $$; SET a = 1';

    expect(driverFor({ dbType: 'athena', sqlPreamble: preamble }).prepareConnectionQueries())
      .toEqual(['CREATE FUNCTION f() AS $$ SELECT 1; $$', 'SET a = 1']);
  });

  it('reads the env var when no config value is set', () => {
    process.env.CUBEJS_DB_SQL_PREAMBLE = 'SET from_env = 1';

    expect(driverFor({ dbType: 'mysql' }).prepareConnectionQueries())
      .toEqual([MYSQL_BUILT_IN, 'SET from_env = 1']);
  });

  it('prefers the config value over the env var', () => {
    process.env.CUBEJS_DB_SQL_PREAMBLE = 'SET from_env = 1';

    expect(driverFor({ dbType: 'athena', sqlPreamble: 'SET from_config = 1' }).prepareConnectionQueries())
      .toEqual(['SET from_config = 1']);
  });

  it('treats a blank preamble as not configured', () => {
    expect(driverFor({ dbType: 'mysql', sqlPreamble: '   ' }).prepareConnectionQueries())
      .toEqual([MYSQL_BUILT_IN]);
  });

  describe('deprecated prepareConnectionQueries', () => {
    // This option has always REPLACED the built-ins, so someone who set it to
    // override the MySQL timezone still gets only their own statements. Making
    // it append would change behavior for existing deployments.
    it('still replaces the built-ins rather than appending', () => {
      const driver = driverFor({ dbType: 'mysql', prepareConnectionQueries: ['SET time_zone = \'+03:00\''] });

      expect(driver.prepareConnectionQueries()).toEqual(['SET time_zone = \'+03:00\'']);
      expect(driver.prepareConnectionQueries()).not.toContain(MYSQL_BUILT_IN);
    });

    it('warns that the option is deprecated', () => {
      const driver = driverFor({ dbType: 'mysql', prepareConnectionQueries: ['SET a = 1'] });

      driver.prepareConnectionQueries();

      expect(driver.logger).toHaveBeenCalledTimes(1);
      expect(driver.logger.mock.calls[0][1].warning).toContain('prepareConnectionQueries');
      expect(driver.logger.mock.calls[0][1].warning).toContain('sqlPreamble');
    });

    // This runs per query, so an unlatched warning would emit thousands of
    // identical log lines a minute under load.
    it('warns once per driver instance, not once per query', () => {
      const driver = driverFor({ dbType: 'mysql', prepareConnectionQueries: ['SET a = 1'] });

      driver.prepareConnectionQueries();
      driver.prepareConnectionQueries();
      driver.prepareConnectionQueries();

      expect(driver.logger).toHaveBeenCalledTimes(1);
    });

    it('is overridden by the env var, which is not silently dropped', () => {
      process.env.CUBEJS_DB_SQL_PREAMBLE = 'SET from_env = 1';
      const driver = driverFor({ dbType: 'athena', prepareConnectionQueries: ['SET old = 1'] });

      expect(driver.prepareConnectionQueries()).toEqual(['SET from_env = 1']);
    });

    it('is kept when sqlPreamble is blank, rather than silently discarded', () => {
      const driver = driverFor({
        dbType: 'mysql',
        sqlPreamble: '   ',
        prepareConnectionQueries: ['SET time_zone = \'+03:00\''],
      });

      expect(driver.prepareConnectionQueries()).toEqual(['SET time_zone = \'+03:00\'']);
    });

    it('is overridden by sqlPreamble, which appends as usual', () => {
      const driver = driverFor({
        dbType: 'mysql',
        sqlPreamble: 'SET new = 1',
        prepareConnectionQueries: ['SET old = 1'],
      });

      expect(driver.prepareConnectionQueries()).toEqual([MYSQL_BUILT_IN, 'SET new = 1']);
      expect(driver.logger).not.toHaveBeenCalled();
    });

    it('falls back to the built-ins when set to an empty array', () => {
      expect(driverFor({ dbType: 'mysql', prepareConnectionQueries: [] }).prepareConnectionQueries())
        .toEqual([MYSQL_BUILT_IN]);
    });
  });
});
