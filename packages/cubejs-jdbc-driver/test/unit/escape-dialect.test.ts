import { SupportedDrivers } from '../../src/supported-drivers';

// Keep the JVM out of the unit test: escaping rules are pure logic.
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

// The constructor requires a JVM-backed pool, while the escaping rules only
// depend on the configured db type.
const driverFor = (dbType: string): any => {
  const driver = Object.create(JDBCDriver.prototype);
  driver.config = { dbType };
  return driver;
};

describe('JDBC escape dialect', () => {
  it('declares a dialect for every supported engine', () => {
    for (const [name, options] of Object.entries(SupportedDrivers)) {
      expect([name, options.escapeDialect]).toEqual([name, expect.stringMatching(/^(ansi|mysql|spark)$/)]);
    }
  });

  it('escapes params with the dialect of the configured engine', () => {
    expect(driverFor('mysql').prepareQueryWithParams('SELECT ?', ['a\\b'])).toEqual('SELECT \'a\\\\b\'');
    expect(driverFor('athena').prepareQueryWithParams('SELECT ?', ['a\\b'])).toEqual('SELECT \'a\\b\'');
    expect(driverFor('sparksql').prepareQueryWithParams('SELECT ?', ['a\\b'])).toEqual('SELECT \'a\\\\b\'');
  });

  it('throws for an unknown engine instead of guessing the dialect', () => {
    expect(() => driverFor('kekdb').prepareQueryWithParams('SELECT ?', ['a\'b']))
      .toThrow(/Unable to detect SQL escaping rules/);
  });
});
