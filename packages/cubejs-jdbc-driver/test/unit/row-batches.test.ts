// Keep the JVM out of the unit test: reading batches only depends on the result set object.
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

class TestJDBCDriver extends JDBCDriver {
  public override executeStatement(conn: any, query: any, cancelObj?: any) {
    return super.executeStatement(conn, query, cancelObj);
  }

  public override jdbcTypeName(jdbcType: number, getterNames: (string | undefined)[]) {
    return super.jdbcTypeName(jdbcType, getterNames);
  }
}

// The constructor requires a JVM-backed pool.
const driver: TestJDBCDriver = Object.create(TestJDBCDriver.prototype);

function connectionReturning(resultSet: unknown) {
  const statement = {
    cancel: (cb: Function) => cb(null),
    setQueryTimeout: (_seconds: number, cb: Function) => cb(null),
    execute: (_query: string, cb: Function) => cb(null, resultSet),
  };

  return { createStatement: (cb: Function) => cb(null, statement) };
}

describe('JDBCDriver row batches', () => {
  it('concatenates every batch of a result set', async () => {
    const batches = [[{ id: 1 }, { id: 2 }], [{ id: 3 }], null];
    const options: unknown[] = [];
    const resultSet = {
      toObjectBatchIter: async (opts: unknown) => {
        options.push(opts);
        return { labels: ['id'], types: [4], next: async () => batches.shift() };
      },
    };

    expect(await driver.executeStatement(connectionReturning(resultSet), 'SELECT 1')).toEqual(
      [{ id: 1 }, { id: 2 }, { id: 3 }]
    );
    // the batch size is left to the node-jdbc default
    expect(options).toEqual([{ batchSize: undefined }]);
  });

  it('returns an empty array for an empty result set', async () => {
    const resultSet = {
      toObjectBatchIter: async () => ({ labels: ['id'], types: [4], next: async () => null }),
    };

    expect(await driver.executeStatement(connectionReturning(resultSet), 'SELECT 1')).toEqual([]);
  });

  it('returns a non result set value as is', async () => {
    expect(await driver.executeStatement(connectionReturning(false), 'CREATE TABLE t (a int)')).toEqual(false);
  });

  it('propagates a batch error', async () => {
    const resultSet = {
      toObjectBatchIter: async () => ({
        labels: ['id'],
        types: [4],
        next: async () => { throw new Error('fetch failed'); },
      }),
    };

    await expect(driver.executeStatement(connectionReturning(resultSet), 'SELECT 1')).rejects.toThrow('fetch failed');
  });

  it('names column types independently of the getter node-jdbc reads them with', () => {
    // node-jdbc reads BIGINT / NUMERIC / DECIMAL with getString() to keep their precision
    const getterNames: string[] = [];
    getterNames[-5] = 'String';
    getterNames[2] = 'String';
    getterNames[3] = 'String';
    getterNames[4] = 'Int';
    getterNames[8] = 'Double';
    getterNames[12] = 'String';

    expect(driver.jdbcTypeName(-5, getterNames)).toEqual('bigint');
    expect(driver.jdbcTypeName(2, getterNames)).toEqual('bigdecimal');
    expect(driver.jdbcTypeName(3, getterNames)).toEqual('bigdecimal');
    expect(driver.jdbcTypeName(4, getterNames)).toEqual('int');
    expect(driver.jdbcTypeName(8, getterNames)).toEqual('double');
    expect(driver.jdbcTypeName(12, getterNames)).toEqual('string');
    // a type node-jdbc has no getter for
    expect(driver.jdbcTypeName(2003, getterNames)).toEqual('string');
  });
});
