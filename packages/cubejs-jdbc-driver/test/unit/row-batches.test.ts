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
import type { StreamTableData } from '@cubejs-backend/base-driver';
// eslint-disable-next-line import/first
import { JDBCDriver } from '../../src/JDBCDriver';
// eslint-disable-next-line import/first
import { QueryStream } from '../../src/QueryStream';

class TestJDBCDriver extends JDBCDriver {
  public override executeStatement(conn: any, query: any, cancelObj?: any) {
    return super.executeStatement(conn, query, cancelObj);
  }

  public override escapeDialect() {
    return 'ansi' as const;
  }

  public override jdbcTypeName(jdbcType: number, getterNames: (string | undefined)[]) {
    return super.jdbcTypeName(jdbcType, getterNames);
  }
}

// The constructor requires a JVM-backed pool.
const driver: TestJDBCDriver = Object.create(TestJDBCDriver.prototype, {
  config: { value: { batchSize: 1024 } },
});

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
    expect(options).toEqual([{ batchSize: 1024 }]);
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

  it('releases a streamed connection only after the batch being fetched and the statement are done', async () => {
    const events: string[] = [];
    let resolveBatch: (rows: unknown[]) => void = () => undefined;
    const resultSet = {
      _types: [],
      toObjectBatchIter: async () => ({
        labels: ['id'],
        types: [4],
        next: () => new Promise((resolve) => { resolveBatch = resolve; }),
      }),
    };
    const statement = {
      cancel: (cb: Function) => cb(null),
      execute: (_query: string, cb: Function) => cb(null, resultSet),
      close: (cb: Function) => { events.push('statement.close'); cb(null); },
    };
    const conn = { createStatement: (cb: Function) => cb(null, statement) };
    const streamDriver: TestJDBCDriver = Object.create(TestJDBCDriver.prototype, {
      config: { value: { batchSize: 1024 } },
      pool: {
        value: {
          acquire: async () => conn,
          release: async () => { events.push('pool.release'); },
        },
      },
    });

    const { rowStream, release } = await streamDriver.stream('SELECT 1', [], { highWaterMark: 16 }) as StreamTableData;
    (rowStream as QueryStream).read(0);
    await new Promise((resolve) => { setImmediate(resolve); });

    const releasing = release!();
    await new Promise((resolve) => { setImmediate(resolve); });
    expect(events).toEqual([]);

    events.push('batch');
    resolveBatch([{ id: 1 }]);
    await releasing;

    expect(events).toEqual(['batch', 'statement.close', 'pool.release']);
  });

  it('logs a failed statement close and still releases the streamed connection', async () => {
    const released = jest.fn();
    const logger = jest.fn();
    const resultSet = {
      _types: [],
      toObjectBatchIter: async () => ({ labels: ['id'], types: [4], next: async () => null }),
    };
    const statement = {
      cancel: (cb: Function) => cb(null),
      execute: (_query: string, cb: Function) => cb(null, resultSet),
      close: (cb: Function) => cb(new Error('close failed')),
    };
    const conn = { createStatement: (cb: Function) => cb(null, statement) };
    const streamDriver: TestJDBCDriver = Object.create(TestJDBCDriver.prototype, {
      config: { value: { batchSize: 1024 } },
      pool: { value: { acquire: async () => conn, release: released } },
      logger: { value: logger },
    });

    const { release } = await streamDriver.stream('SELECT 1', [], { highWaterMark: 16 });

    await expect(release!()).resolves.toBeUndefined();
    expect(logger).toHaveBeenCalledWith('Closing statement failed', { error: expect.stringContaining('close failed') });
    expect(released).toHaveBeenCalledWith(conn);
  });
});
