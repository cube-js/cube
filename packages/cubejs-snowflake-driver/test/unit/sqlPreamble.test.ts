import { describe, expect, test } from 'vitest';

import { SnowflakeDriver } from '../../src/SnowflakeDriver';

// Snowflake is the one driver where the preamble genuinely runs once per
// connection rather than per query: there is a single long-lived connection, so
// every later query — including a streamed one — inherits the session state.
// That means no stream-specific code carries the preamble, and a regression in
// initConnection would be silent without this.
const driverWith = (preamble?: string) => {
  const driver = Object.create(SnowflakeDriver.prototype) as any;
  const executed: string[] = [];

  driver.config = { executionTimeout: 600, identIgnoreCase: false };
  driver.sqlPreamble = () => preamble;
  driver.execute = async (_connection: unknown, statement: string) => {
    executed.push(statement);
    return [];
  };
  driver.connectionPromise = null;

  return { driver, executed };
};

const connect = async (driver: any) => {
  const connection: any = {
    connect: (cb: Function) => cb(null, connection),
  };
  driver.createConnection = async () => connection;

  return driver.initConnection();
};

describe('SnowflakeDriver sql preamble', () => {
  test('runs the preamble on the session, after the ALTER SESSION defaults', async () => {
    const { driver, executed } = driverWith('SET a = 1');

    await connect(driver);

    const alterSession = executed.findIndex(s => s.includes('ALTER SESSION'));
    expect(alterSession).toBeGreaterThanOrEqual(0);
    expect(executed.indexOf('SET a = 1')).toBeGreaterThan(alterSession);
  });

  test('runs each statement of a multi-statement preamble', async () => {
    const { driver, executed } = driverWith('SET a = 1; SET b = 2');

    await connect(driver);

    expect(executed).toContain('SET a = 1');
    expect(executed).toContain('SET b = 2');
  });

  test('runs only the ALTER SESSION when no preamble is configured', async () => {
    const { driver, executed } = driverWith(undefined);

    await connect(driver);

    expect(executed).toHaveLength(1);
    expect(executed[0]).toContain('ALTER SESSION');
  });
});
