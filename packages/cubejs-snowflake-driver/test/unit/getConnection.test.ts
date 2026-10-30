import { afterEach, describe, expect, test, vi } from 'vitest';
import snowflake, { type Connection } from 'snowflake-sdk';

import { SnowflakeDriver } from '../../src/SnowflakeDriver';

class TestSnowflakeDriver extends SnowflakeDriver {
  public constructor() {
    super({ account: 'acct', username: 'service_user', password: 'service_password' });
  }

  public connect() {
    return this.getConnection();
  }
}

type Login = { error?: Error, answer?: Promise<void>, sessionError?: Error };

// Every login the driver asks the SDK for. A login answers once `answer` settles (at once without one) and
// fails with `error` if set; the real initConnection and getConnection run on top.
const fakeLogins = (plan: Login[] = []) => {
  const connections: Connection[] = [];

  vi.spyOn(snowflake, 'createConnection').mockImplementation(() => {
    const { error, answer, sessionError } = plan[connections.length] ?? {};
    let up = false;
    const connection = <Connection>(<unknown>{
      connect: (cb: (err?: Error) => void) => {
        (answer ?? Promise.resolve()).then(() => {
          up = !error;
          cb(error);
        });
      },
      isUp: () => up,
      execute: ({ complete }: { complete: (err: Error | undefined, stmt: object, rows: unknown[]) => void }) => {
        complete(sessionError, {}, []);
        return {};
      },
      destroy: vi.fn((cb: () => void) => {
        up = false;
        cb();
      }),
    });
    connections.push(connection);
    return connection;
  });

  return connections;
};

const drop = (connection: Connection) => connection.destroy(() => undefined);

describe('getConnection()', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  test('callers that find the session down share one new login', async () => {
    const connections = fakeLogins();
    const driver = new TestSnowflakeDriver();
    drop(await driver.connect());

    const got = await Promise.all([driver.connect(), driver.connect(), driver.connect()]);

    expect(connections).toHaveLength(2);
    got.forEach((connection) => expect(connection).toBe(connections[1]));
  });

  test('a failed login fails every caller waiting on it, and the next call logs in again', async () => {
    const connections = fakeLogins([{}, { error: new Error('login refused') }]);
    const driver = new TestSnowflakeDriver();
    drop(await driver.connect());

    const results = await Promise.allSettled([driver.connect(), driver.connect()]);

    expect(results.map(({ status }) => status)).toEqual(['rejected', 'rejected']);
    expect(connections).toHaveLength(2);
    expect(await driver.connect()).toBe(connections[2]);
  });

  test('a session whose setup fails after login is closed, and the next call logs in again', async () => {
    const connections = fakeLogins([{ sessionError: new Error('ALTER SESSION refused') }]);
    const driver = new TestSnowflakeDriver();

    await expect(driver.connect()).rejects.toThrow('ALTER SESSION refused');

    expect(connections[0].destroy).toHaveBeenCalledTimes(1);
    expect(await driver.connect()).toBe(connections[1]);
  });

  test('a failed login leaves a newer connection in place', async () => {
    let failFirst: (() => void) | undefined;
    const connections = fakeLogins([
      { error: new Error('login refused'), answer: new Promise<void>((resolve) => { failFirst = resolve; }) },
    ]);
    const driver = new TestSnowflakeDriver();

    const failing = driver.connect();
    const releasing = driver.release();
    const newer = await driver.connect();
    failFirst?.();
    await Promise.allSettled([failing, releasing]);

    expect(await driver.connect()).toBe(newer);
    expect(connections).toHaveLength(2);
  });
});
