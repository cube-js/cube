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

type Login = { error?: Error, answer?: Promise<void>, sessionError?: Error, closeHangs?: boolean };

// Sessions whose close has called back
const closed = new Set<Connection>();

// Every login the driver asks the SDK for. A login answers once `answer` settles (at once without one) and
// fails with `error` if set; the real initConnection and getConnection run on top.
const fakeLogins = (plan: Login[] = []) => {
  const connections: Connection[] = [];
  closed.clear();

  vi.spyOn(snowflake, 'createConnection').mockImplementation(() => {
    const { error, answer, sessionError, closeHangs } = plan[connections.length] ?? {};
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
      // Like the SDK: the callback comes later, and a session that already dropped answers with an error
      destroy: vi.fn((cb: (err?: Error) => void) => {
        const wasUp = up;
        up = false;
        if (closeHangs) {
          return;
        }
        setImmediate(() => {
          closed.add(connection);
          cb(wasUp ? undefined : new Error('ERR_CONN_DESTROY_STATUS_DISCONNECTED'));
        });
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

    // Checked the moment the caller sees the error: the session is closed by then, not later
    const closedWhenRefused = driver.connect().then(
      () => 'connected',
      (e) => `${e.message}, closed: ${closed.has(connections[0])}`,
    );

    expect(await closedWhenRefused).toBe('ALTER SESSION refused, closed: true');
    expect(connections[0].destroy).toHaveBeenCalledTimes(1);
    expect(await driver.connect()).toBe(connections[1]);
  });

  test('a close that never answers holds a failed setup for a few seconds at most', async () => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
    try {
      fakeLogins([{ sessionError: new Error('ALTER SESSION refused'), closeHangs: true }]);
      const refused = new TestSnowflakeDriver().connect().then(() => 'connected', (e) => e.message);

      await vi.advanceTimersByTimeAsync(5000);

      expect(await refused).toBe('ALTER SESSION refused');
    } finally {
      vi.useRealTimers();
    }
  });

  test('release() resolves without a second close when the session already dropped', async () => {
    const connections = fakeLogins();
    const driver = new TestSnowflakeDriver();
    drop(await driver.connect());

    await expect(driver.release()).resolves.toBeUndefined();
    expect(connections[0].destroy).toHaveBeenCalledTimes(1);
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
