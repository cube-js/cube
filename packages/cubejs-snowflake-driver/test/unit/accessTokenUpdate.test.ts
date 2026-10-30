import { afterEach, describe, expect, test, vi } from 'vitest';
import snowflake, { type Connection } from 'snowflake-sdk';

import { SnowflakeDriver } from '../../src/SnowflakeDriver';

// No Snowflake connection: the point is which credentials a new connection would log in with.
class TestSnowflakeDriver extends SnowflakeDriver {
  public constructor() {
    super({ account: 'acct', username: 'service_user', password: 'service_password', role: 'CUBE_SERVICE' });
  }

  public connectOptions() {
    return this.prepareConnectOptions();
  }

  public holdConnection(destroyed: string[], destroyError?: Error) {
    const connection = <Connection>(<unknown>{
      isUp: () => true,
      destroy: (cb: (err?: Error) => void) => {
        destroyed.push('connection');
        cb(destroyError);
      },
    });
    this.connection = Promise.resolve(connection);
  }

  public holdLogin(login: Promise<Connection>) {
    this.connection = login;
  }

  public hasConnection() {
    return this.connection !== null;
  }
}

describe('updateAccessToken()', () => {
  test('is advertised on the driver', () => {
    expect(new TestSnowflakeDriver().supportsAccessTokenUpdate()).toBe(true);
  });

  test('makes new connections log in with the token over OAuth, without the configured user', async () => {
    const driver = new TestSnowflakeDriver();

    expect(await driver.connectOptions()).toMatchObject({ username: 'service_user', password: 'service_password' });

    driver.updateAccessToken('token-1');
    const options = await driver.connectOptions();

    expect(options).toMatchObject({ account: 'acct', authenticator: 'OAUTH', token: 'token-1' });
    expect(options).not.toHaveProperty('username');
    expect(options).not.toHaveProperty('password');
    expect(options).not.toHaveProperty('role');

    driver.updateAccessToken('token-2');
    expect(await driver.connectOptions()).toMatchObject({ token: 'token-2' });
  });

  test('refuses an empty token, and a first token after a connection with the configured credentials', async () => {
    const driver = new TestSnowflakeDriver();

    expect(() => driver.updateAccessToken('')).toThrow(/non-empty token/);

    driver.holdConnection([]);
    expect(() => driver.updateAccessToken('token-1')).toThrow(/before the driver opens a connection/);
    expect(await driver.connectOptions()).toMatchObject({ username: 'service_user' });
  });

  test('keeps the open session when a newer token for the same principal arrives', async () => {
    const destroyed: string[] = [];
    const driver = new TestSnowflakeDriver();

    driver.updateAccessToken('token-1');
    driver.holdConnection(destroyed);
    driver.updateAccessToken('token-2');

    expect(driver.hasConnection()).toBe(true);
    expect(destroyed).toEqual([]);
    expect(await driver.connectOptions()).toMatchObject({ token: 'token-2' });
  });
});

describe('release()', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  // Every login attempt reaches the SDK here, so a refused one never gets this far
  const countLogins = () => vi.spyOn(snowflake, 'createConnection').mockImplementation(() => {
    throw new Error('logged in again');
  });

  test('waits for the session to close, reports a failed close, and closes it once', async () => {
    const destroyed: string[] = [];
    const driver = new TestSnowflakeDriver();
    driver.holdConnection(destroyed, new Error('logout failed'));

    await expect(driver.release()).rejects.toThrow('logout failed');
    await driver.release();

    expect(destroyed).toEqual(['connection']);
  });

  test('resolves when the login in flight fails, since that opened no session', async () => {
    let refuse: (error: Error) => void = () => undefined;
    const driver = new TestSnowflakeDriver();
    driver.holdLogin(new Promise<Connection>((_, reject) => { refuse = reject; }));

    const releasing = driver.release();
    refuse(new Error('Invalid OAuth access token'));

    await expect(releasing).resolves.toBeUndefined();
  });

  test('a driver with an access token opens no new connection after it', async () => {
    const logins = countLogins();
    const driver = new TestSnowflakeDriver();
    driver.updateAccessToken('token-1');
    driver.holdConnection([]);

    await driver.release();

    await expect(driver.query('select 1')).rejects.toThrow(/was released/);
    await expect(driver.testConnection()).rejects.toThrow(/was released/);
    expect(logins).not.toHaveBeenCalled();
  });

  test('a driver without an access token connects again on its next use', async () => {
    const logins = countLogins();
    const driver = new TestSnowflakeDriver();
    driver.holdConnection([]);

    await driver.release();

    await expect(driver.query('select 1')).rejects.toThrow('logged in again');
    expect(logins).toHaveBeenCalledTimes(1);
  });
});
