import { describe, expect, test } from 'vitest';
import type { Connection } from 'snowflake-sdk';

import { SnowflakeDriver } from '../../src/SnowflakeDriver';

// No Snowflake connection: the point is which credentials a new connection would log in with.
class TestSnowflakeDriver extends SnowflakeDriver {
  public constructor() {
    super({ account: 'acct', username: 'service_user', password: 'service_password', role: 'CUBE_SERVICE' });
  }

  public connectOptions() {
    return this.prepareConnectOptions();
  }

  public holdConnection(destroyed: string[]) {
    const connection = <Connection>(<unknown>{
      destroy: (cb: () => void) => {
        destroyed.push('connection');
        cb();
      },
    });
    this.connection = Promise.resolve(connection);
  }

  public hasConnection() {
    return this.connection !== null;
  }
}

describe('setOAuthToken()', () => {
  test('makes new connections log in with the token over OAuth, without the configured user', async () => {
    const driver = new TestSnowflakeDriver();

    expect(await driver.connectOptions()).toMatchObject({ username: 'service_user', password: 'service_password' });

    driver.setOAuthToken('token-1');
    const options = await driver.connectOptions();

    expect(options).toMatchObject({ account: 'acct', authenticator: 'OAUTH', token: 'token-1' });
    expect(options).not.toHaveProperty('username');
    expect(options).not.toHaveProperty('password');
    expect(options).not.toHaveProperty('role');

    driver.setOAuthToken('token-2');
    expect(await driver.connectOptions()).toMatchObject({ token: 'token-2' });
  });

  test('drops a connection opened with the configured credentials on the first token only', async () => {
    const destroyed: string[] = [];
    const driver = new TestSnowflakeDriver();

    driver.holdConnection(destroyed);
    driver.setOAuthToken('token-1');
    await Promise.resolve();

    expect(driver.hasConnection()).toBe(false);
    expect(destroyed).toEqual(['connection']);

    // A newer token for the same principal keeps the open session
    driver.holdConnection(destroyed);
    driver.setOAuthToken('token-2');
    await Promise.resolve();

    expect(driver.hasConnection()).toBe(true);
    expect(destroyed).toEqual(['connection']);
  });
});
