import { describe, expect, test } from 'vitest';
import type { Connection } from 'snowflake-sdk';

import { SnowflakeDriver } from '../../src/SnowflakeDriver';
import { ConnectionError } from '../../src/errors';

const SECRETS = {
  password: 'secret-password',
  privateKey: 'secret-private-key',
  privateKeyPass: 'secret-private-key-pass',
  oauthToken: 'secret-oauth-token',
  token: 'secret-token',
};

// No Snowflake server: the connection is a stub that fails the way the test asks.
class TestSnowflakeDriver extends SnowflakeDriver {
  public destroyed = 0;

  public constructor(protected readonly stub: { createError?: Error, connectError?: Error, isUp?: boolean, destroyError?: Error }) {
    super({
      account: 'acc',
      username: 'user',
      ...SECRETS,
      exportBucket: {
        bucketType: 's3',
        bucketName: 'bucket',
        keyId: 'secret-key-id',
        secretKey: 'secret-secret-key',
        region: 'us-east-1',
      },
    });
  }

  protected async createConnection(): Promise<Connection> {
    if (this.stub.createError) {
      throw this.stub.createError;
    }

    return <Connection>(<unknown>{
      connect: (cb: (err: Error | undefined, conn: unknown) => void) => cb(this.stub.connectError, this),
      isUp: () => this.stub.isUp ?? true,
      destroy: (cb: (err: Error | undefined, conn: unknown) => void) => {
        this.destroyed++;
        cb(this.stub.destroyError, this);
      },
    });
  }
}

const expectNoSecrets = (e: Error) => {
  expect(e.message).not.toContain('secret');
};

describe('testConnection()', () => {
  test('wraps a connect failure with the cause and no config', async () => {
    const cause = new Error('Incorrect username or password was specified.');
    const driver = new TestSnowflakeDriver({ connectError: cause, destroyError: new Error('not connected') });
    const err = await driver.testConnection().catch((e) => e);

    expect(driver.destroyed).toBe(1);
    expect(err).toBeInstanceOf(ConnectionError);
    expect(err.message).toBe('Unable to connect to the Snowflake instance: Incorrect username or password was specified.');
    expect(err.cause).toBe(cause);
    expectNoSecrets(err);
  });

  test('wraps a failure to create the connection', async () => {
    const cause = new Error('A string must be specified for account.');
    const err = await new TestSnowflakeDriver({ createError: cause }).testConnection().catch((e) => e);

    expect(err).toBeInstanceOf(ConnectionError);
    expect(err.cause).toBe(cause);
    expectNoSecrets(err);
  });

  test('keeps the config out when the connection is not up', async () => {
    const driver = new TestSnowflakeDriver({ isUp: false });
    const err = await driver.testConnection().catch((e) => e);

    expect(driver.destroyed).toBe(1);
    expect(err).toBeInstanceOf(ConnectionError);
    expectNoSecrets(err);
  });

  test('passes when the connection is up', async () => {
    const driver = new TestSnowflakeDriver({});

    await expect(driver.testConnection()).resolves.toBeUndefined();
    expect(driver.destroyed).toBe(1);
  });

  test('reports a destroy failure after a successful connect', async () => {
    const destroyError = new Error('logout failed');

    await expect(new TestSnowflakeDriver({ destroyError }).testConnection()).rejects.toBe(destroyError);
  });
});
