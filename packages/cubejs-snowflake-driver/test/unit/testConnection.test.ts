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
  public constructor(protected readonly stub: { connectError?: Error, isUp?: boolean }) {
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
    return <Connection>(<unknown>{
      connect: (cb: (err: Error | undefined, conn: unknown) => void) => cb(this.stub.connectError, this),
      isUp: () => this.stub.isUp ?? true,
      destroy: (cb: (err: Error | undefined, conn: unknown) => void) => cb(undefined, this),
    });
  }
}

const expectNoSecrets = (e: Error) => {
  expect(e.message).not.toContain('secret');
};

describe('testConnection()', () => {
  test('wraps a connect failure with the cause and no config', async () => {
    const cause = new Error('Incorrect username or password was specified.');
    const err = await new TestSnowflakeDriver({ connectError: cause }).testConnection().catch((e) => e);

    expect(err).toBeInstanceOf(ConnectionError);
    expect(err.message).toBe('Unable to connect to the Snowflake instance: Incorrect username or password was specified.');
    expect(err.cause).toBe(cause);
    expectNoSecrets(err);
  });

  test('keeps the config out when the connection is not up', async () => {
    const err = await new TestSnowflakeDriver({ isUp: false }).testConnection().catch((e) => e);

    expect(err).toBeInstanceOf(ConnectionError);
    expectNoSecrets(err);
  });

  test('passes when the connection is up', async () => {
    await expect(new TestSnowflakeDriver({}).testConnection()).resolves.toBeUndefined();
  });
});
