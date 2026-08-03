import { createCancelablePromise, getEnv, MaybeCancelablePromise } from '@cubejs-backend/shared';
import { CacheDriverInterface } from '@cubejs-backend/base-driver';

import { CubeStoreDriver } from './CubeStoreDriver';

export class CubeStoreCacheDriver implements CacheDriverInterface {
  protected readonly sendParameters: boolean;

  public constructor(
    protected connectionFactory: () => Promise<CubeStoreDriver>,
  ) {
    this.sendParameters = getEnv('cubestoreSendableParameters');
  }

  protected connection: CubeStoreDriver | null = null;

  protected async getConnection(): Promise<CubeStoreDriver> {
    if (this.connection) {
      return this.connection;
    }

    // eslint-disable-next-line no-return-assign
    return this.connection = await this.connectionFactory();
  }

  public withLock = (
    key: string,
    cb: () => MaybeCancelablePromise<any>,
    expiration: number = 60,
    freeAfter: boolean = true,
  ) => createCancelablePromise(async (tkn) => {
    if (tkn.isCanceled()) {
      return false;
    }

    const connection = (await this.getConnection());

    const rows = await connection.query('CACHE SET NX TTL ? ? ?', [expiration, key, '1']);
    if (rows && rows.length === 1 && rows[0]?.success === 'true') {
      if (tkn.isCanceled()) {
        if (freeAfter) {
          await connection.query('CACHE REMOVE ?', [
            key
          ]);
        }

        return false;
      }

      try {
        await tkn.with(cb());
      } finally {
        if (freeAfter) {
          await connection.query('CACHE REMOVE ?', [
            key
          ]);
        }
      }

      return true;
    }

    return false;
  });

  public async get(key: string) {
    const connection = await this.getConnection();
    const rows = await connection.query('CACHE GET ?', [
      key
    ], {
      sendParameters: this.sendParameters && await connection.hasCapability('sendableParameters')
    });
    if (rows && rows.length === 1) {
      return JSON.parse(rows[0].value);
    }

    return null;
  }

  public async set(key: string, value, expiration) {
    const strValue = JSON.stringify(value);
    const connection = await this.getConnection();
    await connection.query('CACHE SET TTL ? ? ?', [expiration, key, strValue], {
      sendParameters: this.sendParameters && await connection.hasCapability('sendableParameters')
    });

    return {
      key,
      bytes: Buffer.byteLength(strValue),
    };
  }

  public async remove(key: string) {
    const connection = await this.getConnection();
    await connection.query('CACHE REMOVE ?', [key], {
      sendParameters: this.sendParameters && await connection.hasCapability('sendableParameters')
    });
  }

  public async keysStartingWith(prefix: string) {
    const rows = await (await this.getConnection()).query('CACHE KEYS ?', [
      prefix
    ]);
    return rows.map((row) => row.key);
  }

  public async cleanup(): Promise<void> {
    //
  }

  public async testConnection(): Promise<void> {
    return (await this.getConnection()).testConnection();
  }
}
