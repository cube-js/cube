import { createCancelablePromise, MaybeCancelablePromise } from '@cubejs-backend/shared';
import { CacheDriverInterface } from '@cubejs-backend/base-driver';

import { CubeStoreDriver } from './CubeStoreDriver';

export class CubeStoreCacheDriver implements CacheDriverInterface {
  public constructor(
    protected readonly connection: CubeStoreDriver
  ) {}

  public withLock = (
    key: string,
    cb: () => MaybeCancelablePromise<any>,
    expiration: number = 60,
    freeAfter: boolean = true,
  ) => createCancelablePromise(async (tkn) => {
    if (tkn.isCanceled()) {
      return false;
    }

    const rows = await this.connection.query('CACHE SET NX TTL ? ? ?', [expiration, key, '1']);
    if (rows && rows.length === 1 && rows[0]?.success === 'true') {
      if (tkn.isCanceled()) {
        if (freeAfter) {
          await this.connection.query('CACHE REMOVE ?', [
            key
          ]);
        }

        return false;
      }

      try {
        await tkn.with(cb());
      } finally {
        if (freeAfter) {
          await this.connection.query('CACHE REMOVE ?', [
            key
          ]);
        }
      }

      return true;
    }

    return false;
  });

  public async get(key: string) {
    const rows = await this.connection.query('CACHE GET ?', [
      key
    ]);
    if (rows && rows.length === 1) {
      return JSON.parse(rows[0].value);
    }

    return null;
  }

  public async set(key: string, value, expiration) {
    const strValue = JSON.stringify(value);
    await this.connection.query('CACHE SET TTL ? ? ?', [expiration, key, strValue]);

    return {
      key,
      bytes: Buffer.byteLength(strValue),
    };
  }

  public async remove(key: string) {
    await this.connection.query('CACHE REMOVE ?', [
      key
    ]);
  }

  public async keysStartingWith(prefix: string) {
    const rows = await this.connection.query('CACHE KEYS ?', [
      prefix
    ]);
    return rows.map((row) => row.key);
  }

  public async cleanup(): Promise<void> {
    //
  }

  public async testConnection(): Promise<void> {
    return this.connection.testConnection();
  }
}
