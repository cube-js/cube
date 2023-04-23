import { brotliCompress, brotliDecompress } from 'zlib';
import { promisify } from 'util';

import { createCancelablePromise, getEnv, MaybeCancelablePromise } from '@cubejs-backend/shared';
import { CacheDriverInterface } from '@cubejs-backend/base-driver';

import { CubeStoreDriver } from './CubeStoreDriver';

const brotliCompressAsync = promisify(brotliCompress);
const brotliDecompressAsync = promisify(brotliDecompress);

export class CubeStoreCacheDriver implements CacheDriverInterface {
  protected readonly compression: boolean = getEnv('cubestoreCompression');

  public constructor(
    protected connectionFactory: () => Promise<CubeStoreDriver>,
  ) {}

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
    const rows = await (await this.getConnection()).query('CACHE GET ?', [
      key
    ], {
      sendParameters: getEnv('cubestoreSendableParameters')
    });
    if (rows && rows.length === 1) {
      console.log(rows);
      return this.deserializePayload(rows[0].value);
    }

    return null;
  }

  protected async deserializePayload(value: string) {
    if (value === null) {
      return value;
    }

    if (this.compression) {
      console.log(value);

      const payload = await brotliDecompressAsync(Buffer.from(value), {});
      return JSON.parse(payload.toString('utf-8'));
    }

    return JSON.parse(value);
  }

  protected async serializePayload(value: unknown) {
    const payload = JSON.stringify(value);

    if (this.compression) {
      const buffer = await brotliCompressAsync(Buffer.from(JSON.stringify(value)), {});

      console.log('utf length', buffer.toString('utf-8').length);
      console.log('binary length', buffer.toString('binary').length);
      console.log('base64 length', buffer.toString('base64').length);

      return buffer;
    }

    return payload;
  }

  public async set(key: string, value: unknown, expiration: number) {
    const payload = await this.serializePayload(value);
    console.log(payload);
    await (await this.getConnection()).query('CACHE SET TTL ? ? ?', [expiration, key, payload], {
      sendParameters: getEnv('cubestoreSendableParameters')
    });

    return {
      key,
      bytes: Buffer.byteLength(payload),
    };
  }

  public async remove(key: string) {
    await (await this.getConnection()).query('CACHE REMOVE ?', [
      key
    ]);
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
