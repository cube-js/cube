import { createCancelablePromise, MaybeCancelablePromise } from '@cubejs-backend/shared';
import { CacheDriverInterface } from '@cubejs-backend/base-driver';

import { RedisPool } from './RedisPool';

interface RedisCacheDriverOptions {
  pool: RedisPool,
}

export class RedisCacheDriver implements CacheDriverInterface {
  protected readonly redisPool: RedisPool;

  public constructor({ pool }: RedisCacheDriverOptions) {
    this.redisPool = pool;
  }

  protected async getClient() {
    return this.redisPool.getClient();
  }

  public async get(key: string) {
    const client = await this.getClient();

    try {
      const res = await client.getAsync(key);
      return res && JSON.parse(res);
    } finally {
      this.redisPool.release(client);
    }
  }

  public withLock = (
    key: string,
    cb: () => MaybeCancelablePromise<any>,
    expiration: number = 60,
    freeAfter: boolean = true,
  ) => createCancelablePromise(async (tkn) => {
    const client = await this.getClient();

    try {
      if (tkn.isCanceled()) {
        return false;
      }

      const response = await client.setAsync(
        key,
        '1',
        // Only set the key if it does not already exist.
        'NX',
        'EX',
        expiration
      );

      if (response === 'OK') {
        if (tkn.isCanceled()) {
          return false;
        }

        try {
          await tkn.with(cb());
        } finally {
          if (freeAfter) {
            await client.delAsync(key);
          }
        }

        return true;
      }

      return false;
    } finally {
      this.redisPool.release(client);
    }
  });

  public async set(key: string, value, expiration) {
    const client = await this.getClient();

    try {
      const strValue = JSON.stringify(value);
      await client.setAsync(key, strValue, 'EX', expiration);
      return {
        key,
        bytes: Buffer.byteLength(strValue),
      };
    } finally {
      this.redisPool.release(client);
    }
  }

  public async remove(key: string) {
    const client = await this.getClient();

    try {
      return await client.delAsync(key);
    } finally {
      this.redisPool.release(client);
    }
  }

  public async keysStartingWith(prefix: string) {
    const client = await this.getClient();

    try {
      return await client.keysAsync(`${prefix}*`);
    } finally {
      this.redisPool.release(client);
    }
  }

  public async cleanup(): Promise<void> {
    return this.redisPool.cleanup();
  }

  public async testConnection(): Promise<void> {
    const client = await this.getClient();

    try {
      await client.ping();
    } finally {
      this.redisPool.release(client);
    }
  }
}
