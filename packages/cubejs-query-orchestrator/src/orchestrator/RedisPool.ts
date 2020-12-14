import genericPool, { Pool, Options as PoolOptions } from 'generic-pool';
import type { RedisClient } from 'redis';

import { createRedisClient } from './RedisFactory';

export type CreateRedisClientFn = () => PromiseLike<RedisClient>;

export interface RedisPoolOptions {
  poolMin?: number;
  poolMax?: number;
  createClient?: CreateRedisClientFn;
  destroyClient?: (client: RedisClient) => PromiseLike<void>;
}

export class RedisPool {
  protected readonly pool: Pool<RedisClient>|null = null;

  protected readonly create: CreateRedisClientFn|null = null;

  public constructor(options: RedisPoolOptions = {}) {
    const defaultMin = process.env.CUBEJS_REDIS_POOL_MIN ? parseInt(process.env.CUBEJS_REDIS_POOL_MIN, 10) : 2;
    const defaultMax = process.env.CUBEJS_REDIS_POOL_MAX ? parseInt(process.env.CUBEJS_REDIS_POOL_MAX, 10) : 1000;
    const min = (typeof options.poolMin !== 'undefined') ? options.poolMin : defaultMin;
    const max = (typeof options.poolMax !== 'undefined') ? options.poolMax : defaultMax;

    const opts: PoolOptions = {
      min,
      max,
      acquireTimeoutMillis: 5000,
      idleTimeoutMillis: 5000,
      evictionRunIntervalMillis: 5000
    };

    const create = options.createClient || (async () => createRedisClient(process.env.REDIS_URL));

    if (max > 0) {
      const destroy = options.destroyClient || (async (client) => client.end(true));

      this.pool = genericPool.createPool<RedisClient>({ create, destroy }, opts);
    } else {
      // fallback to un-pooled behavior if pool max is 0
      this.create = create;
    }
  }

  public async getClient() {
    if (this.pool) {
      return this.pool.acquire();
    } else {
      return this.create();
    }
  }

  public release(client) {
    if (this.pool) {
      this.pool.release(client);
    } else if (client) {
      client.quit();
    }
  }

  public async cleanup() {
    if (this.pool) {
      await this.pool.drain();
      this.pool.clear();
    }
  }
}
