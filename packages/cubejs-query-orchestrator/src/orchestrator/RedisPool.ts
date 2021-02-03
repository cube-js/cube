/* eslint-disable global-require */
import genericPool, { Pool, Options as PoolOptions } from 'generic-pool';
import { getEnv } from '@cubejs-backend/shared';
import AsyncRedisClient from './AsyncRedisClient';
import RedisClientOptions from './RedisClientOptions';
import { createRedisClient as createNodeRedisClient } from './RedisFactory';
import { createRedisSentinelClient } from './RedisSentinelFactory';

function createRedisClient(url: string, opts: RedisClientOptions = {}) {
  if (getEnv('redisUseIORedis')) {
    return createRedisSentinelClient(url, opts);
  }

  return createNodeRedisClient(url, opts);
}

export type CreateRedisClientFn = () => PromiseLike<AsyncRedisClient>;

export interface RedisPoolOptions {
  poolMin?: number;
  poolMax?: number;
  idleTimeoutSeconds?: number;
  softIdleTimeoutSeconds?: number;
  createClient?: CreateRedisClientFn;
  destroyClient?: (client: AsyncRedisClient) => PromiseLike<void>;
}

const MAX_ALLOWED_POOL_ERRORS = 100;

export class RedisPool {
  protected readonly pool: Pool<AsyncRedisClient>|null = null;

  protected readonly create: CreateRedisClientFn|null = null;

  protected poolErrors: number = 0;

  public constructor(options: RedisPoolOptions = {}) {
    const min = (typeof options.poolMin !== 'undefined') ? options.poolMin : getEnv('redisPoolMin');
    const max = (typeof options.poolMax !== 'undefined') ? options.poolMax : getEnv('redisPoolMax');

    const opts: PoolOptions = {
      min,
      max,
      acquireTimeoutMillis: 5000,
      idleTimeoutMillis: 5000,
      evictionRunIntervalMillis: 5000
    };

    const create = options.createClient || (async () => createRedisClient(getEnv('redisUrl')));

    if (max > 0) {
      const destroy = options.destroyClient || (async (client) => client.end());

      this.pool = genericPool.createPool<AsyncRedisClient>({ create, destroy }, opts);

      this.pool.on('factoryCreateError', (error) => {
        this.poolErrors++;
        // prevent the infinite loop when pool creation fails too many times
        if (this.poolErrors > MAX_ALLOWED_POOL_ERRORS) {
          // @ts-ignore
          // eslint-disable-next-line
          this.pool._waitingClientsQueue.dequeue().reject(error);
        }
      });
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

  public async testConnection() {
    const client = await this.getClient();

    try {
      await client.ping();
    } finally {
      this.release(client);
    }
  }

  public async cleanup() {
    if (this.pool) {
      await this.pool.drain();
      this.pool.clear();
    }
  }
}
