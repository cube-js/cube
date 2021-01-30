/* eslint-disable global-require */
import genericPool, { Pool, Options as PoolOptions } from 'generic-pool';
import AsyncRedisClient from './AsyncRedisClient';
import RedisClientOptions from './RedisClientOptions';
import { createRedisClient as createNodeRedisClient } from './RedisFactory';
import { createRedisSentinelClient } from './RedisSentinelFactory';
import config from '../config';

function createRedisClient(url: string, opts: RedisClientOptions = {}) {
  if (config.CUBEJS_REDIS_USE_IOREDIS) {
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
    const min = (typeof options.poolMin !== 'undefined') ? options.poolMin : config.CUBEJS_REDIS_POOL_MIN;
    const max = (typeof options.poolMax !== 'undefined') ? options.poolMax : config.CUBEJS_REDIS_POOL_MAX;
    const idleTimeoutSeconds = (typeof options.idleTimeoutSeconds !== 'undefined') ?
      options.idleTimeoutSeconds : config.CUBEJS_REDIS_SOFT_IDLE_TIMEOUT_SECONDS;
    const softIdleTimeoutSeconds = (typeof options.softIdleTimeoutSeconds !== 'undefined') ?
      options.softIdleTimeoutSeconds : config.CUBEJS_REDIS_SOFT_IDLE_TIMEOUT_SECONDS;

    const opts: PoolOptions = {
      min,
      max,
      acquireTimeoutMillis: 5000,
      idleTimeoutMillis: idleTimeoutSeconds * 1000,
      softIdleTimeoutMillis: softIdleTimeoutSeconds * 1000,
      evictionRunIntervalMillis: Math.min(idleTimeoutSeconds, softIdleTimeoutSeconds) * 1000
    };

    const create = options.createClient || (async () => createRedisClient(config.REDIS_URL));

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
