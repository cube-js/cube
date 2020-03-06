const genericPool = require('generic-pool');
const createRedisClient = require('./RedisFactory');

class RedisPool {
  constructor(options) {
    options = options || {};
    const defaultMin = process.env.CUBEJS_REDIS_POOL_MIN ? parseInt(process.env.CUBEJS_REDIS_POOL_MIN, 10) : 2;
    const defaultMax = process.env.CUBEJS_REDIS_POOL_MAX ? parseInt(process.env.CUBEJS_REDIS_POOL_MAX, 10) : 1000;
    const min = (typeof options.poolMin !== 'undefined') ? options.poolMin : defaultMin;
    const max = (typeof options.poolMax !== 'undefined') ? options.poolMax : defaultMax;
    const create = options.createClient || (() => createRedisClient(process.env.REDIS_URL));
    const destroy = options.destroyClient || (client => client.end(true));
    const opts = {
      min,
      max,
      acquireTimeoutMillis: 5000,
      idleTimeoutMillis: 5000,
      evictionRunIntervalMillis: 5000
    };
    if (max > 0) {
      this.pool = genericPool.createPool({ create, destroy }, opts);
    } else {
      // fallback to un-pooled behavior if pool max is 0
      this.create = create;
    }
  }

  async getClient() {
    if (this.pool) {
      return this.pool.acquire();
    } else {
      return this.create();
    }
  }

  release(client) {
    if (this.pool) {
      this.pool.release(client);
    } else if (client) {
      client.quit();
    }
  }

  async cleanup() {
    if (this.pool) {
      await this.pool.drain();
      this.pool.clear();
    }
  }
}

module.exports = RedisPool;
