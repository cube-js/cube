const genericPool = require('generic-pool');
const createRedisClient = require('./RedisFactory');

class RedisPool {
  constructor(poolMin, poolMax, createClient, destroyClient) {
    const min = (typeof poolMin !== 'undefined') ? poolMin : parseInt(process.env.CUBEJS_REDIS_POOL_MIN, 10) || 2;
    const max = (typeof poolMax !== 'undefined') ? poolMax : parseInt(process.env.CUBEJS_REDIS_POOL_MAX, 10) || 1000;
    const create = createClient || (() => createRedisClient(process.env.REDIS_URL));
    const destroy = destroyClient || (client => client.end(true));
    const opts = { min, max, acquireTimeoutMillis: 5000, idleTimeoutMillis: 5000 }
    if (max > 0) {
      this.pool = genericPool.createPool({ create, destroy }, opts);
    } else {
      // fallback to un-pooled behavior if pool max is 0
      this.create = create;
    }
  }

  async getClient() {
    if (this.pool) {
      return await this.pool.acquire();
    } else {
      return this.create();
    }
  }

  release(client) {
    if (this.pool) {
      this.pool.release(client);
    } else {
      if (client) {
        client.quit();
      }
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
