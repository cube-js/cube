const createRedisClient = require('./RedisFactory');

class RedisCacheDriver {
  constructor() {
    this.redisClient = createRedisClient(process.env.REDIS_URL);
  }

  async get(key) {
    const res = await this.redisClient.get(key);
    return res && JSON.parse(res);
  }

  set(key, value, expiration) {
    return this.redisClient.set(key, JSON.stringify(value), 'EX', expiration);
  }

  remove(key) {
    return this.redisClient.del(key);
  }
}

module.exports = RedisCacheDriver;
