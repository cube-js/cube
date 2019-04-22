const createRedisClient = require('./RedisFactory');

class RedisCacheDriver {
  constructor() {
    this.redisClient = createRedisClient(process.env.REDIS_URL);
  }

  async get(key) {
    const res = await this.redisClient.getAsync(key);
    return res && JSON.parse(res);
  }

  set(key, value, expiration) {
    return this.redisClient.setAsync(key, JSON.stringify(value), 'EX', expiration);
  }

  remove(key) {
    return this.redisClient.delAsync(key);
  }
}

module.exports = RedisCacheDriver;
