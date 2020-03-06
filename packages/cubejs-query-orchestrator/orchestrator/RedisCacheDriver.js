class RedisCacheDriver {
  constructor({ pool }) {
    this.redisPool = pool;
  }

  async getClient() {
    return this.redisPool.getClient();
  }

  async get(key) {
    const client = await this.getClient();
    try {
      const res = await client.getAsync(key);
      return res && JSON.parse(res);
    } finally {
      this.redisPool.release(client);
    }
  }

  async set(key, value, expiration) {
    const client = await this.getClient();
    try {
      return await client.setAsync(key, JSON.stringify(value), 'EX', expiration);
    } finally {
      this.redisPool.release(client);
    }
  }

  async remove(key) {
    const client = await this.getClient();
    try {
      return await client.delAsync(key);
    } finally {
      this.redisPool.release(client);
    }
  }

  async keysStartingWith(prefix) {
    const client = await this.getClient();
    try {
      return await client.keysAsync(`${prefix}*`);
    } finally {
      this.redisPool.release(client);
    }
  }
}

module.exports = RedisCacheDriver;
