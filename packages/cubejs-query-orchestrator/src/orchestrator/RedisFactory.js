const redis = require('redis');
const { promisify } = require('util');

module.exports = function createRedisClient(url) {
  redis.Multi.prototype.execAsync = function execAsync() {
    return new Promise((resolve, reject) => this.exec((err, res) => (
      err ? reject(err) : resolve(res)
    )));
  };

  const options = {
    url,
  };

  if (process.env.REDIS_TLS === 'true') {
    options.tls = {};
  }

  if (process.env.REDIS_PASSWORD) {
    options.password = process.env.REDIS_PASSWORD;
  }

  const client = redis.createClient(options);

  [
    'brpop',
    'del',
    'get',
    'hget',
    'rpop',
    'set',
    'zadd',
    'zrange',
    'zrangebyscore',
    'keys',
    'watch',
    'unwatch',
    'incr',
    'decr',
    'lpush'
  ].forEach(
    k => {
      client[`${k}Async`] = promisify(client[k]);
    }
  );

  return client;
};
