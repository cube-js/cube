const redis = require('redis');
const { promisify } = require('util');

module.exports = function createRedisClient(url) {
  redis.Multi.prototype.execAsync = promisify(redis.Multi.prototype.exec);

  let options;

  if (process.env.REDIS_TLS === 'true') {
    options = {
      url,
      tls: {}
    };
  }

  const client = redis.createClient(options || url);

  ['brpop', 'del', 'get', 'hget', 'rpop', 'set', 'zadd', 'zrange', 'zrangebyscore', 'keys'].forEach(
    k => {
      client[`${k}Async`] = promisify(client[k]);
    }
  );

  return client;
};
