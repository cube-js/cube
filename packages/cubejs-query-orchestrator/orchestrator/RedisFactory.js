const redis = require('redis');
const { promisify } = require('util');

module.exports = function createRedisClient(url) {
  redis.Multi.prototype.execAsync = promisify(redis.Multi.prototype.exec);

  const client = redis.createClient(url);

  ['brpop', 'del', 'exec', 'get', 'hget', 'rpop', 'set', 'zadd', 'zrange', 'zrangebyscore'].forEach(
    k => client[k] = promisify(client[k])
  );

  return client;
}
