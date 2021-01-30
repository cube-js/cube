import redis, { ClientOpts, RedisClient } from 'redis';
import { promisify } from 'util';
import AsyncRedisClient from './AsyncRedisClient';
import config from '../config';

function decorateRedisClient(client: RedisClient): AsyncRedisClient {
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
    'lpush',
  ].forEach(
    k => {
      client[`${k}Async`] = promisify(client[k]);
    }
  );

  return <AsyncRedisClient>client;
}

export function createRedisClient(url: string, opts: ClientOpts = {}) {
  redis.Multi.prototype.execAsync = function execAsync() {
    return new Promise((resolve, reject) => this.exec((err, res) => (
      err ? reject(err) : resolve(res)
    )));
  };

  const options: ClientOpts = {
    url,
  };

  if (config.CUBEJS_REDIS_TLS) {
    options.tls = {};
  }

  if (config.REDIS_PASSWORD) {
    options.password = config.REDIS_PASSWORD;
  }

  return Promise.resolve(decorateRedisClient(
    redis.createClient({
      ...options,
      ...opts,
    })
  ));
}
