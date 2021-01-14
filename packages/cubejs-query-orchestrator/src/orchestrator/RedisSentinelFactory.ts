import Redis, { Redis as redis, RedisOptions, Pipeline } from 'ioredis';
import { getEnv } from '@cubejs-backend/shared';

async function createIORedisClient(url: string, opts: RedisOptions) {
  const [host, portStr] = (getEnv('redisSentinel') || url || 'localhost').replace('redis://', '').split(':');
  const port = portStr ? Number(portStr) : 6379;

  const options: RedisOptions = {
    ...opts,
    enableReadyCheck: true,
    lazyConnect: true
  };

  if (getEnv('redisSentinel')) {
    options.sentinels = [{ host, port }];
    options.name = 'mymaster';
    options.enableOfflineQueue = false;
  } else {
    options.host = host;
    options.port = port;
  }

  if (getEnv('redisTls')) {
    options.tls = {};
  }

  if (getEnv('redisPassword')) {
    options.password = getEnv('redisPassword');
  }

  const client = new Redis(options);

  return client.connect().then(() => client);
}

Pipeline.prototype.execAsync = function execAsync() {
  return this.exec()
    .then((array) => (array ? array.map((skipFirst) => skipFirst[1]) : array));
};

async function addAsyncMethods(client: redis) {
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
      client[`${k}Async`] = client[k];
    }
  );

  return client;
}

async function replaceEnd(client: redis) {
  client.end = () => client.disconnect();

  return client;
}

export function createRedisSentinelClient(url: string, opts: RedisOptions): PromiseLike<redis> {
  return createIORedisClient(url, opts)
    .then(addAsyncMethods)
    .then(replaceEnd);
}
