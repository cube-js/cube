import Redis, { Redis as redis, RedisOptions, Pipeline } from 'ioredis';

import config from '../config';

function debugLog(msg) {
  if (config.FLAG_ENABLE_REDIS_SENTINEL_DEBUG) {
    console.debug(msg);
  }
}

async function createIORedisClient(url: string, opts: RedisOptions) {
  const [host, portStr] = (config.REDIS_SENTINEL || url || 'localhost').replace('redis://', '').split(':');
  const port = portStr ? Number(portStr) : 6379;

  const options: RedisOptions = {
    ...opts,
    enableReadyCheck: true,
    lazyConnect: true
  };

  if (config.REDIS_SENTINEL) {
    options.sentinels = [{ host, port }];
    options.name = 'mymaster';
    options.enableOfflineQueue = false;
  } else {
    options.host = host;
    options.port = port;
  }

  if (config.REDIS_TLS) {
    options.tls = {};
  }

  if (config.REDIS_PASSWORD) {
    options.password = config.REDIS_PASSWORD;
  }

  const client = new Redis(options);

  client.on('connect', () => {
    debugLog('Redis connection established');
  });

  client.on('ready', () => {
    debugLog('Redis ready');
  });

  client.on('close', () => {
    debugLog('Redis connection closed');
  });

  client.on('end', () => {
    debugLog('Redis connection ended');
  });

  client.on('error', (e) => {
    console.error('Redis connection failed: ', e);
  });

  client.on('reconnecting', (times) => {
    console.warn('Redis connection is being reconnected, attempt no: ', times);
  });

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
