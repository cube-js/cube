import Redis, { Redis as redis, RedisOptions, Pipeline } from 'ioredis';

function debugLog(msg) {
  if (process.env.FLAG_ENABLE_REDIS_SENTINEL_DEBUG) {
    console.debug(msg);
  }
}

async function createIORedisClient(url: string, opts: RedisOptions) {
  const [host, portStr] = (process.env.REDIS_SENTINEL || url || 'localhost').replace('redis://', '').split(':');
  const port = portStr ? Number(portStr) : 6379;

  const options: RedisOptions = {
    ...opts
  };

  if (process.env.REDIS_SENTINEL) {
    options.sentinels = [{ host, port }];
    options.name = 'mymaster';
    options.enableOfflineQueue = false;
  } else {
    options.host = host;
    options.port = port;
  }

  if (process.env.REDIS_TLS === 'true') {
    options.tls = {};
  }

  if (process.env.REDIS_PASSWORD) {
    options.password = process.env.REDIS_PASSWORD;
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

  return client;
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

export function createRedisSentinelClient(url: string, opts: RedisOptions): PromiseLike<redis> {
  return createIORedisClient(url, opts)
    .then(addAsyncMethods);
}
