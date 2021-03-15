import Redis, { Redis as redis, RedisOptions } from 'ioredis';
import { getEnv, LoggerFn } from '@cubejs-backend/shared';
import AsyncRedisClient from './AsyncRedisClient';
import { parseRedisUrl } from './utils';

export type IORedisOptions = RedisOptions;

// @ts-ignore
Redis.Pipeline.prototype.execAsync = function execAsync() {
  return this.exec()
    .then((array) => (array ? array.map((skipFirst) => skipFirst[1]) : array));
};

function decorateRedisClient(client: redis): AsyncRedisClient {
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

  (<any>client).end = () => client.disconnect();

  return <any>client;
}

export async function createIORedisClient(url: string, opts: RedisOptions, logger: LoggerFn = () => { /* noop */ }):
    Promise<AsyncRedisClient> {
  const options: RedisOptions = {
    enableReadyCheck: true,
    lazyConnect: true
  };

  const parsedUrl = parseRedisUrl(url);
  if (parsedUrl.sentinels) {
    options.sentinels = parsedUrl.sentinels;
    options.name = parsedUrl.name;
    options.db = parsedUrl.db;
    options.enableOfflineQueue = false;
    options.maxRetriesPerRequest = 1;
    options.reconnectOnError = (e) => {
      logger('Requesting reconnect', { error: e });
      return true;
    };
  } else {
    options.username = parsedUrl.username;
    options.password = parsedUrl.password;
    options.host = parsedUrl.host;
    options.port = parsedUrl.port;
    options.path = parsedUrl.path;
    options.db = parsedUrl.db;

    if (parsedUrl.ssl) {
      options.tls = {};
    }
  }

  if (getEnv('redisTls')) {
    options.tls = {};
  }

  const password = getEnv('redisPassword');
  if (password) {
    options.password = password;
  }

  const client = new Redis({
    ...options,
    ...opts,
  });

  client.on('connect', () => logger('IORedis connected'));
  client.on('error', (e) => logger('IORedis error', { error: e }));
  client.on('ready', () => logger('IORedis is ready'));
  client.on('close', () => logger('IORedis closed'));
  client.on('end', () => () => logger('IORedis ended'));
  client.on('reconnecting', (time: number) => logger('IORedis sent connect event', { warning: `IORedis reconnecting in ${time}` }));

  await client.connect();

  return decorateRedisClient(client);
}
