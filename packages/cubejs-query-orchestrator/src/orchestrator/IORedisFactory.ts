import Redis, { ClusterNode, Redis as redis, RedisOptions } from 'ioredis';
import { getEnv } from '@cubejs-backend/shared';
import AsyncRedisClient from './AsyncRedisClient';
import { parseRedisUrl } from './utils';

export type IORedisOptions = RedisOptions & { isCluster?: boolean };

// @ts-ignore
Redis.Pipeline.prototype.execAsync = function execAsync() {
  return this.exec()
    .then((array) => (array ? array.map((skipFirst) => skipFirst[1]) : array));
};

function decorateRedisClient(client: redis | Redis.Cluster): AsyncRedisClient {
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

export async function createIORedisClient(url: string, opts: RedisOptions, isCluster?: boolean): Promise<AsyncRedisClient> {
  const clusterNodes: ClusterNode[] = [];
  const options: RedisOptions = {
    enableReadyCheck: true,
    lazyConnect: true,
  };

  if (!isCluster) {
    const parsedUrl = parseRedisUrl(url);
    if (parsedUrl.sentinels) {
      options.sentinels = parsedUrl.sentinels;
      options.name = parsedUrl.name;
      options.db = parsedUrl.db;
      options.enableOfflineQueue = false;
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
  } else {
    const parsedUrl = parseRedisUrl(url);
    options.username = parsedUrl.username;
    options.password = parsedUrl.password;
    options.host = parsedUrl.host;
    options.port = parsedUrl.port;
    options.path = parsedUrl.path;
    options.db = parsedUrl.db;
    parsedUrl.clusterNodes?.forEach(clusterNode => {
      clusterNodes.push({
        host: clusterNode.host,
        port: clusterNode.port,
      });
    });
  }

  if (getEnv('redisTls')) {
    options.tls = {};
  }

  const password = getEnv('redisPassword');
  if (password) {
    options.password = password;
  }

  const client = isCluster ? new Redis.Cluster(clusterNodes, {
    redisOptions: options,
  }) : new Redis({
    ...options,
    ...opts,
  });
  await client.connect();

  return decorateRedisClient(client);
}
