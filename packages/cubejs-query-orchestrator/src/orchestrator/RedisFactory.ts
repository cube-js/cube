import Redis, { RedisOptions } from 'ioredis';

export async function createRedisClient(url: string): Promise<Redis.Redis> {
  const options: RedisOptions = {};

  const [host, portStr] = (process.env.REDIS_SENTINEL || url || 'localhost').replace('redis://', '').split(':');
  const port = portStr ? Number(portStr) : 6379;

  if (process.env.REDIS_SENTINEL) {
    options.sentinels = [{ host, port }];
    options.name = 'mymaster';
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

  return Promise.resolve(new Redis(options));
}
