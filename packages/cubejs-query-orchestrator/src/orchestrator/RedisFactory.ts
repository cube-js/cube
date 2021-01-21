import Redis, { RedisOptions } from 'ioredis';

export async function createRedisClient(url: string): Promise<Redis.Redis> {
  const [host, portStr] = (process.env.REDIS_SENTINEL || url || 'localhost').replace('redis://', '').split(':');
  const port = portStr ? Number(portStr) : 6379;

  const options: RedisOptions = {
    enableReadyCheck: true,
    lazyConnect: true
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

  client.on("connect", () => {
    console.debug(`Redis connection established`);
  });

  client.on("ready", () => {
    console.debug(`Redis ready`);
  });

  client.on("close", () => {
    console.debug(`Redis connection closed`);
  });

  client.on("end", () => {
    console.debug(`Redis connection ended`);
  });

  client.on("error", (e) => {
    console.error(`Redis connection failed: `, e);
  });

  client.on('reconnecting', (times) => {
    console.warn('Redis connection is being reconnected, attempt no: ', times);
  });

  return client.connect().then(() => client);
}
