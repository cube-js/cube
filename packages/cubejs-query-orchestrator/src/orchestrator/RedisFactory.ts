import redis, { ClientOpts, RedisClient, Commands } from 'redis';
import { promisify } from 'util';

export interface AsyncRedisClient extends RedisClient {
  brpopAsync: Commands<Promise<any>>['brpop'],
  delAsync: Commands<Promise<any>>['del'],
  getAsync: Commands<Promise<any>>['get'],
  hgetAsync: Commands<Promise<any>>['hget'],
  rpopAsync: Commands<Promise<any>>['rpop'],
  setAsync: Commands<Promise<any>>['set'],
  zaddAsync: Commands<Promise<any>>['zadd'],
  zrangeAsync: Commands<Promise<any>>['zrange'],
  zrangebyscoreAsync: Commands<Promise<any>>['zrangebyscore'],
  keysAsync: Commands<Promise<any>>['keys'],
  watchAsync: Commands<Promise<any>>['watch'],
  unwatchAsync: Commands<Promise<any>>['unwatch'],
  incrAsync: Commands<Promise<any>>['incr'],
  decrAsync: Commands<Promise<any>>['decr'],
  lpushAsync: Commands<Promise<any>>['lpush'],
}

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

  return <any>client;
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

  if (process.env.REDIS_TLS === 'true') {
    options.tls = {};
  }

  if (process.env.REDIS_PASSWORD) {
    options.password = process.env.REDIS_PASSWORD;
  }

  return decorateRedisClient(
    redis.createClient({
      ...options,
      ...opts,
    })
  );
}
