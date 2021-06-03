import { RedisClient, Commands, Callback } from 'redis';

interface Multi extends Commands<Multi> {
  exec(cb?: Callback<any[]>): boolean;
  EXEC(cb?: Callback<any[]>): boolean;

  // eslint-disable-next-line camelcase
  exec_atomic(cb?: Callback<any[]>): boolean;
  EXEC_ATOMIC(cb?: Callback<any[]>): boolean;

  execAsync: <T = any>() => Promise<T>,
}

interface AsyncRedisClient extends RedisClient {
  evalAsync: Commands<Promise<any>>['eval'],
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
  // @todo Improve types
  multi: () => Multi,
  end: (flush?: boolean) => void,
}

export default AsyncRedisClient;
