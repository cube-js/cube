import { RedisClient, Commands } from 'redis';

interface AsyncRedisClient extends RedisClient {
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

export default AsyncRedisClient;
