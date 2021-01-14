import { ClientOpts } from 'redis';
import { RedisOptions } from 'ioredis';

type RedisClientOptions = ClientOpts | RedisOptions;

export default RedisClientOptions;
