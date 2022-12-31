import { getEnv } from '@cubejs-backend/shared';
import { QueryQueueTest } from '../../unit/QueryQueue.abstract';
import { RedisPool } from '../../../src/orchestrator/RedisPool';

function doRedisTest(useIORedis: boolean) {
  process.env.CUBEJS_REDIS_USE_IOREDIS = <any>useIORedis;

  const title = `RedisPool, Driver: ${useIORedis ? 'plain redis' : 'ioredis'}`;

  QueryQueueTest(
    title,
    {
      cacheAndQueueDriver: 'redis',
      redisPool: new RedisPool()
    }
  );
  QueryQueueTest(
    `${title} without pool`,
    {
      cacheAndQueueDriver: 'redis',
      redisPool: new RedisPool({ poolMin: 0, poolMax: 0 })
    }
  );
}

if (process.env.CUBEJS_REDIS_USE_IOREDIS !== undefined) {
  doRedisTest(getEnv('redisUseIORedis'));
} else {
  doRedisTest(true);
  doRedisTest(false);
}
