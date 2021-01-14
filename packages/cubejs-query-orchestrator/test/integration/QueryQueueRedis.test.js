const QueryQueueTest = require('../unit/QueryQueue.test');
const { RedisPool } = require('../../src/orchestrator/RedisPool');

[false, true].forEach((sentinelFlag) => {
  process.env.CUBEJS_REDIS_USE_IOREDIS = sentinelFlag;

  QueryQueueTest(`RedisPool, sentinel ${process.env.CUBEJS_REDIS_USE_IOREDIS ? 'enabled' : 'disabled'}`,
    { cacheAndQueueDriver: 'redis', redisPool: new RedisPool() });
  QueryQueueTest(`RedisNoPool, sentinel ${process.env.CUBEJS_REDIS_USE_IOREDIS ? 'enabled' : 'disabled'}`,
    { cacheAndQueueDriver: 'redis', redisPool: new RedisPool({ poolMin: 0, poolMax: 0 }) });
});
