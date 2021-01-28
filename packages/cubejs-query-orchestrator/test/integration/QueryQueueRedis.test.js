const QueryQueueTest = require('../unit/QueryQueue.test');
const { RedisPool } = require('../../src/orchestrator/RedisPool');

const config = require('../../config');

[false, true].forEach((sentinelFlag) => {
  config.FLAG_ENABLE_REDIS_SENTINEL = sentinelFlag;

  QueryQueueTest(`RedisPool, sentinel ${config.FLAG_ENABLE_REDIS_SENTINEL ? 'enabled' : 'disabled'}`,
    { cacheAndQueueDriver: 'redis', redisPool: new RedisPool() });
  QueryQueueTest(`RedisNoPool, sentinel ${config.FLAG_ENABLE_REDIS_SENTINEL ? 'enabled' : 'disabled'}`,
    { cacheAndQueueDriver: 'redis', redisPool: new RedisPool({ poolMin: 0, poolMax: 0 }) });
});
