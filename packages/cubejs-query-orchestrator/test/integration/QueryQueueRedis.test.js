const QueryQueueTest = require('../unit/QueryQueue.test');
const { RedisPool } = require('../../src/orchestrator/RedisPool');

[false, true].forEach((sentinelFlag) => {
    process.env.FLAG_ENABLE_REDIS_SENTINEL = sentinelFlag

    QueryQueueTest(`RedisPool, redis sentinel=${process.env.FLAG_ENABLE_REDIS_SENTINEL}`,
        { cacheAndQueueDriver: 'redis', redisPool: new RedisPool() });
    QueryQueueTest(`RedisNoPool, redis sentinel=${process.env.FLAG_ENABLE_REDIS_SENTINEL}`,
        { cacheAndQueueDriver: 'redis', redisPool: new RedisPool({ poolMin: 0, poolMax: 0 }) });
});