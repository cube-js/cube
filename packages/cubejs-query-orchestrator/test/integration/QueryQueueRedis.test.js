const QueryQueueTest = require('../unit/QueryQueue.test');
const RedisPool = require('../../orchestrator/RedisPool');

QueryQueueTest('RedisPool', { cacheAndQueueDriver: 'redis', redisPool: new RedisPool() });
QueryQueueTest('RedisNoPool', { cacheAndQueueDriver: 'redis', redisPool: new RedisPool({ poolMin: 0, poolMax: 0 }) });
