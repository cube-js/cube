const R = require('ramda');

const BaseQueueDriver = require('./BaseQueueDriver');
const createRedisClient = require('./RedisFactory');

class RedisQueueDriverConnection {
  constructor(driver, options) {
    this.driver = driver;
    this.redisClient = options.createRedisClient();
    this.redisQueuePrefix = options.redisQueuePrefix;
    this.continueWaitTimeout = options.continueWaitTimeout;
    this.orphanedTimeout = options.orphanedTimeout;
    this.heartBeatTimeout = options.heartBeatTimeout;
    this.concurrency = options.concurrency;
  }

  async getResultBlocking(queryKey) {
    const resultListKey = this.resultListKey(queryKey);
    const result = await this.redisClient.brpopAsync([resultListKey, this.continueWaitTimeout]);
    return result && JSON.parse(result[1]);
  }

  async getResult(queryKey) {
    const resultListKey = this.resultListKey(queryKey);
    const result = await this.redisClient.rpopAsync(resultListKey);
    return result && JSON.parse(result);
  }

  addToQueue(keyScore, queryKey, time, queryHandler, query, priority, options) {
    return this.redisClient.multi()
      .zadd([this.toProcessRedisKey(), 'NX', keyScore, this.redisHash(queryKey)])
      .zadd([this.recentRedisKey(), time, this.redisHash(queryKey)])
      .hsetnx([
        this.queriesDefKey(),
        this.redisHash(queryKey),
        JSON.stringify({
          queryHandler, query, queryKey, stageQueryKey: options.stageQueryKey, priority
        })
      ])
      .zcard(this.toProcessRedisKey())
      .execAsync();
  }

  getToProcessQueries() {
    return this.redisClient.zrangeAsync([this.toProcessRedisKey(), 0, -1]);
  }

  getActiveQueries() {
    return this.redisClient.zrangeAsync([this.activeRedisKey(), 0, -1]);
  }

  async getQueryAndRemove(queryKey) {
    const [query, ...restResult] = await this.redisClient.multi()
      .hget([this.queriesDefKey(), this.redisHash(queryKey)])
      .zrem([this.activeRedisKey(), this.redisHash(queryKey)])
      .zrem([this.toProcessRedisKey(), this.redisHash(queryKey)])
      .zrem([this.recentRedisKey(), this.redisHash(queryKey)])
      .hdel([this.queriesDefKey(), this.redisHash(queryKey)])
      .execAsync();
    return [JSON.parse(query), ...restResult];
  }

  setResultAndRemoveQuery(queryKey, executionResult) {
    return this.redisClient.multi()
      .lpush([this.resultListKey(queryKey), JSON.stringify(executionResult)])
      .zrem([this.activeRedisKey(), this.redisHash(queryKey)])
      .zrem([this.toProcessRedisKey(), this.redisHash(queryKey)])
      .zrem([this.recentRedisKey(), this.redisHash(queryKey)])
      .hdel([this.queriesDefKey(), this.redisHash(queryKey)])
      .execAsync();
  }

  removeQuery(queryKey) {
    return this.redisClient.multi()
      .zrem([this.activeRedisKey(), this.redisHash(queryKey)])
      .zrem([this.toProcessRedisKey(), this.redisHash(queryKey)])
      .zrem([this.recentRedisKey(), this.redisHash(queryKey)])
      .hdel([this.queriesDefKey(), this.redisHash(queryKey)])
      .execAsync();
  }

  getOrphanedQueries() {
    return this.redisClient.zrangebyscoreAsync(
      [this.recentRedisKey(), 0, (new Date().getTime() - this.orphanedTimeout * 1000)]
    );
  }

  getStalledQueries() {
    return this.redisClient.zrangebyscoreAsync(
      [this.activeRedisKey(), 0, (new Date().getTime() - this.heartBeatTimeout * 1000)]
    );
  }

  async getQueryStageState() {
    const [active, toProcess, allQueryDefs] = await this.redisClient.multi()
      .zrange([this.activeRedisKey(), 0, -1])
      .zrange([this.toProcessRedisKey(), 0, -1])
      .hgetall(this.queriesDefKey())
      .execAsync();
    return [active, toProcess, R.map(q => JSON.parse(q), allQueryDefs || {})];
  }

  async getQueryDef(queryKey) {
    const query = await this.redisClient.hgetAsync([this.queriesDefKey(), this.redisHash(queryKey)]);
    return JSON.parse(query);
  }

  updateHeartBeat(queryKey) {
    return this.redisClient.zaddAsync([this.activeRedisKey(), new Date().getTime(), this.redisHash(queryKey)]);
  }

  retrieveForProcessing(queryKey) {
    return this.redisClient.multi()
      .zadd([this.activeRedisKey(), 'NX', new Date().getTime(), this.redisHash(queryKey)])
      .zremrangebyrank([this.activeRedisKey(), this.concurrency, -1])
      .zrange([this.activeRedisKey(), 0, this.concurrency - 1])
      .zcard(this.toProcessRedisKey())
      .execAsync();
  }

  async optimisticQueryUpdate(queryKey, toUpdate) {
    let query = await this.getQueryDef(queryKey);
    for (let i = 0; i < 10; i++) {
      if (query) {
        // eslint-disable-next-line no-await-in-loop
        let [beforeUpdate] = await this.redisClient
          .multi()
          .hget([this.queriesDefKey(), this.redisHash(queryKey)])
          .hset([this.queriesDefKey(), this.redisHash(queryKey), JSON.stringify({ ...query, ...toUpdate })])
          .execAsync();
        beforeUpdate = JSON.parse(beforeUpdate);
        if (JSON.stringify(query) === JSON.stringify(beforeUpdate)) {
          return true;
        }
        query = beforeUpdate;
      }
    }
    throw new Error(`Can't update ${queryKey} with ${JSON.stringify(toUpdate)}`);
  }

  release() {
    return this.redisClient.quit();
  }

  toProcessRedisKey() {
    return this.queueRedisKey('QUEUE');
  }

  recentRedisKey() {
    return this.queueRedisKey('RECENT');
  }

  activeRedisKey() {
    return this.queueRedisKey('ACTIVE');
  }

  queryRedisKey(queryKey, suffix) {
    return `${this.redisQueuePrefix}_${this.redisHash(queryKey)}_${suffix}`;
  }

  queueRedisKey(suffix) {
    return `${this.redisQueuePrefix}_${suffix}`;
  }

  queriesDefKey() {
    return this.queryRedisKey('QUERIES');
  }

  resultListKey(queryKey) {
    return this.queryRedisKey(queryKey, 'RESULT');
  }

  redisHash(queryKey) {
    return this.driver.redisHash(queryKey);
  }
}

class RedisQueueDriver extends BaseQueueDriver {
  constructor(options) {
    super();
    this.createRedisClient = options.createRedisClient || (() => createRedisClient(process.env.REDIS_URL));
    this.options = options;
  }

  createConnection() {
    return new RedisQueueDriverConnection(this, {
      ...this.options,
      createRedisClient: this.createRedisClient
    });
  }
}

module.exports = RedisQueueDriver;
