import R from 'ramda';
import { BaseQueueDriver } from './BaseQueueDriver';

export class RedisQueueDriverConnection {
  constructor(driver, options) {
    this.driver = driver;
    this.redisClient = options.redisClient;
    this.redisQueuePrefix = options.redisQueuePrefix;
    this.continueWaitTimeout = options.continueWaitTimeout;
    this.heartBeatTimeout = options.heartBeatTimeout;
    this.concurrency = options.concurrency;
    this.getQueueEventsBus = options.getQueueEventsBus;
  }

  async getResultBlocking(queryKey) {
    const resultListKey = this.resultListKey(queryKey);
    if (!(await this.redisClient.hgetAsync([this.queriesDefKey(), this.redisHash(queryKey)]))) {
      return this.getResult(queryKey);
    }
    const result = await this.redisClient.brpopAsync([resultListKey, this.continueWaitTimeout]);
    if (result) {
      await this.redisClient.lpushAsync([resultListKey, result[1]]);
      await this.redisClient.rpopAsync(resultListKey);
    }
    return result && JSON.parse(result[1]);
  }

  async getResult(queryKey) {
    const resultListKey = this.resultListKey(queryKey);
    const result = await this.redisClient.rpopAsync(resultListKey);
    return result && JSON.parse(result);
  }

  /**
   * Adds specified by the queryKey query to the queue, returns tuple
   * with the operation result.
   *
   * @typedef {[added: number, _b: *, _c: *, toProcessLength: number, addedTime: number]} AddedTuple
   *
   * @param {number} keyScore
   * @param {*} queryKey
   * @param {number} orphanedTime
   * @param {string} queryHandler (for the regular query is eq to 'query')
   * @param {*} query
   * @param {number} priority
   * @param {*} options
   * @returns {AddedTuple}
   */
  addToQueue(keyScore, queryKey, orphanedTime, queryHandler, query, priority, options) {
    const data = {
      queryHandler,
      query,
      queryKey,
      stageQueryKey: options.stageQueryKey,
      priority,
      requestId: options.requestId,
      addedToQueueTime: new Date().getTime()
    };

    const tx = this.redisClient.multi()
      .zadd([this.toProcessRedisKey(), 'NX', keyScore, this.redisHash(queryKey)])
      .zadd([this.recentRedisKey(), orphanedTime, this.redisHash(queryKey)])
      .hsetnx([
        this.queriesDefKey(),
        this.redisHash(queryKey),
        JSON.stringify(data)
      ])
      .zcard(this.toProcessRedisKey());

    if (this.getQueueEventsBus) {
      tx.publish(
        this.getQueueEventsBus().eventsChannel,
        JSON.stringify({
          event: 'addedToQueue',
          redisQueuePrefix: this.redisQueuePrefix,
          queryKey: this.redisHash(queryKey),
          payload: data
        })
      );
    }
    return tx.execAsync().then(result => [...result, data.addedToQueueTime]);
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
      .zrem([this.heartBeatRedisKey(), this.redisHash(queryKey)])
      .zrem([this.toProcessRedisKey(), this.redisHash(queryKey)])
      .zrem([this.recentRedisKey(), this.redisHash(queryKey)])
      .hdel([this.queriesDefKey(), this.redisHash(queryKey)])
      .del(this.queryProcessingLockKey(queryKey))
      .execAsync();
    return [JSON.parse(query), ...restResult];
  }

  async cancelQuery(queryKey) {
    const [query] = await this.getQueryAndRemove(queryKey);

    if (this.getQueueEventsBus) {
      await this.redisClient.publish(
        this.getQueueEventsBus().eventsChannel,
        JSON.stringify({
          event: 'cancelQuery',
          redisQueuePrefix: this.redisQueuePrefix,
          queryKey: this.redisHash(queryKey),
          payload: query
        })
      );
    }

    return query;
  }

  async setResultAndRemoveQuery(queryKey, executionResult, processingId) {
    try {
      await this.redisClient.watchAsync(this.queryProcessingLockKey(queryKey));
      const currentProcessId = await this.redisClient.getAsync(this.queryProcessingLockKey(queryKey));
      if (processingId !== currentProcessId) {
        return false;
      }
      const tx = this.redisClient.multi()
        .lpush([this.resultListKey(queryKey), JSON.stringify(executionResult)])
        .zrem([this.activeRedisKey(), this.redisHash(queryKey)])
        .zrem([this.heartBeatRedisKey(), this.redisHash(queryKey)])
        .zrem([this.toProcessRedisKey(), this.redisHash(queryKey)])
        .zrem([this.recentRedisKey(), this.redisHash(queryKey)])
        .hdel([this.queriesDefKey(), this.redisHash(queryKey)])
        .del(this.queryProcessingLockKey(queryKey));
      
      if (this.getQueueEventsBus) {
        tx.publish(
          this.getQueueEventsBus().eventsChannel,
          JSON.stringify({
            event: 'setResultAndRemoveQuery',
            redisQueuePrefix: this.redisQueuePrefix,
            queryKey: this.redisHash(queryKey),
            payload: executionResult
          })
        );
      }
      return tx.execAsync();
    } finally {
      await this.redisClient.unwatchAsync();
    }
  }

  getOrphanedQueries() {
    return this.redisClient.zrangebyscoreAsync(
      [this.recentRedisKey(), 0, new Date().getTime()]
    );
  }

  getStalledQueries() {
    return this.redisClient.zrangebyscoreAsync(
      [this.heartBeatRedisKey(), 0, (new Date().getTime() - this.heartBeatTimeout * 1000)]
    );
  }

  async getQueryStageState(onlyKeys) {
    let request = this.redisClient.multi()
      .zrange([this.activeRedisKey(), 0, -1])
      .zrange([this.toProcessRedisKey(), 0, -1]);
    if (!onlyKeys) {
      request = request.hgetall(this.queriesDefKey());
    }
    const [active, toProcess, allQueryDefs] = await request.execAsync();
    return [active, toProcess, R.map(q => JSON.parse(q), allQueryDefs || {})];
  }

  async getQueryDef(queryKey) {
    const query = await this.redisClient.hgetAsync([this.queriesDefKey(), this.redisHash(queryKey)]);
    return JSON.parse(query);
  }

  /**
   * Updates heart beat for the processing query by its `queryKey`.
   *
   * @param {string} queryKey
   */
  updateHeartBeat(queryKey) {
    return this.redisClient.zaddAsync([this.heartBeatRedisKey(), new Date().getTime(), this.redisHash(queryKey)]);
  }

  async getNextProcessingId() {
    const id = await this.redisClient.incrAsync(this.processingIdKey());
    return id && id.toString();
  }

  async retrieveForProcessing(queryKey, processingId) {
    try {
      const lockKey = this.queryProcessingLockKey(queryKey);
      await this.redisClient.watchAsync(lockKey);

      const currentProcessId = await this.redisClient.getAsync(lockKey);

      if (currentProcessId) {
        return null;
      }

      const result =
        await this.redisClient.multi()
          .zadd([this.activeRedisKey(), 'NX', processingId, this.redisHash(queryKey)])
          .zremrangebyrank([this.activeRedisKey(), this.concurrency, -1])
          .zrange([this.activeRedisKey(), 0, this.concurrency - 1])
          .zcard(this.toProcessRedisKey())
          .hget(([this.queriesDefKey(), this.redisHash(queryKey)]))
          .set(lockKey, processingId, 'NX')
          .zadd([this.heartBeatRedisKey(), 'NX', new Date().getTime(), this.redisHash(queryKey)])
          .execAsync();
      if (result) {
        result[4] = JSON.parse(result[4]);

        if (this.getQueueEventsBus) {
          await this.redisClient.publish(
            this.getQueueEventsBus().eventsChannel,
            JSON.stringify({
              event: 'retrievedForProcessing',
              redisQueuePrefix: this.redisQueuePrefix,
              queryKey: this.redisHash(queryKey),
              payload: result[4]
            })
          );
        }
      }
      return result;
    } finally {
      await this.redisClient.unwatchAsync();
    }
  }

  async freeProcessingLock(queryKey, processingId, activated) {
    try {
      const lockKey = this.queryProcessingLockKey(queryKey);
      await this.redisClient.watchAsync(lockKey);
      const currentProcessId = await this.redisClient.getAsync(lockKey);
      if (currentProcessId === processingId) {
        let removeCommand = this.redisClient.multi()
          .del(lockKey);
        if (activated) {
          removeCommand = removeCommand.zrem([this.activeRedisKey(), this.redisHash(queryKey)]);
        }
        await removeCommand
          .execAsync();
        return null;
      } else {
        return currentProcessId;
      }
    } finally {
      await this.redisClient.unwatchAsync();
    }
  }

  async optimisticQueryUpdate(queryKey, toUpdate, processingId) {
    try {
      let query = await this.getQueryDef(queryKey);
      for (let i = 0; i < 10; i++) {
        if (query) {
          // eslint-disable-next-line no-await-in-loop
          await this.redisClient.watchAsync(this.queryProcessingLockKey(queryKey));
          const currentProcessId = await this.redisClient.getAsync(this.queryProcessingLockKey(queryKey));
          if (currentProcessId !== processingId) {
            return false;
          }
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
    } finally {
      await this.redisClient.unwatchAsync();
    }
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

  heartBeatRedisKey() {
    return this.queueRedisKey('HEART_BEAT');
  }

  queryRedisKey(queryKey, suffix) {
    return `${this.redisQueuePrefix}_${this.redisHash(queryKey)}_${suffix}`;
  }

  queueRedisKey(suffix) {
    return `${this.redisQueuePrefix}_${suffix}`;
  }

  queriesDefKey() {
    return this.queueRedisKey('QUERIES');
  }

  processingIdKey() {
    return this.queueRedisKey('PROCESSING_COUNTER');
  }

  resultListKey(queryKey) {
    return this.queryRedisKey(queryKey, 'RESULT');
  }

  queryProcessingLockKey(queryKey) {
    return this.queryRedisKey(queryKey, 'LOCK');
  }

  redisHash(queryKey) {
    return this.driver.redisHash(queryKey);
  }
}

export class RedisQueueDriver extends BaseQueueDriver {
  constructor(options) {
    super();
    this.redisPool = options.redisPool;
    this.options = options;
  }

  async createConnection() {
    const redisClient = await this.redisPool.getClient();
    return new RedisQueueDriverConnection(this, {
      redisClient,
      ...this.options
    });
  }

  release(connection) {
    this.redisPool.release(connection.redisClient);
  }
}
