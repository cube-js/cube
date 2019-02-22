const redis = require('redis');
const crypto = require('crypto');
const promisifyAll = require('util-promisifyall');
const R = require('ramda');
const TimeoutError = require('./TimeoutError');
const ContinueWaitError = require('./ContinueWaitError');

promisifyAll(redis.RedisClient.prototype);
promisifyAll(redis.Multi.prototype);

class QueryQueue {
  constructor(redisQueuePrefix, options) {
    this.redisQueuePrefix = redisQueuePrefix;
    this.concurrency = options.concurrency || 2;
    this.continueWaitTimeout = options.continueWaitTimeout || 5;
    this.executionTimeout = options.executionTimeout || 600;
    this.orphanedTimeout = options.orphanedTimeout || 120;
    this.heartBeatInterval = options.heartBeatInterval || 30;
    this.sendProcessMessageFn = options.sendProcessMessageFn || ((queryKey) => { this.processQuery(queryKey) });
    this.sendCancelMessageFn = options.sendCancelMessageFn || ((query) => { this.processCancel(query) });
    this.queryHandlers = options.queryHandlers;
    this.cancelHandlers = options.cancelHandlers;
    this.createRedisClient = options.createdRedisClient || (() => redis.createClient(process.env.REDIS_URL));
    this.logger = options.logger || ((message, event) => console.log(`${message} ${JSON.stringify(event)}`));
  }

  async executeInQueue(queryHandler, queryKey, query, priority, options) {
    options = options || {};
    const redisClient = this.createRedisClient();
    try {
      if (priority == null) {
        priority = 0;
      }
      if (!(priority >= 0 && priority <= 100)) {
        throw new Error('Priority should be between 0 and 100');
      }
      const resultListKey = this.resultListKey(queryKey);
      let result = await redisClient.rpopAsync(resultListKey);
      if (result) {
        return this.parseResult(result);
      }
      const time = new Date().getTime();
      const keyScore = time + (100 - priority) * 1E14;

      const [added, b, c, queueSize] = await redisClient.multi()
        .zadd([this.toProcessRedisKey(), 'NX', keyScore, this.redisHash(queryKey)])
        .zadd([this.recentRedisKey(), time, this.redisHash(queryKey)])
        .hsetnx([
          this.queriesDefKey(),
          this.redisHash(queryKey),
          JSON.stringify({ queryHandler, query, queryKey, stageQueryKey: options.stageQueryKey, priority })
        ])
        .zcard(this.toProcessRedisKey())
        .execAsync();

      if (added > 0) {
        this.logger('Added to queue', { priority, queueSize, queryKey });
      }

      await this.reconcileQueue(redisClient);
      result = await redisClient.brpopAsync([resultListKey, this.continueWaitTimeout]);
      if (!result) {
        throw new ContinueWaitError();
      }
      return this.parseResult(result[1]);
    } finally {
      redisClient.quit();
    }
  }

  parseResult(result) {
    if (!result) {
      return;
    }
    result = JSON.parse(result);
    if (result.error) {
      throw new Error(result.error); // TODO
    } else {
      return result.result;
    }
  }

  queriesDefKey() {
    return this.queryRedisKey('QUERIES');
  }

  resultListKey(queryKey) {
    return this.queryRedisKey(queryKey, 'RESULT');
  }

  async reconcileQueue(redisClient) {
    const toCancel = (
        await redisClient.zrangebyscoreAsync([this.activeRedisKey(), 0, (new Date().getTime() - this.heartBeatInterval * 4 * 1000)])
      ).concat(
        await redisClient.zrangebyscoreAsync([this.recentRedisKey(), 0, (new Date().getTime() - this.orphanedTimeout * 1000)])
      );

    await Promise.all(toCancel.map(async queryKey => {
      let [query] = await redisClient.multi()
        .hget([this.queriesDefKey(), this.redisHash(queryKey)])
        .zrem([this.activeRedisKey(), this.redisHash(queryKey)])
        .zrem([this.toProcessRedisKey(), this.redisHash(queryKey)])
        .zrem([this.recentRedisKey(), this.redisHash(queryKey)])
        .hdel([this.queriesDefKey(), this.redisHash(queryKey)])
        .execAsync();
      if (query) {
        query = JSON.parse(query);
        this.logger('Removing orphaned query', { queryKey: query.queryKey });
        await this.sendCancelMessageFn(query);
      }
    }));

    const active = await redisClient.zrangeAsync([this.activeRedisKey(), 0, -1]);
    const toProcess = await redisClient.zrangeAsync([this.toProcessRedisKey(), 0, -1]);
    await Promise.all(
      R.pipe(
        R.filter(p => active.indexOf(p) === -1),
        R.take(this.concurrency),
        R.map(this.sendProcessMessageFn)
      )(toProcess)
    );
  }

  queryTimeout(promise) {
    let timeout;
    const executionTimeout = this.executionTimeout;

    return Promise.race([
      promise,
      new Promise(function(resolve, reject) {
        timeout = setTimeout(function() {
          reject(new TimeoutError(`Query execution timeout after ${executionTimeout / 60} min of waiting`));
        }, executionTimeout * 1000);
      }),
    ]).then(function(v) {
      clearTimeout(timeout);
      return v;
    }, function(err) {
      clearTimeout(timeout);
      throw err;
    });
  }

  async fetchQueryStageState() {
    const redisClient = this.createRedisClient();
    try {
      const [active, toProcess, allQueryDefs] = await redisClient.multi()
        .zrange([this.activeRedisKey(), 0, -1])
        .zrange([this.toProcessRedisKey(), 0, -1])
        .hgetall(this.queriesDefKey())
        .execAsync();
      return [active, toProcess, R.map(q => JSON.parse(q), allQueryDefs || {})]
    } finally {
      redisClient.quit();
    }
  }

  async getQueryStage(stageQueryKey, priorityFilter, queryStageState) {
    const [active, toProcess, allQueryDefs] = queryStageState || await this.fetchQueryStageState();

    const queryDefs = toProcess.map(k => allQueryDefs[k]).filter(q => !!q);
    const queryInQueue = queryDefs.find(q =>
      this.redisHash(q.stageQueryKey) === this.redisHash(stageQueryKey) &&
      (priorityFilter != null ? q.priority === priorityFilter : true)
    );

    if (queryInQueue) {
      if (active.indexOf(this.redisHash(queryInQueue.queryKey)) !== -1) {
        return {
          stage: 'Executing query',
          timeElapsed: queryInQueue.startQueryTime ? new Date().getTime() - queryInQueue.startQueryTime : undefined
        };
      }
      const index = queryDefs.filter(q => active.indexOf(this.redisHash(q.queryKey)) === -1).indexOf(queryInQueue);
      if (index !== -1) {
        return index !== -1 ? { stage: `#${index + 1} in queue` } : undefined;
      }
    }

    return undefined;
  }

  async processQuery(queryKey) {
    const redisClient = this.createRedisClient();
    try {
      const [insertedCount, removedCount, activeKeys, queueSize] = await redisClient.multi()
        .zadd([this.activeRedisKey(), 'NX', new Date().getTime(), this.redisHash(queryKey)])
        .zremrangebyrank([this.activeRedisKey(), this.concurrency, -1])
        .zrange([this.activeRedisKey(), 0, this.concurrency - 1])
        .zcard(this.toProcessRedisKey())
        .execAsync();
      if (insertedCount && activeKeys.indexOf(this.redisHash(queryKey)) !== -1) {
        let query = await redisClient.hgetAsync([this.queriesDefKey(), this.redisHash(queryKey)]);
        if (query) {
          query = JSON.parse(query);
          let executionResult;
          const startQueryTime = (new Date()).getTime();
          this.logger('Performing query', { queueSize, queryKey: query.queryKey });
          await this.optimisticQueryUpdate(redisClient, queryKey, { startQueryTime });

          const heartBeatTimer = setInterval(
            () => redisClient.zaddAsync([this.activeRedisKey(), new Date().getTime(), this.redisHash(queryKey)]),
            this.heartBeatInterval * 1000
          );
          try {
            executionResult = {
              result: await this.queryTimeout(
                this.queryHandlers[query.queryHandler](
                  query.query,
                  async (cancelHandler) => {
                    try {
                      return this.optimisticQueryUpdate(redisClient, queryKey, { cancelHandler });
                    } catch (e) {
                      this.logger(`Error while query update`, { queryKey: queryKey, error: e.stack || e });
                    }
                  }
                )
              )
            };
            this.logger('Performing query completed', { queueSize, duration: ((new Date()).getTime() - startQueryTime), queryKey: query.queryKey });
          } catch (e) {
            executionResult = {
              error: (e.message || e).toString() // TODO error handling
            };
            this.logger('Error while querying', { queryKey: query.queryKey, error: (e.stack || e).toString() });
            if (e instanceof TimeoutError) {
              query = await redisClient.hgetAsync([this.queriesDefKey(), this.redisHash(queryKey)]);
              if (query) {
                this.logger('Cancelling query due to timeout', { queryKey: query.queryKey });
                await this.sendCancelMessageFn(JSON.parse(query));
              }
            }
          }

          clearInterval(heartBeatTimer);

          await redisClient.multi()
            .lpush([this.resultListKey(queryKey), JSON.stringify(executionResult)])
            .zrem([this.activeRedisKey(), this.redisHash(queryKey)])
            .zrem([this.toProcessRedisKey(), this.redisHash(queryKey)])
            .zrem([this.recentRedisKey(), this.redisHash(queryKey)])
            .hdel([this.queriesDefKey(), this.redisHash(queryKey)])
            .execAsync();
        } else {
          this.logger('Query cancelled in-flight', { queueSize, queryKey });
          await redisClient.multi()
            .zrem([this.activeRedisKey(), this.redisHash(queryKey)])
            .zrem([this.toProcessRedisKey(), this.redisHash(queryKey)])
            .zrem([this.recentRedisKey(), this.redisHash(queryKey)])
            .hdel([this.queriesDefKey(), this.redisHash(queryKey)])
            .execAsync();
        }

        await this.reconcileQueue(redisClient);
      }
    } finally {
      redisClient.quit();
    }
  }

  async optimisticQueryUpdate(redisClient, queryKey, toUpdate) {
    let query = await redisClient.hgetAsync([this.queriesDefKey(), this.redisHash(queryKey)]);
    for (let i = 0; i < 10; i++) {
      if (query) {
        const parsedQuery = JSON.parse(query);
        const [beforeUpdate] = await redisClient
          .multi()
          .hget([this.queriesDefKey(), this.redisHash(queryKey)])
          .hset([this.queriesDefKey(), this.redisHash(queryKey), JSON.stringify({ ...parsedQuery, ...toUpdate })])
          .execAsync();
        if (query === beforeUpdate) {
          return true;
        }
        query = beforeUpdate;
      }
    }
    throw new Error(`Can't update ${queryKey} with ${JSON.stringify(toUpdate)}`);
  }

  async processCancel(query) {
    const queryHandler = query.queryHandler;
    try {
      if (!this.cancelHandlers[queryHandler]) {
        throw new Error(`No cancel handler for ${queryHandler}`);
      }
      await this.cancelHandlers[queryHandler](query);
    } catch (e) {
      this.logger(`Error while cancel`, { queryKey: query.queryKey, error: e.stack || e });
    }
  }

  stageRedisKey(stageQueryKey) {
    return this.queryRedisKey(stageQueryKey, 'STAGE');
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

  redisHash(queryKey) {
    return typeof queryKey === 'string' && queryKey.length < 256 ?
      queryKey :
      crypto.createHash('md5').update(JSON.stringify(queryKey)).digest("hex");
  }

  queryRedisKey(queryKey, suffix) {
    return `${this.redisQueuePrefix}_${this.redisHash(queryKey)}_${suffix}`
  }

  queueRedisKey(suffix) {
    return `${this.redisQueuePrefix}_${suffix}`;
  }
}

module.exports = QueryQueue;
