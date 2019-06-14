const R = require('ramda');
const TimeoutError = require('./TimeoutError');
const ContinueWaitError = require('./ContinueWaitError');
const RedisQueueDriver = require('./RedisQueueDriver');
const LocalQueueDriver = require('./LocalQueueDriver');

class QueryQueue {
  constructor(redisQueuePrefix, options) {
    this.redisQueuePrefix = redisQueuePrefix;
    this.concurrency = options.concurrency || 2;
    this.continueWaitTimeout = options.continueWaitTimeout || 5;
    this.executionTimeout = options.executionTimeout || 600;
    this.orphanedTimeout = options.orphanedTimeout || 120;
    this.heartBeatInterval = options.heartBeatInterval || 30;
    this.sendProcessMessageFn = options.sendProcessMessageFn || ((queryKey) => { this.processQuery(queryKey); });
    this.sendCancelMessageFn = options.sendCancelMessageFn || ((query) => { this.processCancel(query); });
    this.queryHandlers = options.queryHandlers;
    this.cancelHandlers = options.cancelHandlers;
    this.logger = options.logger || ((message, event) => console.log(`${message} ${JSON.stringify(event)}`));
    const queueDriverOptions = {
      redisQueuePrefix: this.redisQueuePrefix,
      concurrency: this.concurrency,
      continueWaitTimeout: this.continueWaitTimeout,
      orphanedTimeout: this.orphanedTimeout,
      heartBeatTimeout: this.heartBeatInterval * 4,
      createRedisClient: options.createRedisClient
    };
    this.queueDriver = options.cacheAndQueueDriver === 'redis' ?
      new RedisQueueDriver(queueDriverOptions) :
      new LocalQueueDriver(queueDriverOptions);
  }

  async executeInQueue(queryHandler, queryKey, query, priority, options) {
    options = options || {};
    const redisClient = this.queueDriver.createConnection();
    try {
      if (priority == null) {
        priority = 0;
      }
      if (!(priority >= 0 && priority <= 100)) {
        throw new Error('Priority should be between 0 and 100');
      }
      let result = await redisClient.getResult(queryKey);
      if (result) {
        return this.parseResult(result);
      }
      const time = new Date().getTime();
      const keyScore = time + (100 - priority) * 1E14;

      // eslint-disable-next-line no-unused-vars
      const [added, b, c, queueSize] = await redisClient.addToQueue(
        keyScore, queryKey, time, queryHandler, query, priority, options
      );

      if (added > 0) {
        this.logger('Added to queue', { priority, queueSize, queryKey });
      }

      await this.reconcileQueue(redisClient);
      result = await redisClient.getResultBlocking(queryKey);
      if (!result) {
        throw new ContinueWaitError();
      }
      return this.parseResult(result);
    } finally {
      redisClient.release();
    }
  }

  parseResult(result) {
    if (!result) {
      return;
    }
    if (result.error) {
      throw new Error(result.error); // TODO
    } else {
      // eslint-disable-next-line consistent-return
      return result.result;
    }
  }

  async reconcileQueue(redisClient) {
    const toCancel = (
      await redisClient.getStalledQueries()
    ).concat(
      await redisClient.getOrphanedQueries()
    );

    await Promise.all(toCancel.map(async queryKey => {
      const [query] = await redisClient.getQueryAndRemove(queryKey);
      if (query) {
        this.logger('Removing orphaned query', { queryKey: query.queryKey });
        await this.sendCancelMessageFn(query);
      }
    }));

    const active = await redisClient.getActiveQueries();
    const toProcess = await redisClient.getToProcessQueries();
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
    const { executionTimeout } = this;

    return Promise.race([
      promise,
      new Promise((resolve, reject) => {
        timeout = setTimeout(() => {
          reject(new TimeoutError(`Query execution timeout after ${executionTimeout / 60} min of waiting`));
        }, executionTimeout * 1000);
      }),
    ]).then((v) => {
      clearTimeout(timeout);
      return v;
    }, (err) => {
      clearTimeout(timeout);
      throw err;
    });
  }

  async fetchQueryStageState() {
    const redisClient = this.queueDriver.createConnection();
    try {
      return redisClient.getQueryStageState();
    } finally {
      redisClient.release();
    }
  }

  async getQueryStage(stageQueryKey, priorityFilter, queryStageState) {
    const [active, toProcess, allQueryDefs] = queryStageState || await this.fetchQueryStageState();

    const queryDefs = toProcess.map(k => allQueryDefs[k]).filter(q => !!q);
    const queryInQueue = queryDefs.find(q => this.redisHash(q.stageQueryKey) === this.redisHash(stageQueryKey) &&
      (priorityFilter != null ? q.priority === priorityFilter : true));

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
    const redisClient = this.queueDriver.createConnection();
    try {
      // eslint-disable-next-line no-unused-vars
      const [insertedCount, removedCount, activeKeys, queueSize] =
        await redisClient.retrieveForProcessing(queryKey);
      if (insertedCount && activeKeys.indexOf(this.redisHash(queryKey)) !== -1) {
        let query = await redisClient.getQueryDef(queryKey);
        if (query) {
          let executionResult;
          const startQueryTime = (new Date()).getTime();
          this.logger('Performing query', { queueSize, queryKey: query.queryKey });
          await redisClient.optimisticQueryUpdate(queryKey, { startQueryTime });

          const heartBeatTimer = setInterval(
            () => redisClient.updateHeartBeat(queryKey),
            this.heartBeatInterval * 1000
          );
          try {
            executionResult = {
              result: await this.queryTimeout(
                this.queryHandlers[query.queryHandler](
                  query.query,
                  async (cancelHandler) => {
                    try {
                      return redisClient.optimisticQueryUpdate(queryKey, { cancelHandler });
                    } catch (e) {
                      this.logger(`Error while query update`, { queryKey, error: e.stack || e });
                    }
                    return null;
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
              query = await redisClient.getQueryDef(queryKey);
              if (query) {
                this.logger('Cancelling query due to timeout', { queryKey: query.queryKey });
                await this.sendCancelMessageFn(query);
              }
            }
          }

          clearInterval(heartBeatTimer);

          await redisClient.setResultAndRemoveQuery(queryKey, executionResult);
        } else {
          this.logger('Query cancelled in-flight', { queueSize, queryKey });
          await redisClient.removeQuery(queryKey);
        }

        await this.reconcileQueue(redisClient);
      }
    } finally {
      redisClient.release();
    }
  }

  async processCancel(query) {
    const { queryHandler } = query;
    try {
      if (!this.cancelHandlers[queryHandler]) {
        throw new Error(`No cancel handler for ${queryHandler}`);
      }
      await this.cancelHandlers[queryHandler](query);
    } catch (e) {
      this.logger(`Error while cancel`, { queryKey: query.queryKey, error: e.stack || e });
    }
  }

  redisHash(queryKey) {
    return this.queueDriver.redisHash(queryKey);
  }
}

module.exports = QueryQueue;
