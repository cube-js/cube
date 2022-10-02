import R from 'ramda';
import { getEnv } from '@cubejs-backend/shared';

import { TimeoutError } from './TimeoutError';
import { ContinueWaitError } from './ContinueWaitError';
import { RedisQueueDriver } from './RedisQueueDriver';
import { LocalQueueDriver } from './LocalQueueDriver';

export class QueryQueue {
  constructor(redisQueuePrefix, options) {
    this.redisQueuePrefix = redisQueuePrefix;
    this.concurrency = options.concurrency || 2;
    this.continueWaitTimeout = options.continueWaitTimeout || 5;
    this.executionTimeout = options.executionTimeout || getEnv('dbQueryTimeout');
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
      redisPool: options.redisPool,
      getQueueEventsBus: options.getQueueEventsBus
    };
    this.queueDriver = options.cacheAndQueueDriver === 'redis' ?
      new RedisQueueDriver(queueDriverOptions) :
      new LocalQueueDriver(queueDriverOptions);
    this.skipQueue = options.skipQueue;
  }

  async executeInQueue(queryHandler, queryKey, query, priority, options) {
    options = options || {};
    if (this.skipQueue) {
      const queryDef = {
        queryHandler,
        query,
        queryKey,
        stageQueryKey: options.stageQueryKey,
        priority,
        requestId: options.requestId,
        addedToQueueTime: new Date().getTime(),
      };
      this.logger('Waiting for query', {
        queueSize: 0,
        queryKey: queryDef.queryKey,
        queuePrefix: this.redisQueuePrefix,
        requestId: options.requestId,
        waitingForRequestId: queryDef.requestId
      });
      const result = await this.processQuerySkipQueue(queryDef);
      return this.parseResult(result);
    }
    const redisClient = await this.queueDriver.createConnection();
    try {
      if (priority == null) {
        priority = 0;
      }
      if (!(priority >= -10000 && priority <= 10000)) {
        throw new Error('Priority should be between -10000 and 10000');
      }
      let result = !query.forceBuild && await redisClient.getResult(queryKey);
      
      if (result) {
        return this.parseResult(result);
      }

      if (query.forceBuild) {
        const jobExists = await redisClient.getQueryDef(queryKey);
        if (jobExists) return null;
      }

      const time = new Date().getTime();
      const keyScore = time + (10000 - priority) * 1E14;

      const orphanedTimeout = 'orphanedTimeout' in query ? query.orphanedTimeout : this.orphanedTimeout;
      const orphanedTime = time + (orphanedTimeout * 1000);
      const [added, _b, _c, queueSize, addedToQueueTime] = await redisClient.addToQueue(
        keyScore, queryKey, orphanedTime, queryHandler, query, priority, options
      );

      if (added > 0) {
        this.logger('Added to queue', {
          priority,
          queueSize,
          queryKey,
          queuePrefix: this.redisQueuePrefix,
          requestId: options.requestId,
          metadata: query.metadata,
          preAggregationId: query.preAggregation?.preAggregationId,
          newVersionEntry: query.newVersionEntry,
          forceBuild: query.forceBuild,
          preAggregation: query.preAggregation,
          addedToQueueTime
        });
      }

      await this.reconcileQueue();

      const queryDef = await redisClient.getQueryDef(queryKey);
      const [active, toProcess] = await redisClient.getQueryStageState(true);

      if (queryDef) {
        this.logger('Waiting for query', {
          queueSize,
          queryKey: queryDef.queryKey,
          queuePrefix: this.redisQueuePrefix,
          requestId: options.requestId,
          activeQueryKeys: active,
          toProcessQueryKeys: toProcess,
          active: active.indexOf(redisClient.redisHash(queryKey)) !== -1,
          queueIndex: toProcess.indexOf(redisClient.redisHash(queryKey)),
          waitingForRequestId: queryDef.requestId
        });
      }

      result = await redisClient.getResultBlocking(queryKey);
      if (!result) {
        throw new ContinueWaitError();
      }

      return this.parseResult(result);
    } finally {
      this.queueDriver.release(redisClient);
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

  async reconcileQueue() {
    if (!this.reconcilePromise) {
      this.reconcileAgain = false;
      this.reconcilePromise = this.reconcileQueueImpl()
        .catch((e) => {
          this.reconcilePromise = null;

          throw e;
        })
        .then(() => {
          this.reconcilePromise = null;

          if (this.reconcileAgain) {
            return this.reconcileQueue();
          }

          return null;
        });
    } else {
      this.reconcileAgain = true;
    }

    return this.reconcilePromise;
  }

  async getQueries() {
    const redisClient = await this.queueDriver.createConnection();
    try {
      const [stalledQueries, orphanedQueries, activeQueries, toProcessQueries] = await Promise.all([
        redisClient.getStalledQueries(),
        redisClient.getOrphanedQueries(),
        redisClient.getActiveQueries(),
        redisClient.getToProcessQueries()
      ]);

      const mapWithDefinition = (arr) => Promise.all(arr.map(async queryKey => ({
        ...(await redisClient.getQueryDef(queryKey)),
        queryKey
      })));

      const [stalled, orphaned, active, toProcess] = await Promise.all(
        [stalledQueries, orphanedQueries, activeQueries, toProcessQueries].map(arr => mapWithDefinition(arr))
      );

      const result = {
        orphaned,
        stalled,
        active,
        toProcess
      };

      return Object.values(Object.keys(result).reduce((obj, status) => {
        result[status].forEach(query => {
          if (!obj[query.queryKey]) {
            obj[query.queryKey] = {
              ...query,
              status: []
            };
          }
  
          obj[query.queryKey].status.push(status);
        });
        return obj;
      }, {}));
    } finally {
      this.queueDriver.release(redisClient);
    }
  }

  async cancelQuery(queryKey) {
    const redisClient = await this.queueDriver.createConnection();
    try {
      const query = await redisClient.cancelQuery(queryKey);

      if (query) {
        this.logger('Cancelling query manual', {
          queryKey: query.queryKey,
          queuePrefix: this.redisQueuePrefix,
          requestId: query.requestId,
          metadata: query.query?.metadata,
          preAggregationId: query.query?.preAggregation?.preAggregationId,
          newVersionEntry: query.query?.newVersionEntry,
          preAggregation: query.query?.preAggregation,
          addedToQueueTime: query.addedToQueueTime,
        });
        await this.sendCancelMessageFn(query);
      }

      return true;
    } finally {
      this.queueDriver.release(redisClient);
    }
  }

  async reconcileQueueImpl() {
    const redisClient = await this.queueDriver.createConnection();
    try {
      const toCancel = (
        await redisClient.getStalledQueries()
      ).concat(
        await redisClient.getOrphanedQueries()
      );

      await Promise.all(toCancel.map(async queryKey => {
        const [query] = await redisClient.getQueryAndRemove(queryKey);
        if (query) {
          this.logger('Removing orphaned query', {
            queryKey: query.queryKey,
            queuePrefix: this.redisQueuePrefix,
            requestId: query.requestId,
            metadata: query.query?.metadata,
            preAggregationId: query.query?.preAggregation?.preAggregationId,
            newVersionEntry: query.query?.newVersionEntry,
            preAggregation: query.query?.preAggregation,
            addedToQueueTime: query.addedToQueueTime,
          });
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
    } finally {
      this.queueDriver.release(redisClient);
    }
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
    const redisClient = await this.queueDriver.createConnection();
    try {
      return redisClient.getQueryStageState();
    } finally {
      this.queueDriver.release(redisClient);
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

  async processQuerySkipQueue(query) {
    const startQueryTime = (new Date()).getTime();
    this.logger('Performing query', {
      queueSize: 0,
      queryKey: query.queryKey,
      queuePrefix: this.redisQueuePrefix,
      requestId: query.requestId,
      timeInQueue: 0
    });
    let executionResult;
    let handler;

    try {
      executionResult = {
        result: await this.queryTimeout(
          this.queryHandlers[query.queryHandler](
            query.query,
            async (cancelHandler) => {
              handler = cancelHandler;
            }
          )
        )
      };
      this.logger('Performing query completed', {
        queueSize: 0,
        duration: ((new Date()).getTime() - startQueryTime),
        queryKey: query.queryKey,
        queuePrefix: this.redisQueuePrefix,
        requestId: query.requestId,
        timeInQueue: 0
      });
    } catch (e) {
      executionResult = {
        error: (e.message || e).toString() // TODO error handling
      };
      this.logger('Error while querying', {
        queueSize: 0,
        duration: ((new Date()).getTime() - startQueryTime),
        queryKey: query.queryKey,
        queuePrefix: this.redisQueuePrefix,
        requestId: query.requestId,
        timeInQueue: 0,
        error: (e.stack || e).toString()
      });
      if (e instanceof TimeoutError) {
        if (handler) {
          this.logger('Cancelling query due to timeout', {
            queryKey: query.queryKey,
            queuePrefix: this.redisQueuePrefix,
            requestId: query.requestId
          });
          await handler(query);
        }
      }
    }
    return executionResult;
  }

  async processQuery(queryKey) {
    const redisClient = await this.queueDriver.createConnection();
    let insertedCount;
    let _removedCount;
    let activeKeys;
    let queueSize;
    let query;
    let processingLockAcquired;
    try {
      const processingId = await redisClient.getNextProcessingId();
      const retrieveResult = await redisClient.retrieveForProcessing(queryKey, processingId);
      if (retrieveResult) {
        [insertedCount, _removedCount, activeKeys, queueSize, query, processingLockAcquired] = retrieveResult;
      }
      const activated = activeKeys && activeKeys.indexOf(this.redisHash(queryKey)) !== -1;
      if (!query) {
        query = await redisClient.getQueryDef(this.redisHash(queryKey));
      }
      if (query && insertedCount && activated && processingLockAcquired) {
        let executionResult;
        const startQueryTime = (new Date()).getTime();
        const timeInQueue = (new Date()).getTime() - query.addedToQueueTime;
        this.logger('Performing query', {
          processingId,
          queueSize,
          queryKey: query.queryKey,
          queuePrefix: this.redisQueuePrefix,
          requestId: query.requestId,
          timeInQueue,
          metadata: query.query?.metadata,
          preAggregationId: query.query?.preAggregation?.preAggregationId,
          newVersionEntry: query.query?.newVersionEntry,
          preAggregation: query.query?.preAggregation,
          addedToQueueTime: query.addedToQueueTime,
        });
        await redisClient.optimisticQueryUpdate(queryKey, { startQueryTime }, processingId);

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
                    return redisClient.optimisticQueryUpdate(queryKey, { cancelHandler }, processingId);
                  } catch (e) {
                    this.logger('Error while query update', {
                      queryKey: query.queryKey,
                      error: e.stack || e,
                      queuePrefix: this.redisQueuePrefix,
                      requestId: query.requestId,
                      metadata: query.query?.metadata,
                      preAggregationId: query.query?.preAggregation?.preAggregationId,
                      newVersionEntry: query.query?.newVersionEntry,
                      preAggregation: query.query?.preAggregation,
                      addedToQueueTime: query.addedToQueueTime,
                    });
                  }
                  return null;
                }
              )
            )
          };
          this.logger('Performing query completed', {
            processingId,
            queueSize,
            duration: ((new Date()).getTime() - startQueryTime),
            queryKey: query.queryKey,
            queuePrefix: this.redisQueuePrefix,
            requestId: query.requestId,
            timeInQueue,
            metadata: query.query?.metadata,
            preAggregationId: query.query?.preAggregation?.preAggregationId,
            newVersionEntry: query.query?.newVersionEntry,
            preAggregation: query.query?.preAggregation,
            addedToQueueTime: query.addedToQueueTime,
          });
        } catch (e) {
          executionResult = {
            error: (e.message || e).toString() // TODO error handling
          };
          this.logger('Error while querying', {
            processingId,
            queueSize,
            duration: ((new Date()).getTime() - startQueryTime),
            queryKey: query.queryKey,
            queuePrefix: this.redisQueuePrefix,
            requestId: query.requestId,
            timeInQueue,
            metadata: query.query?.metadata,
            preAggregationId: query.query?.preAggregation?.preAggregationId,
            newVersionEntry: query.query?.newVersionEntry,
            preAggregation: query.query?.preAggregation,
            addedToQueueTime: query.addedToQueueTime,
            error: (e.stack || e).toString()
          });
          if (e instanceof TimeoutError) {
            const queryWithCancelHandle = await redisClient.getQueryDef(queryKey);
            if (queryWithCancelHandle) {
              this.logger('Cancelling query due to timeout', {
                processingId,
                queryKey: queryWithCancelHandle.queryKey,
                queuePrefix: this.redisQueuePrefix,
                requestId: queryWithCancelHandle.requestId,
                metadata: queryWithCancelHandle.query?.metadata,
                preAggregationId: queryWithCancelHandle.query?.preAggregation?.preAggregationId,
                newVersionEntry: queryWithCancelHandle.query?.newVersionEntry,
                preAggregation: queryWithCancelHandle.query?.preAggregation,
                addedToQueueTime: queryWithCancelHandle.addedToQueueTime,
              });
              await this.sendCancelMessageFn(queryWithCancelHandle);
            }
          }
        }

        clearInterval(heartBeatTimer);

        if (!(await redisClient.setResultAndRemoveQuery(queryKey, executionResult, processingId))) {
          this.logger('Orphaned execution result', {
            processingId,
            warn: 'Result for query was not set due to processing lock wasn\'t acquired',
            queryKey: query.queryKey,
            queuePrefix: this.redisQueuePrefix,
            requestId: query.requestId,
            metadata: query.query?.metadata,
            preAggregationId: query.query?.preAggregation?.preAggregationId,
            newVersionEntry: query.query?.newVersionEntry,
            preAggregation: query.query?.preAggregation,
            addedToQueueTime: query.addedToQueueTime,
          });
        }

        await this.reconcileQueue();
      } else {
        this.logger('Skip processing', {
          processingId,
          queryKey: query && query.queryKey || queryKey,
          requestId: query && query.requestId,
          queuePrefix: this.redisQueuePrefix,
          processingLockAcquired,
          query,
          insertedCount,
          activeKeys,
          activated,
          queryExists: !!query
        });
        const currentProcessingId = await redisClient.freeProcessingLock(queryKey, processingId, activated);
        if (currentProcessingId) {
          this.logger('Skipping free processing lock', {
            processingId,
            currentProcessingId,
            queryKey: query && query.queryKey || queryKey,
            requestId: query && query.requestId,
            queuePrefix: this.redisQueuePrefix,
            processingLockAcquired,
            query,
            insertedCount,
            activeKeys,
            activated,
            queryExists: !!query
          });
        }
      }
    } catch (e) {
      this.logger('Queue storage error', {
        queryKey: query && query.queryKey || queryKey,
        requestId: query && query.requestId,
        error: (e.stack || e).toString(),
        queuePrefix: this.redisQueuePrefix
      });
    } finally {
      this.queueDriver.release(redisClient);
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
      this.logger('Error while cancel', {
        queryKey: query.queryKey,
        error: e.stack || e,
        queuePrefix: this.redisQueuePrefix,
        requestId: query.requestId
      });
    }
  }

  redisHash(queryKey) {
    return this.queueDriver.redisHash(queryKey);
  }
}
