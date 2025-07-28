import { EventEmitter } from 'events';
import { getEnv, getProcessUid } from '@cubejs-backend/shared';
import {
  QueueDriverInterface,
  QueryKey,
  QueryKeyHash,
  QueueId,
  QueryDef,
  QueryStageStateResponse,
  AddToQueueOptions
} from '@cubejs-backend/base-driver';
import { CubeStoreQueueDriver } from '@cubejs-backend/cubestore-driver';

import { TimeoutError } from './TimeoutError';
import { ContinueWaitError } from './ContinueWaitError';
import { LocalQueueDriver } from './LocalQueueDriver';
import { QueryStream } from './QueryStream';
import { CacheAndQueryDriverType } from './QueryOrchestrator';

export type CancelHandlerFn = (query: QueryDef) => Promise<void>;
export type QueryHandlerFn = (query: QueryDef, cancelHandler: CancelHandlerFn) => Promise<unknown>;
export type StreamHandlerFn = (query: QueryDef, stream: QueryStream) => Promise<unknown>;
export type QueryHandlersMap = Record<string, QueryHandlerFn>;

export type SendProcessMessageFn = (queryKeyHash: QueryKeyHash, queueId: QueueId | null) => Promise<void> | void;
export type SendCancelMessageFn = (query: QueryDef, queueId: QueueId | null) => Promise<void> | void;

export type ExecuteInQueueOptions = Omit<AddToQueueOptions, 'queueId'> & {
  spanId?: string
};

export type QueryQueueOptions = {
  cacheAndQueueDriver: CacheAndQueryDriverType;
  logger: (message, event) => void;
  sendCancelMessageFn?: SendCancelMessageFn;
  sendProcessMessageFn?: SendProcessMessageFn;
  cancelHandlers: Record<string, CancelHandlerFn>;
  queryHandlers: QueryHandlersMap;
  streamHandler?: StreamHandlerFn;
  processUid?: string;
  concurrency?: number,
  continueWaitTimeout?: number,
  executionTimeout?: number,
  orphanedTimeout?: number,
  heartBeatInterval?: number,
  redisPool?: any,
  cubeStoreDriverFactory?: any,
  queueDriverFactory?: (cacheAndQueueDriver: string, queueDriverOptions: any) => QueueDriverInterface,
  skipQueue?: boolean,
};

function factoryQueueDriver(cacheAndQueueDriver: string, queueDriverOptions): QueueDriverInterface {
  switch (cacheAndQueueDriver || 'memory') {
    case 'memory':
      return new LocalQueueDriver(queueDriverOptions);
    case 'cubestore':
      if (!queueDriverOptions.cubeStoreDriverFactory) {
        throw new Error('cubeStoreDriverFactory is a required option for Cube Store queue driver');
      }

      return new CubeStoreQueueDriver(
        queueDriverOptions.cubeStoreDriverFactory,
        queueDriverOptions
      );
    default:
      throw new Error(`Unknown queue driver: ${cacheAndQueueDriver}`);
  }
}

export class QueryQueue {
  protected concurrency: number;

  protected continueWaitTimeout: number;

  protected executionTimeout: number;

  protected orphanedTimeout: number;

  protected heartBeatInterval: number;

  protected readonly sendProcessMessageFn: SendProcessMessageFn;

  protected readonly sendCancelMessageFn: SendCancelMessageFn;

  protected readonly queryHandlers: QueryHandlersMap;

  protected readonly streamHandler: StreamHandlerFn | undefined;

  protected cancelHandlers: Record<string, CancelHandlerFn>;

  protected logger: any;

  protected processUid: string;

  protected queueDriver: QueueDriverInterface;

  protected skipQueue: boolean;

  /**
   * Persistent queries streams maps.
   */
  protected readonly streams = new Map();

  /**
   * Notify streaming queries when streaming has been started and stream is available.
   */
  protected readonly streamEvents = new EventEmitter();

  public constructor(
      protected readonly redisQueuePrefix: string,
      options: QueryQueueOptions
  ) {
    this.concurrency = options.concurrency || 2;
    this.continueWaitTimeout = options.continueWaitTimeout || 5;
    this.executionTimeout = options.executionTimeout || getEnv('dbQueryTimeout');
    this.orphanedTimeout = options.orphanedTimeout || 120;
    this.heartBeatInterval = options.heartBeatInterval || 30;

    this.sendProcessMessageFn = options.sendProcessMessageFn || ((queryKey, queryId) => { this.processQuery(queryKey, queryId); });
    this.sendCancelMessageFn = options.sendCancelMessageFn || ((query, queueId) => { this.processCancel(query, queueId); });
    this.queryHandlers = options.queryHandlers;
    this.streamHandler = options.streamHandler;
    this.cancelHandlers = options.cancelHandlers;
    this.logger = options.logger || ((message, event) => console.log(`${message} ${JSON.stringify(event)}`));
    this.processUid = options.processUid || getProcessUid();

    const queueDriverOptions = {
      redisQueuePrefix: this.redisQueuePrefix,
      concurrency: this.concurrency,
      continueWaitTimeout: this.continueWaitTimeout,
      orphanedTimeout: this.orphanedTimeout,
      heartBeatTimeout: this.heartBeatInterval * 4,
      redisPool: options.redisPool,
      cubeStoreDriverFactory: options.cubeStoreDriverFactory,
      processUid: this.processUid,
    };

    const queueDriverFactory = options.queueDriverFactory || factoryQueueDriver;

    this.queueDriver = queueDriverFactory(options.cacheAndQueueDriver, queueDriverOptions);
    this.skipQueue = options.skipQueue;
  }

  public getQueueDriver(): QueueDriverInterface {
    return this.queueDriver;
  }

  public getConcurrency(): number {
    return this.concurrency;
  }

  /**
   * Returns stream object which will be used to pipe data from data source.
   */
  public getQueryStream(queryKeyHash: QueryKeyHash): QueryStream | undefined {
    return this.streams.get(queryKeyHash);
  }

  public createQueryStream(key: QueryKeyHash, aliasNameToMember: Record<string, string>): QueryStream {
    const stream = new QueryStream({
      key,
      streams: this.streams,
      aliasNameToMember,
    });
    this.streams.set(key, stream);
    this.streamEvents.emit('streamStarted', key);

    return stream;
  }

  protected counter = 0;

  public generateQueueId() {
    return this.counter++;
  }

  /**
   * Push query to the queue and call `QueryQueue.reconcileQueue()` method if
   * `options.skipQueue` is set to `false`, execute query skipping queue
   * otherwise.
   *
   * @throw {ContinueWaitError}
   */
  public async executeInQueue(
    queryHandler: string,
    queryKey: QueryKey,
    query: QueryDef,
    priority?: number,
    executeOptions?: ExecuteInQueueOptions,
  ) {
    const options: AddToQueueOptions = {
      queueId: this.generateQueueId(),
      ...executeOptions,
    };

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
        queueId: options.queueId,
        spanId: options.spanId,
        queueSize: 0,
        queryKey: queryDef.queryKey,
        queuePrefix: this.redisQueuePrefix,
        requestId: options.requestId,
        waitingForRequestId: queryDef.requestId
      });
      if (queryHandler === 'stream') {
        throw new Error('Streaming queries to Cube Store aren\'t supported');
      }
      const result = await this.processQuerySkipQueue(queryDef, options.queueId);
      return this.parseResult(result);
    }

    const queueConnection = await this.queueDriver.createConnection();
    let waitingContext;
    try {
      if (priority == null) {
        priority = 0;
      }

      if (!(priority >= -10000 && priority <= 10000)) {
        throw new Error('Priority should be between -10000 and 10000');
      }

      // Result here won't be fetched for a forced build query and a jobbed build
      // query (initialized by the /cubejs-system/v1/pre-aggregations/jobs
      // endpoint).
      let result = !query.forceBuild && await queueConnection.getResult(queryKey);
      if (result && !result.streamResult) {
        return this.parseResult(result);
      }

      const queryKeyHash = this.redisHash(queryKey);

      if (query.forceBuild) {
        const jobExists = await queueConnection.getQueryDef(queryKeyHash, null);
        if (jobExists) return null;
      }

      const time = new Date().getTime();
      const keyScore = time + (10000 - priority) * 1E14;

      options.orphanedTimeout = query.orphanedTimeout;

      const orphanedTimeout = 'orphanedTimeout' in query ? query.orphanedTimeout : this.orphanedTimeout;
      const orphanedTime = time + (orphanedTimeout * 1000);

      const [added, queueId, queueSize, addedToQueueTime] = await queueConnection.addToQueue(
        keyScore, queryKey, orphanedTime, queryHandler, query, priority, options
      );

      if (added > 0) {
        waitingContext = {
          queueId,
          spanId: options.spanId,
          queryKey,
          queuePrefix: this.redisQueuePrefix,
          requestId: options.requestId,
          waitingForRequestId: options.requestId
        };

        this.logger('Added to queue', {
          queueId,
          spanId: options.spanId,
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
          addedToQueueTime,
          persistent: !!queryKey.persistent,
        });
      }

      await this.reconcileQueue();

      if (!added) {
        const queryDef = await queueConnection.getQueryDef(queryKeyHash, queueId);
        if (queryDef) {
          waitingContext = {
            queueId,
            spanId: options.spanId,
            queryKey: queryDef.queryKey,
            queuePrefix: this.redisQueuePrefix,
            requestId: options.requestId,
            waitingForRequestId: queryDef.requestId
          };
        }
      }

      const [active, toProcess] = await queueConnection.getQueryStageState(true);

      this.logger('Waiting for query', {
        ...waitingContext,
        queueSize,
        activeQueryKeys: active,
        toProcessQueryKeys: toProcess,
        active: active.indexOf(queryKeyHash) !== -1,
        queueIndex: toProcess.indexOf(queryKeyHash),
      });

      // Stream processing goes here under assumption there's no way of a stream close just after it was added to the `streams` map.
      // Otherwise `streamStarted` event listener should go before the `reconcileQueue` call.
      // TODO: Fix an issue with a fast execution of stream handler which caused by removal of QueryStream from streams,
      // while EventListener doesnt start to listen for started stream event
      if (queryHandler === 'stream') {
        const self = this;
        result = await new Promise((resolve) => {
          let timeoutTimerId = null;

          const onStreamStarted = (streamStartedHash) => {
            if (streamStartedHash === queryKeyHash) {
              if (timeoutTimerId) {
                clearTimeout(timeoutTimerId);
              }

              resolve(self.getQueryStream(queryKeyHash));
            }
          };

          self.streamEvents.addListener('streamStarted', onStreamStarted);

          const stream = this.getQueryStream(queryKeyHash);
          if (stream) {
            self.streamEvents.removeListener('streamStarted', onStreamStarted);
            resolve(stream);
          } else {
            timeoutTimerId = setTimeout(
              () => {
                self.streamEvents.removeListener('streamStarted', onStreamStarted);
                resolve(null);
              },
              this.continueWaitTimeout * 10000
            );
          }
        });
      } else {
        // Result here won't be fetched for a jobed build query (initialized by
        // the /cubejs-system/v1/pre-aggregations/jobs endpoint).
        result = !query.isJob && await queueConnection.getResultBlocking(queryKeyHash, queueId);
      }

      // We don't want to throw the ContinueWaitError for a jobed build query.
      if (!query.isJob && !result) {
        throw new ContinueWaitError();
      }

      return this.parseResult(result);
    } catch (error) {
      if (waitingContext) {
        this.logger('Finished waiting for query', waitingContext);
      }
      throw error;
    } finally {
      this.queueDriver.release(queueConnection);
    }
  }

  /**
   * @throw {Error}
   */
  protected parseResult(result: any): any {
    if (!result) {
      return;
    }
    if (result instanceof QueryStream) {
      // eslint-disable-next-line consistent-return
      return result;
    }
    if (result.error) {
      throw new Error(result.error); // TODO
    } else {
      // eslint-disable-next-line consistent-return
      return result.result;
    }
  }

  protected reconcileAgain: boolean = false;

  protected reconcilePromise: Promise<void> | null = null;

  /**
   * Run query queue reconciliation flow by calling internal `reconcileQueueImpl`
   * method. Returns promise which will be resolved with the reconciliation
   * result.
   */
  public async reconcileQueue(): Promise<void> {
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

  public async shutdown(): Promise<boolean> {
    if (this.reconcilePromise) {
      await this.reconcilePromise;

      return true;
    } else {
      return false;
    }
  }

  /**
   * Returns a full list of queued queries, including stalled, orphaned, active
   * and planned to be processed with their statuses and queries definitions.
   *
   * @returns {Promise<Object>}
   */
  public async getQueries() {
    const queueConnection = await this.queueDriver.createConnection();
    try {
      const [stalledQueries, orphanedQueries, activeQueries, toProcessQueries] = await Promise.all([
        queueConnection.getStalledQueries(),
        queueConnection.getOrphanedQueries(),
        queueConnection.getActiveQueries(),
        queueConnection.getToProcessQueries()
      ]);

      /**
       * @param {QueryKeysTuple[]} arr
       */
      const mapWithDefinition = (arr) => Promise.all(arr.map(async ([queryKey, queueId]) => ({
        ...(await queueConnection.getQueryDef(queryKey, queueId)),
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
      this.queueDriver.release(queueConnection);
    }
  }

  public async cancelQuery(queryKey: QueryKeyHash, queueId: QueueId | null): Promise<boolean> {
    const queueConnection = await this.queueDriver.createConnection();
    try {
      const query = await queueConnection.cancelQuery(queryKey, queueId);

      if (query) {
        this.logger('Cancelling query manual', {
          queueId,
          queryKey: query.queryKey,
          queuePrefix: this.redisQueuePrefix,
          requestId: query.requestId,
          metadata: query.query?.metadata,
          preAggregationId: query.query?.preAggregation?.preAggregationId,
          newVersionEntry: query.query?.newVersionEntry,
          preAggregation: query.query?.preAggregation,
          addedToQueueTime: query.addedToQueueTime,
        });

        await this.sendCancelMessageFn(query, queueId);
      }

      return true;
    } finally {
      this.queueDriver.release(queueConnection);
    }
  }

  /**
   * Reconciliation logic: cancel stalled and orphaned queries from the queue
   * and pick some planned to be processed queries to process.
   *
   * @private
   * @returns {Promise<void>}
   */
  protected async reconcileQueueImpl() {
    const queueConnection = await this.queueDriver.createConnection();
    try {
      const toCancel = await queueConnection.getQueriesToCancel();

      await Promise.all(toCancel.map(async ([queryKey, queueId]) => {
        const [queryDef] = await queueConnection.getQueryAndRemove(queryKey, queueId);
        if (queryDef) {
          this.logger('Removing orphaned query', {
            queueId: queueId || queryDef.queueId /** Special handling for Redis */,
            queryKey: queryDef.queryKey,
            queuePrefix: this.redisQueuePrefix,
            requestId: queryDef.requestId,
            metadata: queryDef.query?.metadata,
            preAggregationId: queryDef.query?.preAggregation?.preAggregationId,
            newVersionEntry: queryDef.query?.newVersionEntry,
            preAggregation: queryDef.query?.preAggregation,
            addedToQueueTime: queryDef.addedToQueueTime,
          });

          await this.sendCancelMessageFn(queryDef, queueId);
        }
      }));

      const [_active, toProcess] = await queueConnection.getActiveAndToProcess();

      const tasks = toProcess
        .filter(([queryKey, _queueId]) => {
          const subKeys = queryKey.split('@');
          if (subKeys.length === 1) {
            // common queries
            return true;
          } else if (subKeys[1] === this.processUid) {
            // current process persistent queries
            return true;
          } else {
            // other processes persistent queries
            return false;
          }
        })
        .slice(0, this.concurrency)
        .map(([queryKey, queueId]) => this.sendProcessMessageFn(queryKey, queueId));

      await Promise.all(tasks);
    } finally {
      this.queueDriver.release(queueConnection);
    }
  }

  /**
   * Apply query timeout to the query. Throw if query execution time takes more
   * than the specified timeout. Returns resolved `promise` value.
   *
   * @throw {TimeoutError}
   */
  protected queryTimeout<T>(promise: Promise<T>): Promise<T> {
    let timeout;
    const { executionTimeout } = this;

    return Promise.race<T>([
      promise,
      new Promise((_resolve, reject) => {
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

  /**
   * Returns the list of queries planned to be processed and the list of active
   * queries.
   */
  public async fetchQueryStageState(): Promise<QueryStageStateResponse> {
    const queueConnection = await this.queueDriver.createConnection();
    try {
      return queueConnection.getQueryStageState(false);
    } finally {
      this.queueDriver.release(queueConnection);
    }
  }

  /**
   * Returns current state of the specified by the `stageQueryKey` query if it
   * exists.
   *
   * @returns {Promise<undefined> | Promise<{ stage: string, timeElapsed: number }>}
   */
  public async getQueryStage(stageQueryKey: QueryKey, priorityFilter?: number, queryStageState?: QueryStageStateResponse) {
    const [active, toProcess, allQueryDefs] = queryStageState || await this.fetchQueryStageState();

    const queryInQueue = Object.values(allQueryDefs).find(
      q => this.redisHash(q.stageQueryKey) === this.redisHash(stageQueryKey) &&
      (priorityFilter != null ? q.priority === priorityFilter : true)
    );
    if (queryInQueue) {
      if (active.indexOf(this.redisHash(queryInQueue.queryKey)) !== -1) {
        return {
          stage: 'Executing query',
          timeElapsed: queryInQueue.startQueryTime ? new Date().getTime() - queryInQueue.startQueryTime : undefined
        };
      }

      const index = toProcess
        .filter((queryKey) => (priorityFilter != null ? allQueryDefs[queryKey]?.priority === priorityFilter : true))
        .indexOf(this.redisHash(queryInQueue.queryKey));
      if (index !== -1) {
        return index !== -1 ? { stage: `#${index + 1} in queue` } : undefined;
      }
    }

    return undefined;
  }

  /**
   * Execute query without adding it to the queue.
   */
  protected async processQuerySkipQueue(query: QueryDef, queueId: QueueId) {
    const startQueryTime = (new Date()).getTime();
    this.logger('Performing query', {
      queueId,
      queueSize: 0,
      queryKey: query.queryKey,
      queuePrefix: this.redisQueuePrefix,
      requestId: query.requestId,
      timeInQueue: 0
    });
    let executionResult;
    let handler;

    try {
      // TODO handle streams
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
        queueId,
        queueSize: 0,
        duration: ((new Date()).getTime() - startQueryTime),
        queryKey: query.queryKey,
        queuePrefix: this.redisQueuePrefix,
        requestId: query.requestId,
        timeInQueue: 0
      });
    } catch (e: any) {
      executionResult = {
        error: (e.message || e).toString() // TODO error handling
      };
      this.logger('Error while querying', {
        queueId,
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
            queueId,
            queryKey: query.queryKey,
            queuePrefix: this.redisQueuePrefix,
            requestId: query.requestId,
          });
          await handler(query);
        }
      }
    }
    return executionResult;
  }

  /**
   * Processing query specified by the `queryKey`. This method encapsulates most
   * of the logic related to the queue updates, heartbeat, etc.
   */
  protected async processQuery(queryKeyHashed: QueryKeyHash, queueId: QueueId | null): Promise<void> {
    const queueConnection = await this.queueDriver.createConnection();

    let insertedCount;
    let activeKeys;
    let queueSize;
    let query;
    let processingLockAcquired;

    try {
      const processingId = queueId || /** for Redis only */ await queueConnection.getNextProcessingId();
      const retrieveResult = await queueConnection.retrieveForProcessing(queryKeyHashed, processingId);

      if (retrieveResult) {
        let retrieveQueueId;

        [insertedCount, retrieveQueueId, activeKeys, queueSize, query, processingLockAcquired] = retrieveResult;

        // Backward compatibility for old Cube Store, Redis and Memory
        if (retrieveQueueId) {
          queueId = retrieveQueueId;
        }
      }

      const activated = activeKeys && activeKeys.indexOf(queryKeyHashed) !== -1;
      if (!query) {
        query = await queueConnection.getQueryDef(queryKeyHashed, null);
      }

      if (query && insertedCount && activated && processingLockAcquired) {
        let executionResult;
        const startQueryTime = (new Date()).getTime();
        const timeInQueue = (new Date()).getTime() - query.addedToQueueTime;
        this.logger('Performing query', {
          queueId,
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
        await queueConnection.optimisticQueryUpdate(queryKeyHashed, { startQueryTime }, processingId, queueId);

        let queryProcessHeartbeat = Date.now();
        const heartBeatTimer = setInterval(
          () => {
            if ((Date.now() - queryProcessHeartbeat) > 5 * 60 * 1000) {
              this.logger('Query processing heartbeat', {
                queueId,
                queryKey: query.queryKey,
                requestId: query.requestId,
              });
              queryProcessHeartbeat = Date.now();
            }

            return queueConnection.updateHeartBeat(queryKeyHashed, queueId);
          },
          this.heartBeatInterval * 1000
        );
        try {
          const handler = query?.queryHandler;
          switch (handler) {
            case 'stream':
              // eslint-disable-next-line no-case-declarations
              const queryStream = this.createQueryStream(queryKeyHashed, query.query?.aliasNameToMember);

              try {
                await this.streamHandler(query.query, queryStream);
                // CubeStore has special handling for null
                executionResult = {
                  streamResult: true
                };
              } finally {
                if (this.streams.get(queryKeyHashed) === queryStream) {
                  this.streams.delete(queryKeyHashed);
                }
              }
              break;
            default:
              executionResult = {
                result: await this.queryTimeout(
                  this.queryHandlers[handler](
                    query.query,
                    async (cancelHandler) => {
                      try {
                        await queueConnection.optimisticQueryUpdate(queryKeyHashed, { cancelHandler }, processingId, queueId);
                      } catch (e: any) {
                        this.logger('Error while query update', {
                          queueId,
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
                    },
                  )
                )
              };
              break;
          }

          this.logger('Performing query completed', {
            queueId,
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
        } catch (e: any) {
          executionResult = {
            error: (e.message || e).toString() // TODO error handling
          };
          this.logger('Error while querying', {
            queueId,
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
            const queryWithCancelHandle = await queueConnection.getQueryDef(queryKeyHashed, queueId);
            if (queryWithCancelHandle) {
              this.logger('Cancelling query due to timeout', {
                queueId,
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

              await this.sendCancelMessageFn(queryWithCancelHandle, queueId);
            }
          }
        } finally {
          // catch block can throw an exception, it's why it's important to clearInterval here
          clearInterval(heartBeatTimer);
        }

        if (!(await queueConnection.setResultAndRemoveQuery(queryKeyHashed, executionResult, processingId, queueId))) {
          this.logger('Orphaned execution result', {
            queueId,
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
        // TODO Ideally streaming queries should reconcile queue here after waiting on open slot however in practice continue wait timeout reconciles faster CPU-wise
        // if (query?.queryHandler === 'stream') {
        //   const [active] = await queueConnection.getQueryStageState(true);
        //   if (active && active.length > 0) {
        //     await Promise.race(active.map(keyHash => queueConnection.getResultBlocking(keyHash)));
        //     await this.reconcileQueue();
        //   }
        // }

        this.logger('Skip processing', {
          queueId,
          processingId,
          queryKey: query && query.queryKey || queryKeyHashed,
          requestId: query && query.requestId,
          queuePrefix: this.redisQueuePrefix,
          processingLockAcquired,
          query,
          insertedCount,
          activeKeys,
          activated,
          queryExists: !!query
        });
        await queueConnection.freeProcessingLock(queryKeyHashed, processingId, activated);
      }
    } catch (e: any) {
      this.logger('Queue storage error', {
        queueId,
        queryKey: query && query.queryKey || queryKeyHashed,
        requestId: query && query.requestId,
        error: (e.stack || e).toString(),
        queuePrefix: this.redisQueuePrefix
      });
    } finally {
      this.queueDriver.release(queueConnection);
    }
  }

  /**
   * Processing cancel query flow.
   */
  protected async processCancel(query: QueryDef, queueId: QueueId | null) {
    const { queryHandler } = query;

    try {
      if (!this.cancelHandlers[queryHandler]) {
        throw new Error(`No cancel handler for ${queryHandler}`);
      }

      await this.cancelHandlers[queryHandler](query);
    } catch (e: any) {
      this.logger('Error while cancel', {
        queueId,
        queryKey: query.queryKey,
        error: e.stack || e,
        queuePrefix: this.redisQueuePrefix,
        requestId: query.requestId
      });
    }
  }

  protected redisHash(queryKey: QueryKey): QueryKeyHash {
    return this.queueDriver.redisHash(queryKey);
  }
}
