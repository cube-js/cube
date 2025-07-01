import R from 'ramda';
import { QueueDriverInterface, QueueDriverConnectionInterface } from '@cubejs-backend/base-driver';
import { BaseQueueDriver } from './BaseQueueDriver';

/**
 * @implements {QueueDriverConnectionInterface}
 */
export class LocalQueueDriverConnection {
  constructor(driver, options) {
    this.redisQueuePrefix = options.redisQueuePrefix;
    this.continueWaitTimeout = options.continueWaitTimeout;
    this.heartBeatTimeout = options.heartBeatTimeout;
    this.concurrency = options.concurrency;
    this.driver = driver;
    this.results = driver.results;
    this.resultPromises = driver.resultPromises;
    this.queryDef = driver.queryDef;
    this.toProcess = driver.toProcess;
    this.recent = driver.recent;
    this.active = driver.active;
    this.heartBeat = driver.heartBeat;
    this.processingCounter = driver.processingCounter;
    this.processingLocks = driver.processingLocks;
    this.getQueueEventsBus = options.getQueueEventsBus;
  }

  async getQueriesToCancel() {
    const [stalled, orphaned] = await Promise.all([
      this.getStalledQueries(),
      this.getOrphanedQueries(),
    ]);

    return stalled.concat(orphaned);
  }

  /**
   * @returns {Promise<GetActiveAndToProcessResponse>}
   */
  async getActiveAndToProcess() {
    const active = this.queueArrayAsTuple(this.active);
    const toProcess = this.queueArrayAsTuple(this.toProcess);

    return [
      active,
      toProcess
    ];
  }

  getResultPromise(resultListKey) {
    if (!this.resultPromises[resultListKey]) {
      let resolveMethod = null;
      this.resultPromises[resultListKey] = new Promise(resolve => {
        resolveMethod = resolve;
      });
      this.resultPromises[resultListKey].resolve = resolveMethod;
    }
    return this.resultPromises[resultListKey];
  }

  async getResultBlocking(queryKeyHash) {
    // Double redisHash apply is being used here
    const resultListKey = this.resultListKey(queryKeyHash);
    if (!this.queryDef[queryKeyHash] && !this.resultPromises[resultListKey]) {
      return null;
    }
    const timeoutPromise = (timeout) => new Promise((resolve) => setTimeout(() => resolve(null), timeout));

    const res = await Promise.race([
      this.getResultPromise(resultListKey),
      timeoutPromise(this.continueWaitTimeout * 1000),
    ]);

    if (res) {
      delete this.resultPromises[resultListKey];
    }
    return res;
  }

  /**
   * Returns promise wich will be resolved with the specified by the
   * queryKey query result or null if query was not added to the
   * processing.
   * @param {*} queryKey
   * @returns {Promise<null | *>}
   */
  async getResult(queryKey) {
    const resultListKey = this.resultListKey(queryKey);
    if (this.resultPromises[resultListKey] && this.resultPromises[resultListKey].resolved) {
      return this.getResultBlocking(queryKey);
    }

    return null;
  }

  /**
   * @protected
   */
  queueArray(queueObj, orderFilterLessThan) {
    return R.pipe(
      R.values,
      R.filter(orderFilterLessThan ? q => q.order < orderFilterLessThan : R.identity),
      R.sortBy(q => q.order),
      R.map(q => q.key)
    )(queueObj);
  }

  /**
   * @protected
   * @param queueObj
   * @param orderFilterLessThan
   * @returns {QueryKeysTuple[]}
   */
  queueArrayAsTuple(queueObj, orderFilterLessThan) {
    return R.pipe(
      R.values,
      R.filter(orderFilterLessThan ? q => q.order < orderFilterLessThan : R.identity),
      R.sortBy(q => q.order),
      R.map(q => [q.key, q.queueId])
    )(queueObj);
  }

  /**
   * Adds specified by the queryKey query to the queue, returns tuple
   * with the operation result.
   *
   * @typedef {[added: number, _b: null, _c: null, toProcessLength: number, addedTime: number]} AddedTuple
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
    const queryQueueObj = {
      queueId: options.queueId,
      queryHandler,
      query,
      queryKey,
      stageQueryKey: options.stageQueryKey,
      priority,
      requestId: options.requestId,
      addedToQueueTime: new Date().getTime()
    };

    const key = this.redisHash(queryKey);
    if (!this.queryDef[key]) {
      this.queryDef[key] = queryQueueObj;
    }

    let added = 0;

    if (!this.toProcess[key] && !this.active[key]) {
      this.toProcess[key] = {
        order: keyScore,
        queueId: options.queueId,
        key
      };

      added = 1;
    }

    this.recent[key] = {
      order: orphanedTime,
      key,
      queueId: options.queueId,
    };

    if (this.getQueueEventsBus) {
      this.getQueueEventsBus().emit({
        event: 'addedToQueue',
        redisQueuePrefix: this.redisQueuePrefix,
        queryKey: this.redisHash(queryKey),
        payload: queryQueueObj
      });
    }

    return [
      added,
      queryQueueObj.queueId,
      Object.keys(this.toProcess).length,
      queryQueueObj.addedToQueueTime
    ];
  }

  getToProcessQueries() {
    return this.queueArrayAsTuple(this.toProcess);
  }

  getActiveQueries() {
    return this.queueArrayAsTuple(this.active);
  }

  async getQueryAndRemove(queryKey) {
    const key = this.redisHash(queryKey);
    const query = this.queryDef[key];
    delete this.active[key];
    delete this.heartBeat[key];
    delete this.toProcess[key];
    delete this.recent[key];
    delete this.queryDef[key];
    delete this.processingLocks[key];
    return [query];
  }

  async cancelQuery(queryKey) {
    const [query] = await this.getQueryAndRemove(queryKey);

    if (this.getQueueEventsBus) {
      this.getQueueEventsBus().emit({
        event: 'cancelQuery',
        redisQueuePrefix: this.redisQueuePrefix,
        queryKey: this.redisHash(queryKey),
        payload: query
      });
    }

    return query;
  }

  async setResultAndRemoveQuery(queryKey, executionResult, processingId) {
    const key = this.redisHash(queryKey);
    if (this.processingLocks[key] !== processingId) {
      return false;
    }
    const promise = this.getResultPromise(this.resultListKey(queryKey));
    delete this.active[key];
    delete this.heartBeat[key];
    delete this.toProcess[key];
    delete this.recent[key];
    delete this.queryDef[key];
    delete this.processingLocks[key];
    promise.resolved = true;
    promise.resolve(executionResult);

    if (this.getQueueEventsBus) {
      this.getQueueEventsBus().emit({
        event: 'setResultAndRemoveQuery',
        redisQueuePrefix: this.redisQueuePrefix,
        queryKey: this.redisHash(queryKey),
        payload: executionResult
      });
    }

    return true;
  }

  getNextProcessingId() {
    this.processingCounter.counter = this.processingCounter.counter ? this.processingCounter.counter + 1 : 1;
    return this.processingCounter.counter;
  }

  getOrphanedQueries() {
    return this.queueArrayAsTuple(this.recent, new Date().getTime());
  }

  getStalledQueries() {
    return this.queueArrayAsTuple(this.heartBeat, new Date().getTime() - this.heartBeatTimeout * 1000);
  }

  async getQueryStageState(onlyKeys) {
    return [this.queueArray(this.active), this.queueArray(this.toProcess), onlyKeys ? {} : R.clone(this.queryDef)];
  }

  async getQueryDef(queryKey) {
    return this.queryDef[queryKey];
  }

  /**
   * Updates heart beat for the processing query by its `queryKey`.
   *
   * @param {string} queryKey
   */
  updateHeartBeat(queryKey) {
    const key = this.redisHash(queryKey);
    if (this.heartBeat[key]) {
      this.heartBeat[key] = { key, order: new Date().getTime() };
    }
  }

  retrieveForProcessing(queryKey, processingId) {
    const key = this.redisHash(queryKey);
    let lockAcquired = false;

    if (!this.processingLocks[key]) {
      this.processingLocks[key] = processingId;
      lockAcquired = true;
    } else {
      return null;
    }

    let added = 0;

    if (Object.keys(this.active).length < this.concurrency && !this.active[key]) {
      this.active[key] = { key, order: processingId, queueId: processingId };
      delete this.toProcess[key];

      added = 1;
    }

    this.heartBeat[key] = { key, order: new Date().getTime() };

    if (this.getQueueEventsBus) {
      this.getQueueEventsBus().emit({
        event: 'retrievedForProcessing',
        redisQueuePrefix: this.redisQueuePrefix,
        queryKey: this.redisHash(queryKey),
        payload: this.queryDef[key]
      });
    }

    return [
      added,
      this.queryDef[key]?.queueId,
      this.queueArray(this.active),
      Object.keys(this.toProcess).length,
      this.queryDef[key],
      lockAcquired
    ];
  }

  freeProcessingLock(queryKey, processingId, activated) {
    const key = this.redisHash(queryKey);
    if (this.processingLocks[key] === processingId) {
      delete this.processingLocks[key];
      if (activated) {
        delete this.active[key];
      }
    }
  }

  async optimisticQueryUpdate(queryKey, toUpdate, processingId) {
    const key = this.redisHash(queryKey);
    if (this.processingLocks[key] !== processingId) {
      return false;
    }
    this.queryDef[key] = { ...this.queryDef[key], ...toUpdate };
    return true;
  }

  // eslint-disable-next-line @typescript-eslint/no-empty-function
  release() {
  }

  /**
   * Returns cache key to the specified by the queryKey query and the
   * specified by the suffix query state.
   * @param {*} queryKey
   * @param {string} suffix
   * @returns {string}
   */
  queryRedisKey(queryKey, suffix) {
    return `${this.redisQueuePrefix}_${this.redisHash(queryKey)}_${suffix}`;
  }

  /**
   * Returns cache key to the cached query result.
   * @param {*} queryKey
   * @returns {string}
   */
  resultListKey(queryKey) {
    return this.queryRedisKey(queryKey, 'RESULT');
  }

  /**
   * Returns hash sum of the query specified by the queryKey.
   * @param {*} queryKey
   * @returns {string}
   */
  redisHash(queryKey) {
    return this.driver.redisHash(queryKey);
  }
}

const results = {};
const resultPromises = {};
const queryDef = {};
const toProcess = {};
const recent = {};
const active = {};
const heartBeat = {};
const processingCounters = {};
const processingLocks = {};

/**
 * @implements {QueueDriverInterface}
 */
export class LocalQueueDriver extends BaseQueueDriver {
  constructor(options) {
    super(options.processUid);
    this.options = options;
    results[options.redisQueuePrefix] = results[options.redisQueuePrefix] || {};
    resultPromises[options.redisQueuePrefix] = resultPromises[options.redisQueuePrefix] || {};
    queryDef[options.redisQueuePrefix] = queryDef[options.redisQueuePrefix] || {};
    toProcess[options.redisQueuePrefix] = toProcess[options.redisQueuePrefix] || {};
    recent[options.redisQueuePrefix] = recent[options.redisQueuePrefix] || {};
    active[options.redisQueuePrefix] = active[options.redisQueuePrefix] || {};
    heartBeat[options.redisQueuePrefix] = heartBeat[options.redisQueuePrefix] || {};
    processingCounters[options.redisQueuePrefix] = processingCounters[options.redisQueuePrefix] || {};
    processingLocks[options.redisQueuePrefix] = processingLocks[options.redisQueuePrefix] || {};
    this.results = results[options.redisQueuePrefix];
    this.resultPromises = resultPromises[options.redisQueuePrefix];
    this.queryDef = queryDef[options.redisQueuePrefix];
    this.toProcess = toProcess[options.redisQueuePrefix];
    this.recent = recent[options.redisQueuePrefix];
    this.active = active[options.redisQueuePrefix];
    this.heartBeat = heartBeat[options.redisQueuePrefix];
    this.processingCounter = processingCounters[options.redisQueuePrefix];
    this.processingLocks = processingLocks[options.redisQueuePrefix];
  }

  createConnection() {
    return new LocalQueueDriverConnection(this, this.options);
  }

  release(client) {
    client.release();
  }
}
