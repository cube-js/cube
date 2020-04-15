const R = require('ramda');
const BaseQueueDriver = require('./BaseQueueDriver');

class LocalQueueDriverConnection {
  constructor(driver, options) {
    this.redisQueuePrefix = options.redisQueuePrefix;
    this.continueWaitTimeout = options.continueWaitTimeout;
    this.orphanedTimeout = options.orphanedTimeout;
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

  async getResultBlocking(queryKey) {
    const resultListKey = this.resultListKey(queryKey);
    if (!this.queryDef[this.redisHash(queryKey)] && !this.resultPromises[resultListKey]) {
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

  async getResult(queryKey) {
    const resultListKey = this.resultListKey(queryKey);
    if (this.resultPromises[resultListKey] && this.resultPromises[resultListKey].resolved) {
      return this.getResultBlocking(queryKey);
    }
    return null;
  }

  queueArray(queueObj, orderFilterLessThan) {
    return R.pipe(
      R.values,
      R.filter(orderFilterLessThan ? q => q.order < orderFilterLessThan : R.identity),
      R.sortBy(q => q.order),
      R.map(q => q.key)
    )(queueObj);
  }

  addToQueue(keyScore, queryKey, time, queryHandler, query, priority, options) {
    const queryQueueObj = {
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
    if (!this.toProcess[key]) {
      this.toProcess[key] = {
        order: keyScore,
        key
      };
      added = 1;
    }
    this.recent[key] = { order: time, key };

    return [added, null, null, Object.keys(this.toProcess).length]; // TODO nulls
  }

  getToProcessQueries() {
    return this.queueArray(this.toProcess);
  }

  getActiveQueries() {
    return this.queueArray(this.active);
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
    return true;
  }

  getNextProcessingId() {
    this.processingCounter.counter = this.processingCounter.counter ? this.processingCounter.counter + 1 : 1;
    return this.processingCounter.counter;
  }

  getOrphanedQueries() {
    return this.queueArray(this.recent, new Date().getTime() - this.orphanedTimeout * 1000);
  }

  getStalledQueries() {
    return this.queueArray(this.heartBeat, new Date().getTime() - this.heartBeatTimeout * 1000);
  }

  async getQueryStageState(onlyKeys) {
    return [this.queueArray(this.active), this.queueArray(this.toProcess), onlyKeys ? {} : R.clone(this.queryDef)];
  }

  async getQueryDef(queryKey) {
    return this.queryDef[this.redisHash(queryKey)];
  }

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
    }
    let added = 0;
    if (Object.keys(this.active).length < this.concurrency && !this.active[key]) {
      this.active[key] = { key, order: processingId };
      added = 1;
    }
    this.heartBeat[key] = { key, order: new Date().getTime() };
    return [
      added, null, this.queueArray(this.active), Object.keys(this.toProcess).length, this.queryDef[key], lockAcquired
    ]; // TODO nulls
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

  release() {
  }

  queryRedisKey(queryKey, suffix) {
    return `${this.redisQueuePrefix}_${this.redisHash(queryKey)}_${suffix}`;
  }

  resultListKey(queryKey) {
    return this.queryRedisKey(queryKey, 'RESULT');
  }

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

class LocalQueueDriver extends BaseQueueDriver {
  constructor(options) {
    super();
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

module.exports = LocalQueueDriver;
