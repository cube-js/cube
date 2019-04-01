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

  async getResultBlocking(queryKey, continueWaitTimeout) {
    const resultListKey = this.resultListKey(queryKey);
    const timeoutPromise = (timeout) => new Promise((resolve) => setTimeout(() => resolve(null), timeout));

    const res = await Promise.race([
      this.getResultPromise(resultListKey),
      timeoutPromise(continueWaitTimeout || this.continueWaitTimeout * 1000),
    ]);
    if (res) {
      delete this.resultPromises[resultListKey];
    }
    return res;
  }

  async getResult(queryKey) {
    const resultListKey = this.resultListKey(queryKey);
    if (this.resultPromises[resultListKey]) {
      return this.getResultBlocking(queryKey, 5);
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
      queryHandler, query, queryKey, stageQueryKey: options.stageQueryKey, priority
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
    delete this.toProcess[key];
    delete this.recent[key];
    delete this.queryDef[key];
    return [query];
  }

  setResultAndRemoveQuery(queryKey, executionResult) {
    const key = this.redisHash(queryKey);
    const promise = this.getResultPromise(this.resultListKey(queryKey));
    delete this.active[key];
    delete this.toProcess[key];
    delete this.recent[key];
    delete this.queryDef[key];
    promise.resolve(executionResult);
  }

  removeQuery(queryKey) {
    const key = this.redisHash(queryKey);
    delete this.active[key];
    delete this.toProcess[key];
    delete this.recent[key];
    delete this.queryDef[key];
  }

  getOrphanedQueries() {
    return this.queueArray(this.recent, new Date().getTime() - this.orphanedTimeout * 1000);
  }

  getStalledQueries() {
    return this.queueArray(this.active, new Date().getTime() - this.heartBeatTimeout * 1000);
  }

  async getQueryStageState() {
    return [this.queueArray(this.active), this.queueArray(this.toProcess), R.clone(this.queryDef)];
  }

  async getQueryDef(queryKey) {
    return this.queryDef[this.redisHash(queryKey)];
  }

  updateHeartBeat(queryKey) {
    const key = this.redisHash(queryKey);
    if (this.active[key]) {
      this.active[key] = { key, order: new Date().getTime() };
    }
  }

  retrieveForProcessing(queryKey) {
    const key = this.redisHash(queryKey);
    let added = 0;
    if (Object.keys(this.active).length < this.concurrency && !this.active[key]) {
      this.active[key] = { key, order: new Date().getTime() };
      added = 1;
    }
    return [added, null, this.queueArray(this.active), Object.keys(this.toProcess).length]; // TODO nulls
  }

  async optimisticQueryUpdate(queryKey, toUpdate) {
    const key = this.redisHash(queryKey);
    this.queryDef[key] = { ...this.queryDef[key], ...toUpdate };
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

class LocalQueueDriver extends BaseQueueDriver {
  constructor(options) {
    super();
    this.options = options;
    this.results = {};
    this.resultPromises = {};
    this.queryDef = {};
    this.toProcess = {};
    this.recent = {};
    this.active = {};
  }

  createConnection() {
    return new LocalQueueDriverConnection(this, this.options);
  }
}

module.exports = LocalQueueDriver;
