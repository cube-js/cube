import R from 'ramda';
import {
  QueueDriverConnectionInterface,
  QueryKey,
  QueryKeyHash,
  QueueId,
  ProcessingId,
  QueryDef,
  AddToQueueQuery,
  AddToQueueOptions,
  AddToQueueResponse,
  QueryKeysTuple,
  GetActiveAndToProcessResponse,
  QueryStageStateResponse,
  RetrieveForProcessingResponse,
  QueueDriverOptions
} from '@cubejs-backend/base-driver';
import {
  LocalQueueDriver
} from './LocalQueueDriver';

export interface QueueItem {
  order: number;
  key: any;
  queueId: any;
}

export interface QueryQueueObject {
  queueId: any;
  queryHandler: string;
  query: any;
  queryKey: any;
  stageQueryKey: string;
  priority: number;
  requestId: string;
  addedToQueueTime: number;
}

export interface PromiseWithResolve<T = any> extends Promise<T> {
  resolve?: (value: T) => void;
  resolved?: boolean;
}

export interface ProcessingCounter {
  counter: number;
}

export class LocalQueueDriverConnectionState {
  public resultPromises: Record<string, PromiseWithResolve> = {};

  public queryDef: Record<string, QueryQueueObject> = {};

  public toProcess: Record<string, QueueItem> = {};

  public recent: Record<string, QueueItem> = {};

  public active: Record<string, QueueItem> = {};

  public heartBeat: Record<string, QueueItem> = {};

  public processingCounter: ProcessingCounter = { counter: 1 };

  public processingLocks: Record<string, any> = {};
}

export class LocalQueueDriverConnection implements QueueDriverConnectionInterface {
  private redisQueuePrefix: string;

  private continueWaitTimeout: number;

  private heartBeatTimeout: number;

  private concurrency: number;

  private driver: LocalQueueDriver;

  private state: LocalQueueDriverConnectionState;

  public constructor(driver: LocalQueueDriver, state: LocalQueueDriverConnectionState, options: QueueDriverOptions) {
    this.redisQueuePrefix = options.redisQueuePrefix;
    this.continueWaitTimeout = options.continueWaitTimeout;
    this.heartBeatTimeout = options.heartBeatTimeout;
    this.concurrency = options.concurrency;
    this.driver = driver;
    this.state = state;
  }

  public async getQueriesToCancel(): Promise<QueryKeysTuple[]> {
    const [stalled, orphaned] = await Promise.all([
      this.getStalledQueries(),
      this.getOrphanedQueries(),
    ]);

    return stalled.concat(orphaned);
  }

  public async getActiveAndToProcess(): Promise<GetActiveAndToProcessResponse> {
    const activeQueries = this.queueArrayAsTuple(this.state.active);
    const toProcessQueries = this.queueArrayAsTuple(this.state.toProcess);

    return [
      activeQueries,
      toProcessQueries
    ];
  }

  public getResultPromise(resultListKey: string): PromiseWithResolve {
    if (!this.state.resultPromises[resultListKey]) {
      let resolveMethod: ((value: any) => void) | undefined;
      this.state.resultPromises[resultListKey] = new Promise(resolve => {
        resolveMethod = resolve;
      }) as PromiseWithResolve;
      this.state.resultPromises[resultListKey].resolve = resolveMethod;
    }

    return this.state.resultPromises[resultListKey];
  }

  public async getResultBlocking(queryKeyHash: QueryKeyHash, _queueId?: QueueId): Promise<any> {
    const resultListKey = this.resultListKey(queryKeyHash);
    if (!this.state.queryDef[queryKeyHash as unknown as string] && !this.state.resultPromises[resultListKey]) {
      return null;
    }
    const timeoutPromise = (timeout: number) => new Promise((resolve) => setTimeout(() => resolve(null), timeout));

    const res = await Promise.race([
      this.getResultPromise(resultListKey),
      timeoutPromise(this.continueWaitTimeout * 1000),
    ]);

    if (res) {
      delete this.state.resultPromises[resultListKey];
    }
    return res;
  }

  public async getResult(queryKey: QueryKey): Promise<any> {
    const resultListKey = this.resultListKey(queryKey);
    if (this.state.resultPromises[resultListKey] && this.state.resultPromises[resultListKey].resolved) {
      return this.getResultBlocking(this.redisHash(queryKey));
    }

    return null;
  }

  protected queueArray(queueObj: Record<string, QueueItem>, orderFilterLessThan?: number): string[] {
    return R.pipe(
      R.values,
      R.filter(orderFilterLessThan ? (q: QueueItem) => q.order < orderFilterLessThan : R.identity),
      R.sortBy((q: QueueItem) => q.order),
      R.map((q: QueueItem) => q.key as unknown as string)
    )(queueObj);
  }

  protected queueArrayAsTuple(queueObj: Record<string, QueueItem>, orderFilterLessThan?: number): QueryKeysTuple[] {
    return R.pipe(
      R.values,
      R.filter(orderFilterLessThan ? (q: QueueItem) => q.order < orderFilterLessThan : R.identity),
      R.sortBy((q: QueueItem) => q.order),
      R.map((q: QueueItem): QueryKeysTuple => [q.key, q.queueId])
    )(queueObj);
  }

  public async addToQueue(keyScore: number, queryKey: QueryKey, orphanedTime: number, queryHandler: string, query: AddToQueueQuery, priority: number, options: AddToQueueOptions): Promise<AddToQueueResponse> {
    const queryQueueObj: QueryQueueObject = {
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
    const keyStr = key as unknown as string;

    if (!this.state.queryDef[keyStr]) {
      this.state.queryDef[keyStr] = queryQueueObj;
    }

    let added = 0;

    if (!this.state.toProcess[keyStr] && !this.state.active[keyStr]) {
      this.state.toProcess[keyStr] = {
        order: keyScore,
        queueId: options.queueId,
        key
      };

      added = 1;
    }

    this.state.recent[keyStr] = {
      order: orphanedTime,
      key,
      queueId: options.queueId,
    };

    return [
      added,
      queryQueueObj.queueId,
      Object.keys(this.state.toProcess).length,
      queryQueueObj.addedToQueueTime
    ];
  }

  public async getToProcessQueries(): Promise<QueryKeysTuple[]> {
    return this.queueArrayAsTuple(this.state.toProcess);
  }

  public async getActiveQueries(): Promise<QueryKeysTuple[]> {
    return this.queueArrayAsTuple(this.state.active);
  }

  public async getQueryAndRemove(queryKeyHash: QueryKeyHash, _queueId?: QueueId | null): Promise<[QueryDef]> {
    const keyStr = queryKeyHash as unknown as string;
    const query = this.state.queryDef[keyStr];

    delete this.state.active[keyStr];
    delete this.state.heartBeat[keyStr];
    delete this.state.toProcess[keyStr];
    delete this.state.recent[keyStr];
    delete this.state.queryDef[keyStr];
    delete this.state.processingLocks[keyStr];

    return [query];
  }

  public async cancelQuery(queryKey: QueryKey, queueId?: QueueId | null): Promise<QueryDef | null> {
    const [query] = await this.getQueryAndRemove(this.redisHash(queryKey), queueId);
    return query;
  }

  public async setResultAndRemoveQuery(queryKeyHash: QueryKeyHash, executionResult: any, processingId: ProcessingId, _queueId?: QueueId | null): Promise<boolean> {
    const keyStr = queryKeyHash as unknown as string;
    if (this.state.processingLocks[keyStr] !== processingId) {
      return false;
    }

    const promise = this.getResultPromise(this.resultListKey(queryKeyHash));

    delete this.state.active[keyStr];
    delete this.state.heartBeat[keyStr];
    delete this.state.toProcess[keyStr];
    delete this.state.recent[keyStr];
    delete this.state.queryDef[keyStr];
    delete this.state.processingLocks[keyStr];

    promise.resolved = true;
    if (promise.resolve) {
      promise.resolve(executionResult);
    }

    return true;
  }

  public async getNextProcessingId(): Promise<ProcessingId> {
    this.state.processingCounter.counter += 1;
    return this.state.processingCounter.counter;
  }

  public async getOrphanedQueries(): Promise<QueryKeysTuple[]> {
    return this.queueArrayAsTuple(this.state.recent, new Date().getTime());
  }

  public async getStalledQueries(): Promise<QueryKeysTuple[]> {
    return this.queueArrayAsTuple(this.state.heartBeat, new Date().getTime() - this.heartBeatTimeout * 1000);
  }

  public async getQueryStageState(onlyKeys: boolean): Promise<QueryStageStateResponse> {
    return [this.queueArray(this.state.active), this.queueArray(this.state.toProcess), onlyKeys ? {} : R.clone(this.state.queryDef)];
  }

  public async getQueryDef(queryKeyHash: QueryKeyHash, _queueId?: QueueId | null): Promise<QueryDef | null> {
    return this.state.queryDef[queryKeyHash as unknown as string] || null;
  }

  public async updateHeartBeat(queryKeyHash: QueryKeyHash, queueId?: QueueId | null): Promise<void> {
    const keyStr = queryKeyHash as unknown as string;
    if (this.state.heartBeat[keyStr]) {
      this.state.heartBeat[keyStr] = { key: queryKeyHash, order: new Date().getTime(), queueId: queueId || this.state.heartBeat[keyStr].queueId };
    }
  }

  public async retrieveForProcessing(queryKeyHash: QueryKeyHash, processingId: ProcessingId): Promise<RetrieveForProcessingResponse> {
    const keyStr = queryKeyHash as unknown as string;
    let lockAcquired = false;

    if (!this.state.processingLocks[keyStr]) {
      this.state.processingLocks[keyStr] = processingId;
      lockAcquired = true;
    } else {
      return null;
    }

    let added = 0;

    if (Object.keys(this.state.active).length < this.concurrency && !this.state.active[keyStr]) {
      this.state.active[keyStr] = { key: queryKeyHash, order: Number(processingId), queueId: Number(processingId) };
      delete this.state.toProcess[keyStr];

      added = 1;
    }

    this.state.heartBeat[keyStr] = { key: queryKeyHash, order: new Date().getTime(), queueId: Number(processingId) };

    return [
      added,
      this.state.queryDef[keyStr]?.queueId || null,
      this.queueArray(this.state.active) as unknown as QueryKeyHash[],
      Object.keys(this.state.toProcess).length,
      this.state.queryDef[keyStr],
      lockAcquired
    ];
  }

  public async freeProcessingLock(queryKeyHash: QueryKeyHash, processingId: ProcessingId, activated: any): Promise<void> {
    const keyStr = queryKeyHash as unknown as string;
    if (this.state.processingLocks[keyStr] === processingId) {
      delete this.state.processingLocks[keyStr];
      if (activated) {
        delete this.state.active[keyStr];
      }
    }
  }

  public async optimisticQueryUpdate(queryKeyHash: QueryKeyHash, toUpdate: any, processingId: ProcessingId, _queueId?: QueueId | null): Promise<boolean> {
    const keyStr = queryKeyHash as unknown as string;
    if (this.state.processingLocks[keyStr] !== processingId) {
      return false;
    }

    this.state.queryDef[keyStr] = { ...this.state.queryDef[keyStr], ...toUpdate };
    return true;
  }

  public release(): void {
    // Empty implementation as required by interface
  }

  public queryRedisKey(queryKey: QueryKey, suffix: string): string {
    return `${this.redisQueuePrefix}_${this.redisHash(queryKey)}_${suffix}`;
  }

  public resultListKey(queryKey: QueryKey | QueryKeyHash): string {
    return this.queryRedisKey(queryKey, 'RESULT');
  }

  public redisHash(queryKey: QueryKey): QueryKeyHash {
    return this.driver.redisHash(queryKey);
  }
}
