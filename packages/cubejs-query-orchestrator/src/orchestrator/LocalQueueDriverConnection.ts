import R from 'ramda';
import {
  QueueDriverConnectionInterface,
  QueryKey,
  QueryKeyHash,
  QueueId,
  QueryDef,
  AddToQueueQuery,
  AddToQueueOptions,
  AddToQueueResponse,
  QueryKeysTuple,
  GetActiveAndToProcessResponse,
  QueryStageStateResponse,
  RetrieveForProcessingSuccess,
  QueueDriverOptions,
  QueuePriority
} from '@cubejs-backend/base-driver';
import {
  LocalQueueDriver
} from './LocalQueueDriver';

export interface QueueItem {
  order: number;
  key: QueryKeyHash;
  queueId: QueueId;
}

export interface QueryDefObject {
  queueId: QueueId;
  queryHandler: string;
  query: any;
  queryKey: QueryKey;
  stageQueryKey: string;
  priority: number;
  requestId: string;
  addedToQueueTime: number;
}

export interface PromiseWithResolve<T = any> extends Promise<T> {
  resolve?: (value: T) => void;
  resolved?: boolean;
}

export class LocalQueueDriverConnectionState {
  public resultPromises: Record<QueryKeyHash, PromiseWithResolve> = {};

  public queryDef: Record<QueryKeyHash, QueryDefObject> = {};

  public toProcess: Record<QueryKeyHash, QueueItem> = {};

  public recent: Record<QueryKeyHash, QueueItem> = {};

  public active: Record<QueryKeyHash, QueueItem> = {};

  public heartBeat: Record<QueryKeyHash, QueueItem> = {};
}

export class LocalQueueDriverConnection implements QueueDriverConnectionInterface {
  private redisQueuePrefix: string;

  private continueWaitTimeout: number;

  private heartBeatTimeout: number;

  private concurrency: number;

  private orphanedTimeout: number;

  private driver: LocalQueueDriver;

  private state: LocalQueueDriverConnectionState;

  public constructor(driver: LocalQueueDriver, state: LocalQueueDriverConnectionState, options: QueueDriverOptions) {
    this.redisQueuePrefix = options.redisQueuePrefix;
    this.continueWaitTimeout = options.continueWaitTimeout;
    this.heartBeatTimeout = options.heartBeatTimeout;
    this.concurrency = options.concurrency;
    this.orphanedTimeout = options.orphanedTimeout;
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
    if (!this.state.queryDef[queryKeyHash] && !this.state.resultPromises[resultListKey]) {
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

  public async getResult(queryKey: QueryKey, _externalId?: string): Promise<any> {
    const resultListKey = this.resultListKey(queryKey);
    if (this.state.resultPromises[resultListKey] && this.state.resultPromises[resultListKey].resolved) {
      return this.getResultBlocking(this.redisHash(queryKey));
    }

    return null;
  }

  private orderedQueueItems(queueObj: Record<QueryKeyHash, QueueItem>, orderFilterLessThan?: number): QueueItem[] {
    return Object.values(queueObj)
      .filter((q) => !orderFilterLessThan || q.order < orderFilterLessThan)
      .sort((a, b) => a.order - b.order);
  }

  protected queueArray(queueObj: Record<QueryKeyHash, QueueItem>, orderFilterLessThan?: number): string[] {
    return this.orderedQueueItems(queueObj, orderFilterLessThan).map((q) => q.key);
  }

  protected queueArrayAsTuple(queueObj: Record<QueryKeyHash, QueueItem>, orderFilterLessThan?: number): QueryKeysTuple[] {
    return this.orderedQueueItems(queueObj, orderFilterLessThan).map((q) => [q.key, q.queueId]);
  }

  public async addToQueue(queryKey: QueryKey, queryHandler: string, query: AddToQueueQuery, priority: QueuePriority, options: AddToQueueOptions): Promise<AddToQueueResponse> {
    const time = new Date().getTime();
    const queryQueueObj: QueryDefObject = {
      queueId: options.queueId,
      queryHandler,
      query,
      queryKey,
      stageQueryKey: options.stageQueryKey,
      priority,
      requestId: options.requestId,
      addedToQueueTime: time
    };

    const key = this.redisHash(queryKey);

    if (!this.state.queryDef[key]) {
      this.state.queryDef[key] = queryQueueObj;
    }

    let added = 0;

    if (!this.state.toProcess[key] && !this.state.active[key]) {
      this.state.toProcess[key] = {
        // Highest priority first, oldest first within a priority
        order: time + (10000 - priority) * 1E14,
        queueId: options.queueId,
        key
      };

      added = 1;
    }

    this.state.recent[key] = {
      order: time + ((options.orphanedTimeout ?? this.orphanedTimeout) * 1000),
      key,
      queueId: options.queueId,
    };

    return [
      added,
      queryQueueObj.queueId,
      Object.keys(this.state.toProcess).length,
      queryQueueObj.addedToQueueTime,
      // There is no round-trip to save in memory, the item is left for reconcile to pick up
      null
    ];
  }

  public async getToProcessQueries(): Promise<QueryKeysTuple[]> {
    return this.queueArrayAsTuple(this.state.toProcess);
  }

  public async getActiveQueries(): Promise<QueryKeysTuple[]> {
    return this.queueArrayAsTuple(this.state.active);
  }

  public async getQueryAndRemove(queryKeyHash: QueryKeyHash, _queueId?: QueueId | null): Promise<[QueryDef]> {
    const query = this.state.queryDef[queryKeyHash];

    delete this.state.active[queryKeyHash];
    delete this.state.heartBeat[queryKeyHash];
    delete this.state.toProcess[queryKeyHash];
    delete this.state.recent[queryKeyHash];
    delete this.state.queryDef[queryKeyHash];

    return [query];
  }

  public async cancelQuery(queryKey: QueryKey, queueId?: QueueId | null): Promise<QueryDef | null> {
    const [query] = await this.getQueryAndRemove(this.redisHash(queryKey), queueId);
    return query;
  }

  public async setResultAndRemoveQuery(queryKeyHash: QueryKeyHash, executionResult: any, queueId: QueueId): Promise<boolean> {
    if (this.state.active[queryKeyHash]?.queueId !== queueId) {
      return false;
    }

    const promise = this.getResultPromise(this.resultListKey(queryKeyHash));

    delete this.state.active[queryKeyHash];
    delete this.state.heartBeat[queryKeyHash];
    delete this.state.toProcess[queryKeyHash];
    delete this.state.recent[queryKeyHash];
    delete this.state.queryDef[queryKeyHash];

    promise.resolved = true;
    if (promise.resolve) {
      promise.resolve(executionResult);
    }

    return true;
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
    return this.state.queryDef[queryKeyHash] || null;
  }

  public async updateHeartBeat(queryKeyHash: QueryKeyHash, queueId?: QueueId | null): Promise<void> {
    if (this.state.heartBeat[queryKeyHash]) {
      this.state.heartBeat[queryKeyHash] = { key: queryKeyHash, order: new Date().getTime(), queueId: queueId || this.state.heartBeat[queryKeyHash].queueId };
    }
  }

  public async retrieveForProcessing(queryKeyHash: QueryKeyHash, queueId: QueueId): Promise<RetrieveForProcessingSuccess | null> {
    const query = this.state.queryDef[queryKeyHash];
    const activeKeys = this.queueArray(this.state.active) as QueryKeyHash[];

    if (
      !query ||
      query.queueId !== queueId ||
      this.state.toProcess[queryKeyHash]?.queueId !== queueId ||
      this.state.active[queryKeyHash] ||
      activeKeys.length >= this.concurrency
    ) {
      return null;
    }

    this.state.active[queryKeyHash] = { key: queryKeyHash, order: Number(queueId), queueId };
    delete this.state.toProcess[queryKeyHash];

    this.state.heartBeat[queryKeyHash] = { key: queryKeyHash, order: new Date().getTime(), queueId };

    return {
      active: this.queueArray(this.state.active) as QueryKeyHash[],
      queueSize: Object.keys(this.state.toProcess).length,
      def: query,
    };
  }

  public async optimisticQueryUpdate(queryKeyHash: QueryKeyHash, toUpdate: any, queueId: QueueId): Promise<boolean> {
    if (this.state.active[queryKeyHash]?.queueId !== queueId || !this.state.queryDef[queryKeyHash]) {
      return false;
    }

    this.state.queryDef[queryKeyHash] = { ...this.state.queryDef[queryKeyHash], ...toUpdate };
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
