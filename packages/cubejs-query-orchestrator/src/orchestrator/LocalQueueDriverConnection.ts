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

export interface QueueResult {
  promise: Promise<any>;
  resolve: (value: any) => void;
  resolved: boolean;
  // Set once any reader got the result, `getResult` no longer serves it after that
  consumed: boolean;
}

export class LocalQueueDriverConnectionState {
  // By `resultKey`: every run of a query key has its own result, like the queue items of Cube Store
  public results: Record<string, QueueResult> = {};

  // The run which acknowledged the last result of a query key, for the lookup by key of `getResult`
  public lastResultQueueId: Record<QueryKeyHash, QueueId> = {};

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

  protected resultKey(queryKeyHash: QueryKeyHash, queueId: QueueId): string {
    // A queue id alone isn't unique, every QueryQueue sharing this state counts from 1
    return `${this.resultListKey(queryKeyHash)}_${queueId}`;
  }

  protected getOrCreateResult(queryKeyHash: QueryKeyHash, queueId: QueueId): QueueResult {
    const key = this.resultKey(queryKeyHash, queueId);
    if (!this.state.results[key]) {
      let resolve: ((value: any) => void) | undefined;
      const promise = new Promise(r => {
        resolve = r;
      });
      this.state.results[key] = { promise, resolve: resolve!, resolved: false, consumed: false };
    }

    return this.state.results[key];
  }

  public async getResultBlocking(queryKeyHash: QueryKeyHash, queueId: QueueId): Promise<any> {
    let result = this.state.results[this.resultKey(queryKeyHash, queueId)];
    if (!result) {
      if (this.state.queryDef[queryKeyHash]?.queueId !== queueId) {
        return null;
      }

      result = this.getOrCreateResult(queryKeyHash, queueId);
    }

    const timeoutPromise = (timeout: number) => new Promise((resolve) => setTimeout(() => resolve(null), timeout));

    const res = await Promise.race([
      result.promise,
      timeoutPromise(this.continueWaitTimeout * 1000),
    ]);

    // Not removed, the other waiters of this run can get here after it's done
    if (res) {
      result.consumed = true;
    }
    return res;
  }

  public async getResult(queryKey: QueryKey, _externalId?: string): Promise<any> {
    const queryKeyHash = this.redisHash(queryKey);
    const queueId = this.state.lastResultQueueId[queryKeyHash];
    const result = queueId !== undefined ? this.state.results[this.resultKey(queryKeyHash, queueId)] : undefined;
    if (!result?.resolved || result.consumed) {
      return null;
    }

    result.consumed = true;
    return result.promise;
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
      // A run of another queue could have used the same id for this key
      delete this.state.results[this.resultKey(key, options.queueId)];
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

    const { queueId, addedToQueueTime } = this.state.queryDef[key];

    // The run which is already queued is the one to wait for, as Cube Store answers
    return [
      added,
      queueId,
      Object.keys(this.state.toProcess).length,
      addedToQueueTime,
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

    // The run won't be acknowledged, its waiters still time out on the promise they hold
    if (query && !this.state.results[this.resultKey(queryKeyHash, query.queueId)]?.resolved) {
      delete this.state.results[this.resultKey(queryKeyHash, query.queueId)];
    }

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

    const result = this.getOrCreateResult(queryKeyHash, queueId);
    const key = this.resultKey(queryKeyHash, queueId);

    delete this.state.active[queryKeyHash];
    delete this.state.heartBeat[queryKeyHash];
    delete this.state.toProcess[queryKeyHash];
    delete this.state.recent[queryKeyHash];
    delete this.state.queryDef[queryKeyHash];

    result.resolved = true;
    result.resolve(executionResult);
    this.state.lastResultQueueId[queryKeyHash] = queueId;

    // A waiter which saw the query in flight re-polls within `continueWaitTimeout`
    setTimeout(() => {
      if (this.state.results[key] === result) {
        delete this.state.results[key];
      }
      if (this.state.lastResultQueueId[queryKeyHash] === queueId) {
        delete this.state.lastResultQueueId[queryKeyHash];
      }
    }, this.continueWaitTimeout * 1000).unref();

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
