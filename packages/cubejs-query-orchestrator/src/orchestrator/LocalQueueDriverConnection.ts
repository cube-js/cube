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
  queryKeyHash: QueryKeyHash;
  queueId: QueueId;
  promise: Promise<any>;
  resolve: (value: any) => void;
  resolved: boolean;
  // Set once any reader got the result, `getResult` no longer serves it after that
  consumed: boolean;
  // Set on the ack, a pending result lives until its run is acknowledged or removed
  expireAt?: number;
}

export class LocalQueueDriverConnectionState {
  // The two indexes of the Cube Store queue results. By `resultId`: every run of a query key has
  // its own result, pending ones included, for `getResultBlocking`.
  public resultsById: Record<string, QueueResult> = {};

  // By query key hash: the last acknowledged result of the key, for `getResult`
  public resultsByPath: Record<QueryKeyHash, QueueResult> = {};

  public cleanupTimer: ReturnType<typeof setInterval> | null = null;

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

  protected resultId(queryKeyHash: QueryKeyHash, queueId: QueueId): string {
    // A queue id alone isn't unique, every QueryQueue sharing this state counts from 1
    return `${this.resultListKey(queryKeyHash)}_${queueId}`;
  }

  protected getOrCreateResult(queryKeyHash: QueryKeyHash, queueId: QueueId): QueueResult {
    const id = this.resultId(queryKeyHash, queueId);
    if (!this.state.resultsById[id]) {
      let resolve: ((value: any) => void) | undefined;
      const promise = new Promise(r => {
        resolve = r;
      });
      this.state.resultsById[id] = { queryKeyHash, queueId, promise, resolve: resolve!, resolved: false, consumed: false };
    }

    return this.state.resultsById[id];
  }

  protected removeResult(id: string): void {
    const result = this.state.resultsById[id];
    if (!result) {
      return;
    }

    delete this.state.resultsById[id];
    if (this.state.resultsByPath[result.queryKeyHash] === result) {
      delete this.state.resultsByPath[result.queryKeyHash];
    }
  }

  public async getResultBlocking(queryKeyHash: QueryKeyHash, queueId: QueueId): Promise<any> {
    let result = this.state.resultsById[this.resultId(queryKeyHash, queueId)];
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
    const result = this.state.resultsByPath[queryKeyHash];
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
      this.removeResult(this.resultId(key, options.queueId));
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
    if (query && !this.state.resultsById[this.resultId(queryKeyHash, query.queueId)]?.resolved) {
      this.removeResult(this.resultId(queryKeyHash, query.queueId));
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

    delete this.state.active[queryKeyHash];
    delete this.state.heartBeat[queryKeyHash];
    delete this.state.toProcess[queryKeyHash];
    delete this.state.recent[queryKeyHash];
    delete this.state.queryDef[queryKeyHash];

    result.resolved = true;
    // A waiter which saw the query in flight re-polls within `continueWaitTimeout`
    result.expireAt = new Date().getTime() + this.continueWaitTimeout * 1000;
    result.resolve(executionResult);
    this.state.resultsByPath[queryKeyHash] = result;

    this.scheduleCleanup();

    return true;
  }

  /**
   * The only timer of the state, it removes every expired result and stops once no result is left to expire.
   */
  protected scheduleCleanup(): void {
    if (this.state.cleanupTimer) {
      return;
    }

    this.state.cleanupTimer = setInterval(() => {
      const now = new Date().getTime();
      let expiresLater = false;

      for (const [id, result] of Object.entries(this.state.resultsById)) {
        if (result.expireAt !== undefined && result.expireAt <= now) {
          this.removeResult(id);
        } else if (result.expireAt !== undefined) {
          expiresLater = true;
        }
      }

      if (!expiresLater && this.state.cleanupTimer) {
        clearInterval(this.state.cleanupTimer);
        this.state.cleanupTimer = null;
      }
    }, this.continueWaitTimeout * 1000);
    this.state.cleanupTimer.unref();
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
