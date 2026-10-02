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

/**
 * A queue item goes `Pending -> Active -> Deleted`. There is no terminal state: `Active` is the
 * only lock there is, it's set atomically by retrieveForProcessing and released by an ack or a
 * cancel, both of which delete the item outright.
 */
export enum LocalQueueItemStatus {
  Pending = 'pending',
  Active = 'active',
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

export interface LocalQueueItem {
  /**
   * Starts at 1, never 0: callers fall back to a key lookup on a falsy queueId.
   */
  id: number;
  key: QueryKeyHash;
  status: LocalQueueItemStatus;
  priority: number;
  created: number;
  heartbeat: number | null;
  /**
   * Absolute deadline in ms, not a duration. Pushed forward by every addToQueue of the same
   * key, so a query somebody is still waiting for doesn't get orphaned behind a long backlog.
   */
  orphaned: number;
  payload: QueryDefObject;
  /**
   * Written only by optimisticQueryUpdate, kept out of the payload so that a concurrent
   * update cannot mutate the def other connections are holding.
   */
  extra: Record<string, any> | null;
}

export interface PromiseWithResolve<T = any> extends Promise<T> {
  resolve?: (value: T) => void;
  resolved?: boolean;
}

/**
 * Queue items indexed by key and by id, both indexes are only ever updated together.
 */
export class LocalQueueItems {
  /**
   * A Map because insertion order matches id order, which is the order active items are
   * reported in. A plain object would reorder them.
   */
  protected readonly byKey: Map<QueryKeyHash, LocalQueueItem> = new Map();

  protected readonly byId: Map<number, LocalQueueItem> = new Map();

  protected idSequence: number = 0;

  public nextId(): number {
    this.idSequence += 1;

    return this.idSequence;
  }

  public getByKey(key: QueryKeyHash): LocalQueueItem | null {
    return this.byKey.get(key) || null;
  }

  public getById(id: number): LocalQueueItem | null {
    return this.byId.get(id) || null;
  }

  public has(key: QueryKeyHash): boolean {
    return this.byKey.has(key);
  }

  public add(item: LocalQueueItem): void {
    this.byKey.set(item.key, item);
    this.byId.set(item.id, item);
  }

  public remove(item: LocalQueueItem): void {
    this.byKey.delete(item.key);
    this.byId.delete(item.id);
  }

  public values(): IterableIterator<LocalQueueItem> {
    return this.byKey.values();
  }
}

export class LocalQueueDriverConnectionState {
  public resultPromises: Record<string, PromiseWithResolve> = {};

  public items: LocalQueueItems = new LocalQueueItems();
}

export class LocalQueueDriverConnection implements QueueDriverConnectionInterface {
  private redisQueuePrefix: string;

  private continueWaitTimeout: number;

  private orphanedTimeout: number;

  private heartBeatTimeout: number;

  private concurrency: number;

  private driver: LocalQueueDriver;

  private state: LocalQueueDriverConnectionState;

  public constructor(driver: LocalQueueDriver, state: LocalQueueDriverConnectionState, options: QueueDriverOptions) {
    this.redisQueuePrefix = options.redisQueuePrefix;
    this.continueWaitTimeout = options.continueWaitTimeout;
    this.orphanedTimeout = options.orphanedTimeout;
    this.heartBeatTimeout = options.heartBeatTimeout;
    this.concurrency = options.concurrency;
    this.driver = driver;
    this.state = state;
  }

  /**
   * There is deliberately no key fallback once an id is supplied: a stale id has to miss,
   * otherwise an in-flight query would never notice that it was cancelled and re-added
   * under a new id.
   */
  protected resolveItem(queryKeyHash: QueryKeyHash, queueId?: QueueId | null): LocalQueueItem | null {
    if (queueId) {
      const item = this.state.items.getById(Number(queueId));
      // An id never resolves to an item under a different key
      return item?.key === queryKeyHash ? item : null;
    }

    return this.state.items.getByKey(queryKeyHash);
  }

  protected mergeDef(item: LocalQueueItem): QueryDef {
    if (item.extra) {
      return { ...item.payload, ...item.extra };
    }

    return { ...item.payload };
  }

  /**
   * FIFO within a priority band. The id breaks ties between items added in the same millisecond.
   */
  protected sortedItems(): LocalQueueItem[] {
    return Array.from(this.state.items.values()).sort((a, b) => {
      if (a.priority !== b.priority) {
        return b.priority - a.priority;
      }

      if (a.created !== b.created) {
        return a.created - b.created;
      }

      return a.id - b.id;
    });
  }

  protected pendingItems(): LocalQueueItem[] {
    return this.sortedItems().filter((item) => item.status === LocalQueueItemStatus.Pending);
  }

  /**
   * Deliberately not priority sorted, unlike pendingItems: active items are reported in id order.
   */
  protected activeItems(): LocalQueueItem[] {
    return Array.from(this.state.items.values()).filter((item) => item.status === LocalQueueItemStatus.Active);
  }

  protected countPending(): number {
    let count = 0;

    for (const item of this.state.items.values()) {
      if (item.status === LocalQueueItemStatus.Pending) {
        count += 1;
      }
    }

    return count;
  }

  protected asTuple(items: LocalQueueItem[]): QueryKeysTuple[] {
    return items.map((item): QueryKeysTuple => [item.key, item.id]);
  }

  public async getQueriesToCancel(): Promise<QueryKeysTuple[]> {
    const now = Date.now();

    return this.asTuple(
      Array.from(this.state.items.values()).filter((item) => (
        item.status === LocalQueueItemStatus.Pending
          ? this.isOrphaned(item, now)
          : this.isStalled(item, now)
      ))
    );
  }

  public async getActiveAndToProcess(): Promise<GetActiveAndToProcessResponse> {
    return [
      this.asTuple(this.activeItems()),
      this.asTuple(this.pendingItems()),
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
    // With neither an item nor a result there is nothing that could ever resolve, so don't
    // make the caller wait out the timeout
    if (!this.state.items.has(queryKeyHash) && !this.state.resultPromises[resultListKey]) {
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

  public async addToQueue(
    queryKey: QueryKey,
    queryHandler: string,
    query: AddToQueueQuery,
    priority: QueuePriority,
    options: AddToQueueOptions
  ): Promise<AddToQueueResponse> {
    const key = this.redisHash(queryKey);
    const pending = this.countPending();

    // Returning the existing id rather than a fresh one is what makes the caller wait on the
    // query that is already queued instead of on an id that will never be acked.
    const existing = this.state.items.getByKey(key);
    if (existing) {
      existing.orphaned = this.orphanedDeadline(Date.now(), options);

      return [
        0,
        existing.id,
        pending,
        existing.payload.addedToQueueTime,
        null,
      ];
    }

    const created = Date.now();
    const id = this.state.items.nextId();

    const item: LocalQueueItem = {
      id,
      key,
      status: LocalQueueItemStatus.Pending,
      priority,
      created,
      heartbeat: null,
      orphaned: this.orphanedDeadline(created, options),
      payload: {
        queueId: id,
        queryHandler,
        query,
        queryKey,
        stageQueryKey: options.stageQueryKey,
        priority,
        requestId: options.requestId,
        addedToQueueTime: created,
      },
      extra: null,
    };

    this.state.items.add(item);

    return [
      1,
      id,
      pending + 1,
      created,
      // There is no round-trip to save in memory, the item is left for reconcile to pick up
      null,
    ];
  }

  public async getToProcessQueries(): Promise<QueryKeysTuple[]> {
    return this.asTuple(this.pendingItems());
  }

  public async getActiveQueries(): Promise<QueryKeysTuple[]> {
    return this.asTuple(this.activeItems());
  }

  public async getQueryAndRemove(queryKeyHash: QueryKeyHash, queueId?: QueueId | null): Promise<[QueryDef]> {
    const item = this.resolveItem(queryKeyHash, queueId);
    if (!item) {
      return [null];
    }

    this.state.items.remove(item);

    return [this.mergeDef(item)];
  }

  public async cancelQuery(queryKey: QueryKey, queueId?: QueueId | null): Promise<QueryDef | null> {
    const [query] = await this.getQueryAndRemove(this.redisHash(queryKey), queueId);
    return query;
  }

  public async setResultAndRemoveQuery(queryKeyHash: QueryKeyHash, executionResult: any, queueId: QueueId): Promise<boolean> {
    const item = this.resolveItem(queryKeyHash, queueId);
    // The item was cancelled or orphaned while it was executing, so the result is dropped
    if (item?.status !== LocalQueueItemStatus.Active) {
      return false;
    }

    this.state.items.remove(item);

    const promise = this.getResultPromise(this.resultListKey(item.key));

    promise.resolved = true;
    if (promise.resolve) {
      promise.resolve(executionResult);
    }

    return true;
  }

  /**
   * The orphaned timeout only ever applies to pending items, never to ones being executed.
   */
  protected isOrphaned(item: LocalQueueItem, now: number): boolean {
    return item.orphaned < now;
  }

  /**
   * options.orphanedTimeout is in seconds and overrides the driver level one.
   */
  protected orphanedDeadline(now: number, options: AddToQueueOptions): number {
    return now + (options.orphanedTimeout ?? this.orphanedTimeout) * 1000;
  }

  /**
   * The heartbeat timeout only ever applies to active items. retrieveForProcessing always sets
   * a heartbeat, so `created` is only a fallback.
   */
  protected isStalled(item: LocalQueueItem, now: number): boolean {
    return now - (item.heartbeat ?? item.created) > this.heartBeatTimeout * 1000;
  }

  public async getOrphanedQueries(): Promise<QueryKeysTuple[]> {
    const now = Date.now();

    return this.asTuple(this.pendingItems().filter((item) => this.isOrphaned(item, now)));
  }

  public async getStalledQueries(): Promise<QueryKeysTuple[]> {
    const now = Date.now();

    return this.asTuple(this.activeItems().filter((item) => this.isStalled(item, now)));
  }

  public async getQueryStageState(onlyKeys: boolean): Promise<QueryStageStateResponse> {
    const active: string[] = [];
    const toProcess: string[] = [];
    const defs: Record<string, QueryDef> = {};

    for (const item of this.sortedItems()) {
      if (!onlyKeys) {
        defs[item.key] = this.mergeDef(item);
      }

      if (item.status === LocalQueueItemStatus.Active) {
        active.push(item.key);
      } else {
        toProcess.push(item.key);
      }
    }

    return [active, toProcess, defs];
  }

  public async getQueryDef(queryKeyHash: QueryKeyHash, queueId?: QueueId | null): Promise<QueryDef | null> {
    const item = this.resolveItem(queryKeyHash, queueId);

    return item ? this.mergeDef(item) : null;
  }

  public async updateHeartBeat(queryKeyHash: QueryKeyHash, queueId?: QueueId | null): Promise<void> {
    const item = this.resolveItem(queryKeyHash, queueId);
    if (item) {
      item.heartbeat = Date.now();
    }
  }

  public async retrieveForProcessing(queryKeyHash: QueryKeyHash, queueId: QueueId): Promise<RetrieveForProcessingSuccess | null> {
    // Keep this method free of `await`: activation is only atomic while the whole
    // read-modify-write below stays a single synchronous block.
    const active = this.activeItems().map((item) => item.key);

    // Every rejection below happens before anything is mutated, so a caller that fails
    // here has nothing to roll back.
    if (active.length >= this.concurrency) {
      return null;
    }

    const item = this.resolveItem(queryKeyHash, queueId);
    if (item?.status !== LocalQueueItemStatus.Pending) {
      return null;
    }

    item.status = LocalQueueItemStatus.Active;
    // Without a heartbeat the creation time would be used for stalled filtering
    item.heartbeat = Date.now();
    active.push(item.key);

    return {
      active,
      queueSize: this.countPending(),
      def: this.mergeDef(item),
    };
  }

  public async optimisticQueryUpdate(queryKeyHash: QueryKeyHash, toUpdate: any, queueId: QueueId): Promise<boolean> {
    const item = this.resolveItem(queryKeyHash, queueId);
    if (item?.status !== LocalQueueItemStatus.Active) {
      return false;
    }

    item.extra = { ...(item.extra ?? {}), ...toUpdate };

    return true;
  }

  public release(): void {
    // nothing to release
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
