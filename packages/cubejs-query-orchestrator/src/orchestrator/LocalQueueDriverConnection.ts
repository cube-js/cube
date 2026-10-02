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
 * `Pending -> Active`, then removed outright by an ack or a cancel. `Active` is the only lock
 * there is: retrieveForProcessing sets it atomically.
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

/**
 * Mirrors Cube Store's QueueResult: written by the ack under the id of the run it belongs to.
 */
export interface LocalQueueResult {
  id: number;
  key: QueryKeyHash;
  value: any;
  /**
   * Set once anybody took the result, after that only a lookup by id still serves it.
   * Cube Store's `deleted` flag.
   */
  deleted: boolean;
  /**
   * Absolute deadline in ms
   */
  expire: number;
}

/**
 * Results indexed by run id and by key, both indexes are only ever updated together.
 */
export class LocalQueueResults {
  /**
   * Insertion order is ack order, so the expired results are always at the front.
   */
  protected readonly byId: Map<number, LocalQueueResult> = new Map();

  /**
   * The last acknowledged run of every key.
   */
  protected readonly byKey: Map<QueryKeyHash, LocalQueueResult> = new Map();

  public getById(id: number, now: number): LocalQueueResult | null {
    const result = this.byId.get(id);

    return result && result.expire >= now ? result : null;
  }

  public getByKey(key: QueryKeyHash, now: number): LocalQueueResult | null {
    const result = this.byKey.get(key);

    return result && result.expire >= now ? result : null;
  }

  public add(result: LocalQueueResult): void {
    this.byId.set(result.id, result);
    this.byKey.set(result.key, result);
  }

  public removeExpired(now: number): void {
    for (const result of this.byId.values()) {
      if (result.expire >= now) {
        return;
      }

      this.byId.delete(result.id);
      if (this.byKey.get(result.key) === result) {
        this.byKey.delete(result.key);
      }
    }
  }
}

type LocalQueueResultWaiter = (value: any) => void;

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
  public readonly items: LocalQueueItems = new LocalQueueItems();

  public readonly results: LocalQueueResults = new LocalQueueResults();

  /**
   * getResultBlocking calls parked on a run id until its ack, Cube Store's ack listener.
   */
  public readonly waiters: Map<number, Set<LocalQueueResultWaiter>> = new Map();
}

export class LocalQueueDriverConnection implements QueueDriverConnectionInterface {
  private readonly continueWaitTimeout: number;

  private readonly orphanedTimeout: number;

  private readonly heartBeatTimeout: number;

  private readonly concurrency: number;

  private readonly driver: LocalQueueDriver;

  private readonly state: LocalQueueDriverConnectionState;

  public constructor(driver: LocalQueueDriver, state: LocalQueueDriverConnectionState, options: QueueDriverOptions) {
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
      Array.from(this.state.items.values()).filter((item) => this.isOrphaned(item, now) || this.isStalled(item, now))
    );
  }

  public async getActiveAndToProcess(): Promise<GetActiveAndToProcessResponse> {
    return [
      this.asTuple(this.activeItems()),
      this.asTuple(this.pendingItems()),
    ];
  }

  /**
   * A result outlives its first reader: other waiters of the same run, e.g. a joiner that deduped
   * onto it in addToQueue, read it after the ack too. They come back within one
   * continueWaitTimeout of polling, so that's how long it is kept.
   */
  protected resultExpire(now: number): number {
    return now + this.continueWaitTimeout * 1000;
  }

  protected waitForResult(id: number): Promise<any> {
    return new Promise((resolve) => {
      let waiters = this.state.waiters.get(id);
      if (!waiters) {
        waiters = new Set();
        this.state.waiters.set(id, waiters);
      }

      let timeout: ReturnType<typeof setTimeout> | undefined;

      const waiter: LocalQueueResultWaiter = (value) => {
        clearTimeout(timeout);
        resolve(value);
      };

      timeout = setTimeout(() => {
        waiters.delete(waiter);
        if (!waiters.size && this.state.waiters.get(id) === waiters) {
          this.state.waiters.delete(id);
        }

        resolve(null);
      }, this.continueWaitTimeout * 1000);

      waiters.add(waiter);
    });
  }

  /**
   * By id the result is served to every caller until it expires. Without an id it falls back to
   * the key, which, like Cube Store's queue v1, serves the result only once.
   */
  public async getResultBlocking(queryKeyHash: QueryKeyHash, queueId?: QueueId | null): Promise<any> {
    const now = Date.now();

    if (queueId) {
      const result = this.state.results.getById(Number(queueId), now);
      if (result) {
        if (result.key !== queryKeyHash) {
          return null;
        }

        // Unlike Cube Store, which leaves it to the expiry here: a key lookup of a later request
        // must not be answered with the result of a run that was already consumed
        result.deleted = true;
        return result.value;
      }
    } else {
      const result = this.state.results.getByKey(queryKeyHash, now);
      if (result && !result.deleted) {
        result.deleted = true;
        return result.value;
      }
    }

    // With neither an item nor a result there is nothing that could ever resolve, so don't
    // make the caller wait out the timeout
    const item = this.resolveItem(queryKeyHash, queueId);
    if (!item) {
      return null;
    }

    return this.waitForResult(item.id);
  }

  /**
   * Cube Store without CUBEJS_QUEUE_EXTERNAL_ID: a result is served by key only once.
   */
  public async getResult(queryKey: QueryKey, _externalId?: string): Promise<any> {
    const result = this.state.results.getByKey(this.redisHash(queryKey), Date.now());
    if (!result || result.deleted) {
      return null;
    }

    result.deleted = true;
    return result.value;
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

    // A dedupe has to hand back the existing id: lookups by id have no key fallback, so a
    // fresh one would miss the queued item (e.g. getQueryDef right after a dedupe in QueryQueue).
    const existing = this.state.items.getByKey(key);
    if (existing) {
      existing.orphaned = Math.max(existing.orphaned, this.orphanedDeadline(Date.now(), options));

      return [
        0,
        existing.id,
        pending,
        existing.payload.addedToQueueTime,
        null,
      ];
    }

    const created = Date.now();
    this.state.results.removeExpired(created);

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

    const now = Date.now();
    this.state.results.removeExpired(now);

    const result: LocalQueueResult = {
      id: item.id,
      key: item.key,
      value: executionResult,
      deleted: false,
      expire: this.resultExpire(now),
    };
    this.state.results.add(result);

    const waiters = this.state.waiters.get(item.id);
    if (waiters) {
      this.state.waiters.delete(item.id);
      // Handed over to a waiter, so like in Cube Store a key lookup doesn't serve it again
      result.deleted = true;

      for (const waiter of waiters) {
        waiter(executionResult);
      }
    }

    return true;
  }

  protected isOrphaned(item: LocalQueueItem, now: number): boolean {
    return item.status === LocalQueueItemStatus.Pending && item.orphaned < now;
  }

  /**
   * options.orphanedTimeout is in seconds and overrides the driver level one.
   */
  protected orphanedDeadline(now: number, options: AddToQueueOptions): number {
    return now + (options.orphanedTimeout ?? this.orphanedTimeout) * 1000;
  }

  protected isStalled(item: LocalQueueItem, now: number): boolean {
    if (item.status !== LocalQueueItemStatus.Active) {
      return false;
    }

    if (item.heartbeat === null) {
      return false;
    }

    return now - item.heartbeat > this.heartBeatTimeout * 1000;
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
    const defs: Record<string, QueryDef> = {};

    if (!onlyKeys) {
      for (const item of this.state.items.values()) {
        defs[item.key] = this.mergeDef(item);
      }
    }

    return [
      this.activeItems().map((item) => item.key),
      this.pendingItems().map((item) => item.key),
      defs,
    ];
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

  public redisHash(queryKey: QueryKey): QueryKeyHash {
    return this.driver.redisHash(queryKey);
  }
}
