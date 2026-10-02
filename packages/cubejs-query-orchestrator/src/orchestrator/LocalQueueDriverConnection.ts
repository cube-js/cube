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
  /**
   * Resolved by the ack, every waiter of the run awaits the same promise. Goes away with the
   * item when it is removed without an ack.
   */
  result: Promise<any>;
  resolveResult: (value: any) => void;
}

/**
 * Cube Store's QueueResult lifetime. A result is written to the query cache only by a caller
 * that reads it from the queue, so it has to outlive the polling interval of any client.
 */
const RESULT_TTL_MS = 5 * 60 * 1000;

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
 * Results indexed by run id and by key, the indexes are only ever updated together.
 */
export class LocalQueueResults {
  /**
   * Insertion order is ack order, so the expired results are always at the front.
   */
  protected readonly unread: Map<number, LocalQueueResult> = new Map();

  /**
   * Insertion order is read order, and a read sets the same lifetime for all of them, so the
   * expired results are always at the front here too.
   */
  protected readonly consumed: Map<number, LocalQueueResult> = new Map();

  /**
   * The last acknowledged run of every key.
   */
  protected readonly byKey: Map<QueryKeyHash, LocalQueueResult> = new Map();

  public getById(id: number, now: number): LocalQueueResult | null {
    const result = this.unread.get(id) || this.consumed.get(id);

    return result && result.expire >= now ? result : null;
  }

  public getByKey(key: QueryKeyHash, now: number): LocalQueueResult | null {
    const result = this.byKey.get(key);

    return result && result.expire >= now ? result : null;
  }

  public add(result: LocalQueueResult): void {
    this.unread.set(result.id, result);
    this.byKey.set(result.key, result);
  }

  public consume(result: LocalQueueResult, expire: number): void {
    if (result.deleted) {
      return;
    }

    result.deleted = true;
    result.expire = Math.min(result.expire, expire);
    this.unread.delete(result.id);
    this.consumed.set(result.id, result);
  }

  public removeExpired(now: number): void {
    this.removeExpiredFrom(this.unread, now);
    this.removeExpiredFrom(this.consumed, now);
  }

  protected removeExpiredFrom(results: Map<number, LocalQueueResult>, now: number): void {
    for (const result of results.values()) {
      if (result.expire >= now) {
        return;
      }

      results.delete(result.id);
      if (this.byKey.get(result.key) === result) {
        this.byKey.delete(result.key);
      }
    }
  }
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
  public readonly items: LocalQueueItems = new LocalQueueItems();

  public readonly results: LocalQueueResults = new LocalQueueResults();
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
   * Cube Store marks a result ready-to-delete only for a waiter blocked at the ack, here every
   * read does it, so a key lookup of a later request never gets a run that was already read.
   * Past the first read only the joiners of the same executeInQueue call still ask, by id and
   * within milliseconds, so the result no longer has to live for RESULT_TTL_MS.
   */
  protected consume(result: LocalQueueResult, now: number): any {
    this.state.results.consume(result, now + this.continueWaitTimeout * 1000);

    return result.value;
  }

  protected async waitForResult(item: LocalQueueItem): Promise<any> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timeout = new Promise((resolve) => {
      timer = setTimeout(() => resolve(null), this.continueWaitTimeout * 1000);
    });

    try {
      const value = await Promise.race([item.result, timeout]);

      const now = Date.now();
      const result = value && this.state.results.getById(item.id, now);
      if (result) {
        this.consume(result, now);
      }

      return value;
    } finally {
      clearTimeout(timer);
    }
  }

  /**
   * By id the result is served to every caller until it expires. Without an id it falls back to
   * the key, which, like Cube Store's queue v1, serves the result only once.
   */
  public async getResultBlocking(queryKeyHash: QueryKeyHash, queueId?: QueueId | null): Promise<any> {
    const now = Date.now();
    this.state.results.removeExpired(now);

    if (queueId) {
      const result = this.state.results.getById(Number(queueId), now);
      if (result) {
        return result.key === queryKeyHash ? this.consume(result, now) : null;
      }
    } else {
      const result = this.state.results.getByKey(queryKeyHash, now);
      if (result && !result.deleted) {
        return this.consume(result, now);
      }
    }

    // With neither an item nor a result there is nothing that could ever resolve, so don't
    // make the caller wait out the timeout
    const item = this.resolveItem(queryKeyHash, queueId);
    if (!item) {
      return null;
    }

    return this.waitForResult(item);
  }

  /**
   * Cube Store without CUBEJS_QUEUE_EXTERNAL_ID: a result is served by key only once.
   */
  public async getResult(queryKey: QueryKey, _externalId?: string): Promise<any> {
    const now = Date.now();
    this.state.results.removeExpired(now);

    const result = this.state.results.getByKey(this.redisHash(queryKey), now);
    if (!result || result.deleted) {
      return null;
    }

    return this.consume(result, now);
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

    let resolveResult: ((value: any) => void) | undefined;
    const result = new Promise((resolve) => {
      resolveResult = resolve;
    });

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
      result,
      resolveResult: resolveResult!,
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
    this.state.results.add({
      id: item.id,
      key: item.key,
      value: executionResult,
      deleted: false,
      expire: now + RESULT_TTL_MS,
    });
    item.resolveResult(executionResult);

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
