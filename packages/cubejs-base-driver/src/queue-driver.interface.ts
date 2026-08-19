export type QueryDef = any;
// Primary key of Queue item
export type QueueId = string | number | bigint;
export type QueryKey = (string | [string, any[]]) & {
  persistent?: true,
};
export type QueryKeyHash = string & { __type: 'QueryKeyHash' };

export type QueryKeysTuple = [keyHash: QueryKeyHash, queueId: QueueId | null /** Supported by new Cube Store and Memory */];
export type GetActiveAndToProcessResponse = [active: QueryKeysTuple[], toProcess: QueryKeysTuple[]];
export type AddToQueueResponse = [added: number, queueId: QueueId | null, queueSize: number, addedToQueueTime: number];
export type QueryStageStateResponse = [active: string[], toProcess: string[]] | [active: string[], toProcess: string[], defs: Record<string, QueryDef>];
/**
 * `added` is `1` when the queue item was moved from pending to active by this call, `0` otherwise.
 */
export type RetrieveForProcessingResponse = [
  added: number,
  // QueueId is required for Cube Store, other providers don't support it
  queueId: QueueId | null,
  active: QueryKeyHash[],
  pending: number,
  def: QueryDef | null,
];

export interface AddToQueueQuery {
  isJob: boolean,
  orphanedTimeout: unknown
}

export interface AddToQueueOptions {
  // It's an ugly workaround for skip queue tasks
  queueId?: QueueId,
  stageQueryKey?: any,
  requestId: string,
  spanId?: string,
  orphanedTimeout?: number,
  externalId?: string,
}

export interface QueueDriverOptions {
  redisQueuePrefix: string,
  concurrency: number,
  continueWaitTimeout: number,
  orphanedTimeout: number,
  heartBeatTimeout: number,
  processUid?: string;
}

export interface QueueDriverConnectionInterface {
  redisHash(queryKey: QueryKey): QueryKeyHash;
  getResultBlocking(queryKey: QueryKeyHash, queueId: QueueId): Promise<unknown>;
  getResult(queryKey: QueryKey, externalId?: string): Promise<any>;
  /**
   * Adds specified by the queryKey query to the queue, returns tuple
   * with the operation result.
   *
   * @param queryKey
   * @param queryHandler Our queue allows using different handlers. For example, query, cvsQuery, etc.
   * @param query
   * @param priority
   * @param options The per item orphaned deadline comes from options.orphanedTimeout, in seconds
   */
  addToQueue(queryKey: QueryKey, queryHandler: string, query: AddToQueueQuery, priority: number, options: AddToQueueOptions): Promise<AddToQueueResponse>;
  // Return query keys which was sorted by priority and time
  getToProcessQueries(): Promise<QueryKeysTuple[]>;
  getActiveQueries(): Promise<QueryKeysTuple[]>;
  getQueryDef(hash: QueryKeyHash, queueId: QueueId | null): Promise<QueryDef | null>;
  // Queries which was added to queue, but was not processed and not needed
  getOrphanedQueries(): Promise<QueryKeysTuple[]>;
  // Queries which was not completed with old heartbeat
  getStalledQueries(): Promise<QueryKeysTuple[]>;
  getQueryStageState(onlyKeys: boolean): Promise<QueryStageStateResponse>;
  updateHeartBeat(hash: QueryKeyHash, queueId: QueueId | null): Promise<void>;
  // Atomically moves a pending queue item to active, which is what stops multiple nodes
  // from processing the same query. Returns `added: 0` when the item is missing, already
  // active, or the queue is at its concurrency limit - in all of those cases nothing is
  // mutated, so there is nothing for the caller to roll back.
  retrieveForProcessing(hash: QueryKeyHash): Promise<RetrieveForProcessingResponse>;
  optimisticQueryUpdate(hash: QueryKeyHash, toUpdate: unknown, queueId: QueueId | null): Promise<boolean>;
  cancelQuery(queryKey: QueryKey, queueId: QueueId | null): Promise<QueryDef | null>;
  getQueryAndRemove(hash: QueryKeyHash, queueId: QueueId | null): Promise<[QueryDef]>;
  // Returns false when the queue item is gone (cancelled or orphaned while it was executing),
  // which means the result was dropped.
  setResultAndRemoveQuery(hash: QueryKeyHash, executionResult: any, queueId: QueueId | null): Promise<unknown>;
  release(): void;
  getQueriesToCancel(): Promise<QueryKeysTuple[]>
  // @deprecated
  getActiveAndToProcess(): Promise<GetActiveAndToProcessResponse>;
}

export interface QueueDriverInterface {
  redisHash(queryKey: QueryKey): QueryKeyHash;
  createConnection(): Promise<QueueDriverConnectionInterface>;
  release(connection: QueueDriverConnectionInterface): void;
}
