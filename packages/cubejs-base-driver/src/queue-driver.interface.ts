export type QueryDef = unknown;
export type QueryKey = (string | [string, any[]]) & {
  persistent?: true,
};

export type AddToQueueResponse = [added: number, _b: any, _c: any, queueSize: number, addedToQueueTime: number];
export type QueryStageStateResponse = [active: string[], toProcess: string[]] | [active: string[], toProcess: string[], defs: Record<string, QueryDef>];
export type RetrieveForProcessingResponse = [added: any, removed: any, active: string[], toProcess: any, def: QueryDef, lockAquired: boolean] | null;

export interface AddToQueueQuery {
  isJob: boolean,
  orphanedTimeout: unknown
}

export interface AddToQueueOptions {
  stageQueryKey: string,
  requestId: string
}

export interface QueueDriverOptions {
  redisQueuePrefix: string,
  concurrency: number,
  continueWaitTimeout: number,
  orphanedTimeout: number,
  heartBeatTimeout: number,
  getQueueEventsBus?: any,
}

export interface QueueDriverConnectionInterface {
  redisHash(queryKey: QueryKey): string;
  getResultBlocking(queryKey: QueryKey): Promise<unknown>;
  getResult(queryKey: QueryKey): Promise<any>;
  addToQueue(keyScore: number, queryKey: QueryKey, orphanedTime: any, queryHandler: any, query: AddToQueueQuery, priority: number, options: AddToQueueOptions): Promise<AddToQueueResponse>;
  // Return query keys which was sorted by priority and time
  getToProcessQueries(): Promise<string[]>;
  getActiveQueries(): Promise<string[]>;
  getQueryDef(queryKey: QueryKey): Promise<QueryDef | null>;
  // Queries which was added to queue, but was not processed and not needed
  getOrphanedQueries(): Promise<string[]>;
  // Queries which was not completed with old heartbeat
  getStalledQueries(): Promise<string[]>;
  getQueryStageState(onlyKeys: boolean): Promise<QueryStageStateResponse>;
  updateHeartBeat(queryKey: QueryKey): Promise<void>;
  getNextProcessingId(): Promise<string | number>;
  // Trying to acquire a lock for processing a queue item, this method can return null when
  // multiple nodes tries to process the same query
  retrieveForProcessing(queryKey: QueryKey, processingId: number | string): Promise<RetrieveForProcessingResponse>;
  freeProcessingLock(queryKey: QueryKey, processingId: string | number, activated: unknown): Promise<void>;
  optimisticQueryUpdate(queryKey: QueryKey, toUpdate, processingId): Promise<boolean>;
  cancelQuery(queryKey: QueryKey): Promise<QueryDef | null>;
  getQueryAndRemove(queryKey: QueryKey): Promise<[QueryDef]>;
  setResultAndRemoveQuery(queryKey: QueryKey, executionResult: any, processingId: any): Promise<unknown>;
  release(): void;
  //
  getQueriesToCancel(): Promise<string[]>
  getActiveAndToProcess(): Promise<[active: string[], toProcess: string[]]>;
}

export interface QueueDriverInterface {
  redisHash(queryKey: QueryKey): string;
  createConnection(): Promise<QueueDriverConnectionInterface>;
  release(connection: QueueDriverConnectionInterface): void;
}
