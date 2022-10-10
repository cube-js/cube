export interface LocalQueueDriverConnectionInterface {
  getResultBlocking(queryKey: string): Promise<unknown>;
  getResult(queryKey: string): Promise<unknown>;
  addToQueue(queryKey: string): Promise<unknown>;
  getToProcessQueries(): Promise<unknown>;
  getActiveQueries(): Promise<unknown>;
  getOrphanedQueries(): Promise<unknown>;
  getStalledQueries(): Promise<unknown>;
  getQueryStageState(onlyKeys: any): Promise<unknown>;
  updateHeartBeat(queryKey: string): Promise<void>;
  getNextProcessingId(): Promise<string>;
  retrieveForProcessing(queryKey: string, processingId: string): Promise<unknown>;
  freeProcessingLock(queryKe: string, processingId: string, activated: unknown): Promise<unknown>;
  optimisticQueryUpdate(queryKey, toUpdate, processingId): Promise<unknown>;
  cancelQuery(queryKey: string): Promise<unknown>;
  setResultAndRemoveQuery(queryKey: string, executionResult: any, processingId: any): Promise<unknown>;
  release(): Promise<void>;
}

export interface QueueDriverInterface {
  createConnection(): Promise<LocalQueueDriverConnectionInterface>;
  release(connection: LocalQueueDriverConnectionInterface): Promise<void>;
}
