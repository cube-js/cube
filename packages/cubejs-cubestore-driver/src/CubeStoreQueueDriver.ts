import {
  QueueDriverInterface,
  QueueDriverConnectionInterface,
  QueryStageStateResponse,
  QueryDef,
  RetrieveForProcessingResponse,
  QueueDriverOptions,
  AddToQueueQuery,
  AddToQueueOptions,
  AddToQueueResponse,
  QueryKey,
  QueryKeyHash,
  ProcessingId,
  QueueId,
  GetActiveAndToProcessResponse,
  QueryKeysTuple,
} from '@cubejs-backend/base-driver';
import { defaultHasher, getProcessUid } from '@cubejs-backend/shared';

import { CubeStoreDriver } from './CubeStoreDriver';

function hashQueryKey(queryKey: QueryKey, processUid?: string): QueryKeyHash {
  processUid = processUid || getProcessUid();
  const hash = defaultHasher().update(JSON.stringify(queryKey)).digest('hex');

  if (typeof queryKey === 'object' && queryKey.persistent) {
    return `${hash}@${processUid}` as any;
  }

  return hash as any;
}

type CubeStoreListResponse = {
  id: unknown,
  // eslint-disable-next-line camelcase
  queue_id?: string
  status: string
};

class CubestoreQueueDriverConnection implements QueueDriverConnectionInterface {
  public constructor(
    protected readonly driver: CubeStoreDriver,
    protected readonly options: QueueDriverOptions,
  ) { }

  public redisHash(queryKey: QueryKey): QueryKeyHash {
    return hashQueryKey(queryKey, this.options.processUid);
  }

  protected prefixKey(queryKey: QueryKey): string {
    return `${this.options.redisQueuePrefix}:${queryKey}`;
  }

  public async addToQueue(
    keyScore: number,
    queryKey: QueryKey,
    orphanedTime: number,
    queryHandler: string,
    query: AddToQueueQuery,
    priority: number,
    options: AddToQueueOptions
  ): Promise<AddToQueueResponse> {
    const data = {
      queryHandler,
      query,
      queryKey,
      stageQueryKey: options.stageQueryKey,
      priority,
      requestId: options.requestId,
      addedToQueueTime: new Date().getTime()
    };

    const values: (string | number)[] = [
      priority,
    ];

    if (options.orphanedTimeout) {
      values.push(options.orphanedTimeout);
    }

    values.push(this.prefixKey(this.redisHash(queryKey)));
    values.push(JSON.stringify(data));

    const rows = await this.driver.query(`QUEUE ADD PRIORITY ?${options.orphanedTimeout ? ' ORPHANED ?' : ''} ? ?`, values);
    if (rows && rows.length) {
      return [
        rows[0].added === 'true' ? 1 : 0,
        rows[0].id ? parseInt(rows[0].id, 10) : null,
        parseInt(rows[0].pending, 10),
        data.addedToQueueTime
      ];
    }

    throw new Error('Empty response on QUEUE ADD');
  }

  public async getQueryAndRemove(hash: QueryKeyHash, queueId: QueueId | null): Promise<[QueryDef]> {
    return [await this.cancelQuery(hash, queueId)];
  }

  public async cancelQuery(hash: QueryKeyHash, queueId: QueueId | null): Promise<QueryDef | null> {
    const rows = await this.driver.query('QUEUE CANCEL ?', [
      // queryKeyHash as compatibility fallback
      queueId || this.prefixKey(hash),
    ]);
    if (rows && rows.length) {
      return this.decodeQueryDefFromRow(rows[0], 'cancelQuery');
    }

    return null;
  }

  public async freeProcessingLock(_hash: QueryKeyHash, _processingId: string, _activated: unknown): Promise<void> {
    // nothing to do
  }

  public async getActiveQueries(): Promise<QueryKeysTuple[]> {
    const rows = await this.driver.query<CubeStoreListResponse>('QUEUE ACTIVE ?', [
      this.options.redisQueuePrefix
    ]);
    return rows.map((row) => [
      row.id as QueryKeyHash,
      row.queue_id ? parseInt(row.queue_id, 10) : null,
    ]);
  }

  public async getToProcessQueries(): Promise<QueryKeysTuple[]> {
    const rows = await this.driver.query<CubeStoreListResponse>('QUEUE PENDING ?', [
      this.options.redisQueuePrefix
    ]);
    return rows.map((row) => [
      row.id as QueryKeyHash,
      row.queue_id ? parseInt(row.queue_id, 10) : null,
    ]);
  }

  public async getActiveAndToProcess(): Promise<GetActiveAndToProcessResponse> {
    return [
      // We don't return active queries, because it's useless
      // There is only one place where it's used, and it's QueryQueue.reconcileQueueImpl
      // Cube Store provides strict guarantees that queue item cannot be active & pending in the same time
      [],
      await this.getToProcessQueries()
    ];
  }

  public async getNextProcessingId(): Promise<number | string> {
    const rows = await this.driver.query('CACHE INCR ?', [
      `${this.options.redisQueuePrefix}:PROCESSING_COUNTER`
    ]);
    if (rows && rows.length) {
      return rows[0].value;
    }

    throw new Error('Unable to get next processing id');
  }

  public async getQueryStageState(onlyKeys: boolean): Promise<QueryStageStateResponse> {
    const rows = await this.driver.query<CubeStoreListResponse & { payload: string }>(`QUEUE LIST ${onlyKeys ? '?' : 'WITH_PAYLOAD ?'}`, [
      this.options.redisQueuePrefix
    ]);

    const defs: Record<string, QueryDef> = {};
    const toProcess: string[] = [];
    const active: string[] = [];

    for (const row of rows) {
      if (!onlyKeys) {
        defs[row.id as string] = this.decodeQueryDefFromRow(row, 'getQueryStageState');
      }

      if (row.status === 'pending') {
        toProcess.push(row.id as string);
      } else if (row.status === 'active') {
        active.push(row.id as string);
      }
    }

    return [active, toProcess, defs];
  }

  public async getResult(queryKey: QueryKey): Promise<unknown> {
    const rows = await this.driver.query('QUEUE RESULT ?', [
      this.prefixKey(this.redisHash(queryKey)),
    ]);
    if (rows && rows.length) {
      return this.decodeQueryDefFromRow(rows[0], 'getResult');
    }

    return null;
  }

  public async getStalledQueries(): Promise<QueryKeysTuple[]> {
    const rows = await this.driver.query<CubeStoreListResponse>('QUEUE STALLED ? ?', [
      this.options.heartBeatTimeout * 1000,
      this.options.redisQueuePrefix
    ]);
    return rows.map((row) => [
      row.id as QueryKeyHash,
      row.queue_id ? parseInt(row.queue_id, 10) : null,
    ]);
  }

  public async getOrphanedQueries(): Promise<QueryKeysTuple[]> {
    const rows = await this.driver.query<CubeStoreListResponse>('QUEUE ORPHANED ? ?', [
      this.options.orphanedTimeout * 1000,
      this.options.redisQueuePrefix
    ]);
    return rows.map((row) => [
      row.id as QueryKeyHash,
      row.queue_id ? parseInt(row.queue_id, 10) : null,
    ]);
  }

  public async getQueriesToCancel(): Promise<QueryKeysTuple[]> {
    const rows = await this.driver.query<CubeStoreListResponse>('QUEUE TO_CANCEL ? ? ?', [
      this.options.heartBeatTimeout * 1000,
      this.options.orphanedTimeout * 1000,
      this.options.redisQueuePrefix,
    ]);
    return rows.map((row) => [
      row.id as QueryKeyHash,
      row.queue_id ? parseInt(row.queue_id, 10) : null,
    ]);
  }

  protected decodeQueryDefFromRow(row: { payload: string, extra?: string | null }, method: string): QueryDef {
    if (!row.payload) {
      throw new Error(`Field payload is empty, incorrect response for ${method} method`);
    }

    const payload = JSON.parse(row.payload);

    if (row.extra) {
      return Object.assign(payload, JSON.parse(row.extra));
    }

    return payload;
  }

  public async getQueryDef(hash: QueryKeyHash, queueId: QueueId | null): Promise<QueryDef | null> {
    const rows = await this.driver.query('QUEUE GET ?', [
      queueId || this.prefixKey(hash),
    ]);
    if (rows && rows.length) {
      return this.decodeQueryDefFromRow(rows[0], 'getQueryDef');
    }

    return null;
  }

  public async optimisticQueryUpdate(hash: QueryKeyHash, toUpdate: unknown, _processingId: ProcessingId, queueId: QueueId): Promise<boolean> {
    await this.driver.query('QUEUE MERGE_EXTRA ? ?', [
      // queryKeyHash as compatibility fallback
      queueId || this.prefixKey(hash),
      JSON.stringify(toUpdate)
    ]);

    return true;
  }

  public release(): void {
    // nothing to release
  }

  public async retrieveForProcessing(hash: QueryKeyHash, _processingId: string): Promise<RetrieveForProcessingResponse> {
    const rows = await this.driver.query<{ id: string /* cube store convert int64 to string */, active: string | null, pending: string, payload: string, extra: string | null }>('QUEUE RETRIEVE EXTENDED CONCURRENCY ? ?', [
      this.options.concurrency,
      this.prefixKey(hash),
    ]);
    if (rows && rows.length) {
      const active = rows[0].active ? (rows[0].active).split(',') as unknown as QueryKeyHash[] : [];
      const pending = parseInt(rows[0].pending, 10);

      if (rows[0].payload) {
        const def = this.decodeQueryDefFromRow(rows[0], 'retrieveForProcessing');

        return [
          1,
          rows[0].id ? parseInt(rows[0].id, 10) : null,
          active,
          pending,
          def,
          true
        ];
      } else {
        return [
          0, null, active, pending, null, false
        ];
      }
    }

    return null;
  }

  public async getResultBlocking(hash: QueryKeyHash, queueId: QueueId): Promise<QueryDef | null> {
    const rows = await this.driver.query('QUEUE RESULT_BLOCKING ? ?', [
      this.options.continueWaitTimeout * 1000,
      // queryKeyHash as compatibility fallback
      queueId || this.prefixKey(hash),
    ]);
    if (rows && rows.length) {
      return this.decodeQueryDefFromRow(rows[0], 'getResultBlocking');
    }

    return null;
  }

  public async setResultAndRemoveQuery(hash: QueryKeyHash, executionResult: unknown, _processingId: ProcessingId, queueId: QueueId): Promise<boolean> {
    const rows = await this.driver.query('QUEUE ACK ? ? ', [
      // queryKeyHash as compatibility fallback
      queueId || this.prefixKey(hash),
      executionResult ? JSON.stringify(executionResult) : executionResult
    ]);
    if (rows && rows.length === 1) {
      return rows[0].success === 'true';
    }

    // Backward compatibility for old Cube Store
    return true;
  }

  public async updateHeartBeat(hash: QueryKeyHash, queueId: QueueId | null): Promise<void> {
    await this.driver.query('QUEUE HEARTBEAT ?', [
      // queryKeyHash as compatibility fallback
      queueId || this.prefixKey(hash),
    ]);
  }
}

export class CubeStoreQueueDriver implements QueueDriverInterface {
  public constructor(
    protected readonly driverFactory: () => Promise<CubeStoreDriver>,
    protected readonly options: QueueDriverOptions
  ) {}

  protected connection: CubeStoreDriver | null = null;

  public redisHash(queryKey: QueryKey): QueryKeyHash {
    return hashQueryKey(queryKey);
  }

  protected async getConnection(): Promise<CubeStoreDriver> {
    if (this.connection) {
      return this.connection;
    }

    // eslint-disable-next-line no-return-assign
    return this.connection = await this.driverFactory();
  }

  public async createConnection(): Promise<CubestoreQueueDriverConnection> {
    return new CubestoreQueueDriverConnection(await this.getConnection(), this.options);
  }

  public release(): void {
    // nothing to release
  }
}
