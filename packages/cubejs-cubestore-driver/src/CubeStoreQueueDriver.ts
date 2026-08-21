import crypto from 'crypto';
import {
  QueueDriverInterface,
  QueueDriverConnectionInterface,
  QueryStageStateResponse,
  QueryDef,
  RetrieveForProcessingResponse,
  RetrieveForProcessingSuccess,
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
import { getEnv, getProcessUid } from '@cubejs-backend/shared';

import { CubeStoreDriver } from './CubeStoreDriver';

function hashQueryKey(queryKey: QueryKey, processUid?: string): QueryKeyHash {
  processUid = processUid || getProcessUid();
  const hash = crypto.createHash('md5').update(JSON.stringify(queryKey)).digest('hex');

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

// cube store convert int64 to string
type CubeStoreClaimResponse = {
  id: string,
  active: string | null,
  pending: string,
  payload: string | null,
  extra: string | null,
};

export class CubestoreQueueDriverConnection implements QueueDriverConnectionInterface {
  protected readonly externalIdEnabled: boolean;

  protected readonly fastTrackEnabled: boolean;

  protected readonly sendParameters: boolean;

  public constructor(
    protected readonly driver: CubeStoreDriver,
    protected readonly options: QueueDriverOptions,
  ) {
    this.externalIdEnabled = getEnv('queueExternalId');
    this.fastTrackEnabled = getEnv('queueFastTrack');
    this.sendParameters = getEnv('cubestoreSendableParameters');
  }

  public async useFastTrack(): Promise<boolean> {
    if (this.fastTrackEnabled) {
      return this.driver.hasCapability('queueAddAndRetrieve');
    }

    return false;
  }

  public async useExternalId(): Promise<boolean> {
    if (this.externalIdEnabled) {
      return this.driver.hasCapability('queueExternalId');
    }

    return false;
  }

  public redisHash(queryKey: QueryKey): QueryKeyHash {
    return hashQueryKey(queryKey, this.options.processUid);
  }

  protected prefixKey(queryKey: QueryKey): string {
    return `${this.options.redisQueuePrefix}:${queryKey}`;
  }

  protected async buildAddCommand(
    queryKey: QueryKey,
    queryHandler: string,
    query: AddToQueueQuery,
    priority: number,
    options: AddToQueueOptions
  ) {
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

    const useExternalId = options.externalId && await this.useExternalId();
    if (useExternalId) {
      values.push(options.externalId!);
    }

    values.push(this.prefixKey(this.redisHash(queryKey)));
    values.push(JSON.stringify(data));

    const exclusive = queryKey.persistent && await this.driver.hasCapability('queueExclusive');

    return {
      addedToQueueTime: data.addedToQueueTime,
      values,
      modifiers: `${exclusive ? ' EXCLUSIVE' : ''} PRIORITY ?${options.orphanedTimeout ? ' ORPHANED ?' : ''}${useExternalId ? ' EXTERNAL_ID ?' : ''} ? ?`,
    };
  }

  public async addToQueue(
    queryKey: QueryKey,
    queryHandler: string,
    query: AddToQueueQuery,
    priority: number,
    options: AddToQueueOptions
  ): Promise<AddToQueueResponse> {
    const { modifiers, values, addedToQueueTime } = await this.buildAddCommand(queryKey, queryHandler, query, priority, options);

    const fastTrack = await this.useFastTrack();
    if (fastTrack) {
      values.push(this.options.concurrency);
    }

    const command = fastTrack ? 'ADD_AND_RETRIEVE' : 'ADD';
    const rows = await this.driver.query<CubeStoreClaimResponse & { added: string }>(`QUEUE ${command}${modifiers}${fastTrack ? ' ?' : ''}`, values);
    if (rows && rows.length) {
      return [
        rows[0].added === 'true' ? 1 : 0,
        rows[0].id ? parseInt(rows[0].id, 10) : null,
        parseInt(rows[0].pending, 10),
        addedToQueueTime,
        // An item which already existed is never added twice, but it still can be claimed
        fastTrack ? this.decodeClaimFromRow(rows[0], 'addToQueue') : null,
      ];
    }

    throw new Error(`Empty response on QUEUE ${command}`);
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
    const active: QueryKeysTuple[] = [];
    const toProcess: QueryKeysTuple[] = [];

    const rows = await this.driver.query<CubeStoreListResponse>('QUEUE LIST ?', [
      this.options.redisQueuePrefix
    ]);
    if (rows.length) {
      for (const row of rows) {
        if (row.status === 'active') {
          active.push([
            row.id as QueryKeyHash,
            row.queue_id ? parseInt(row.queue_id, 10) : null,
          ]);
        } else {
          toProcess.push([
            row.id as QueryKeyHash,
            row.queue_id ? parseInt(row.queue_id, 10) : null,
          ]);
        }
      }
    }

    return [
      active,
      toProcess,
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

  public async getResult(queryKey: QueryKey, externalId?: string): Promise<unknown> {
    const params: string[] = [];

    const passExternalId = externalId && await this.useExternalId();
    if (passExternalId) {
      params.push(externalId);
    }

    params.push(this.prefixKey(this.redisHash(queryKey)));

    const rows = await this.driver.query(`QUEUE RESULT ${passExternalId ? 'EXTERNAL_ID ? ' : ''}?`, params);
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

  protected decodeActiveKeysFromRow(active: string | null): QueryKeyHash[] {
    return active ? active.split(',') as unknown as QueryKeyHash[] : [];
  }

  /**
   * Shared by `QUEUE RETRIEVE` and `QUEUE ADD_AND_RETRIEVE` so that they cannot drift apart.
   */
  protected decodeClaimFromRow(row: CubeStoreClaimResponse, method: string): RetrieveForProcessingSuccess | null {
    if (!row.payload) {
      return null;
    }

    return [
      1,
      row.id ? parseInt(row.id, 10) : null,
      this.decodeActiveKeysFromRow(row.active),
      parseInt(row.pending, 10),
      this.decodeQueryDefFromRow(row as { payload: string, extra?: string | null }, method),
      true
    ];
  }

  public async retrieveForProcessing(hash: QueryKeyHash, _processingId: string): Promise<RetrieveForProcessingResponse> {
    const rows = await this.driver.query<CubeStoreClaimResponse>('QUEUE RETRIEVE EXTENDED CONCURRENCY ? ?', [
      this.options.concurrency,
      this.prefixKey(hash),
    ]);
    if (rows && rows.length) {
      return this.decodeClaimFromRow(rows[0], 'retrieveForProcessing') || [
        0,
        null,
        this.decodeActiveKeysFromRow(rows[0].active),
        parseInt(rows[0].pending, 10),
        null,
        false
      ];
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
    const rows = await this.driver.query('QUEUE ACK ? ?', [
      // queryKeyHash as compatibility fallback
      queueId || this.prefixKey(hash),
      executionResult ? JSON.stringify(executionResult) : executionResult
    ], {
      sendParameters: this.sendParameters && await this.driver.hasCapability('sendableParameters')
    });
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
