import crypto from 'crypto';
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
} from '@cubejs-backend/base-driver';
import { getProcessUid } from '@cubejs-backend/shared';

import { CubeStoreDriver } from './CubeStoreDriver';

function hashQueryKey(queryKey: QueryKey, processUid?: string): QueryKeyHash {
  processUid = processUid || getProcessUid();
  const hash = crypto.createHash('md5').update(JSON.stringify(queryKey)).digest('hex');

  if (typeof queryKey === 'object' && queryKey.persistent) {
    return `${hash}@${processUid}` as any;
  }

  return hash as any;
}

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
        null,
        null,
        parseInt(rows[0].pending, 10),
        data.addedToQueueTime
      ];
    }

    throw new Error('Empty response on QUEUE ADD');
  }

  // TODO: Looks useless, because we can do it in one step - getQueriesToCancel
  public async getQueryAndRemove(hash: QueryKeyHash): Promise<[QueryDef]> {
    return [await this.cancelQuery(hash)];
  }

  public async cancelQuery(hash: QueryKeyHash): Promise<QueryDef | null> {
    const rows = await this.driver.query('QUEUE CANCEL ?', [
      this.prefixKey(hash)
    ]);
    if (rows && rows.length) {
      return this.decodeQueryDefFromRow(rows[0], 'cancelQuery');
    }

    return null;
  }

  public async freeProcessingLock(_hash: QueryKeyHash, _processingId: string, _activated: unknown): Promise<void> {
    // nothing to do
  }

  public async getActiveQueries(): Promise<string[]> {
    const rows = await this.driver.query('QUEUE ACTIVE ?', [
      this.options.redisQueuePrefix
    ]);
    return rows.map((row) => row.id);
  }

  public async getToProcessQueries(): Promise<string[]> {
    const rows = await this.driver.query('QUEUE PENDING ?', [
      this.options.redisQueuePrefix
    ]);
    return rows.map((row) => row.id);
  }

  public async getActiveAndToProcess(): Promise<[active: string[], toProcess: string[]]> {
    const rows = await this.driver.query('QUEUE LIST ?', [
      this.options.redisQueuePrefix
    ]);
    if (rows.length) {
      const active: string[] = [];
      const toProcess: string[] = [];

      for (const row of rows) {
        if (row.status === 'active') {
          active.push(row.id);
        } else {
          toProcess.push(row.id);
        }
      }

      return [
        active,
        toProcess,
      ];
    }

    return [[], []];
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
    const rows = await this.driver.query(`QUEUE LIST ${onlyKeys ? '?' : 'WITH_PAYLOAD ?'}`, [
      this.options.redisQueuePrefix
    ]);

    const defs: Record<string, QueryDef> = {};
    const toProcess: string[] = [];
    const active: string[] = [];

    for (const row of rows) {
      if (!onlyKeys) {
        defs[row.id] = this.decodeQueryDefFromRow(row, 'getQueryStageState');
      }

      if (row.status === 'pending') {
        toProcess.push(row.id);
      } else if (row.status === 'active') {
        active.push(row.id);
        // TODO: getQueryStage is broken for Executing query stage...
        toProcess.push(row.id);
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

  public async getStalledQueries(): Promise<string[]> {
    const rows = await this.driver.query('QUEUE STALLED ? ?', [
      this.options.heartBeatTimeout * 1000,
      this.options.redisQueuePrefix
    ]);
    return rows.map((row) => row.id);
  }

  public async getOrphanedQueries(): Promise<string[]> {
    const rows = await this.driver.query('QUEUE ORPHANED ? ?', [
      this.options.orphanedTimeout * 1000,
      this.options.redisQueuePrefix
    ]);
    return rows.map((row) => row.id);
  }

  public async getQueriesToCancel(): Promise<string[]> {
    const rows = await this.driver.query('QUEUE TO_CANCEL ? ? ?', [
      this.options.heartBeatTimeout * 1000,
      this.options.orphanedTimeout * 1000,
      this.options.redisQueuePrefix,
    ]);
    return rows.map((row) => row.id);
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

  public async getQueryDef(queryKey: QueryKeyHash): Promise<QueryDef | null> {
    const rows = await this.driver.query('QUEUE GET ?', [
      this.prefixKey(queryKey)
    ]);
    if (rows && rows.length) {
      return this.decodeQueryDefFromRow(rows[0], 'getQueryDef');
    }

    return null;
  }

  public async optimisticQueryUpdate(queryKey: any, toUpdate: any, _processingId: any): Promise<boolean> {
    await this.driver.query('QUEUE MERGE_EXTRA ? ?', [
      this.prefixKey(queryKey),
      JSON.stringify(toUpdate)
    ]);

    return true;
  }

  public release(): void {
    // nothing to release
  }

  public async retrieveForProcessing(queryKeyHashed: QueryKeyHash, _processingId: string): Promise<RetrieveForProcessingResponse> {
    const rows = await this.driver.query<{ active: string | null, pending: string, payload: string, extra: string | null }>('QUEUE RETRIEVE EXTENDED CONCURRENCY ? ?', [
      this.options.concurrency,
      this.prefixKey(queryKeyHashed),
    ]);
    if (rows && rows.length) {
      const active = rows[0].active ? (rows[0].active).split(',') as unknown as QueryKeyHash[] : [];
      const pending = parseInt(rows[0].pending, 10);

      if (rows[0].payload) {
        const def = this.decodeQueryDefFromRow(rows[0], 'retrieveForProcessing');

        return [
          1, null, active, pending, def, true
        ];
      } else {
        return [
          0, null, active, pending, null, false
        ];
      }
    }

    return null;
  }

  public async getResultBlocking(queryKey: string): Promise<QueryDef | null> {
    return this.getResultBlockingByHash(this.redisHash(queryKey));
  }

  public async getResultBlockingByHash(queryKeyHash: QueryKeyHash): Promise<QueryDef | null> {
    const rows = await this.driver.query('QUEUE RESULT_BLOCKING ? ?', [
      this.options.continueWaitTimeout * 1000,
      this.prefixKey(queryKeyHash),
    ]);
    if (rows && rows.length) {
      return this.decodeQueryDefFromRow(rows[0], 'getResultBlocking');
    }

    return null;
  }

  public async setResultAndRemoveQuery(hash: QueryKeyHash, executionResult: unknown, _processingId: ProcessingId): Promise<boolean> {
    const rows = await this.driver.query('QUEUE ACK ? ? ', [
      this.prefixKey(hash),
      executionResult ? JSON.stringify(executionResult) : executionResult
    ]);
    if (rows && rows.length === 1) {
      return rows[0].success === 'true';
    }

    // Backward compatibility for old Cube Store
    return true;
  }

  public async updateHeartBeat(hash: QueryKeyHash): Promise<void> {
    await this.driver.query('QUEUE HEARTBEAT ?', [
      this.prefixKey(hash)
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
