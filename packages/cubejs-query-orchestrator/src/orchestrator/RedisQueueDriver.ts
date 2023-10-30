/* eslint-disable no-use-before-define */
import R from 'ramda';
import {
  QueueDriverInterface,
  QueueDriverConnectionInterface,
  QueueDriverOptions,
  GetActiveAndToProcessResponse,
  QueryDef,
  QueryKeyHash,
  QueryStageStateResponse,
  QueryKey,
  AddToQueueQuery,
  AddToQueueOptions,
  AddToQueueResponse,
  ProcessingId,
  RetrieveForProcessingResponse, QueryKeysTuple,
} from '@cubejs-backend/base-driver';

import { BaseQueueDriver } from './BaseQueueDriver';
import { RedisPool } from './RedisPool';

// TODO: Use real type
type AsyncRedisClient = any;

interface RedisQueueDriverConnectionOptions extends QueueDriverOptions {
  redisClient: AsyncRedisClient,
}

export class RedisQueueDriverConnection implements QueueDriverConnectionInterface {
  protected readonly redisClient: AsyncRedisClient;

  protected readonly driver: RedisQueueDriver;

  protected readonly redisQueuePrefix: string;

  protected readonly heartBeatTimeout: number;

  protected readonly concurrency: number;

  protected readonly getQueueEventsBus: any;

  protected readonly continueWaitTimeout: number;

  public constructor(driver: RedisQueueDriver, options: RedisQueueDriverConnectionOptions) {
    this.driver = driver;
    this.redisClient = options.redisClient;
    this.redisQueuePrefix = options.redisQueuePrefix;
    this.continueWaitTimeout = options.continueWaitTimeout;
    this.heartBeatTimeout = options.heartBeatTimeout;
    this.concurrency = options.concurrency;
    this.getQueueEventsBus = options.getQueueEventsBus;
  }

  public getRedisClient() {
    return this.redisClient;
  }

  public async getResultBlocking(queryKeyHash) {
    // Double redisHash apply is being used here
    const resultListKey = this.resultListKey(queryKeyHash);
    if (!(await this.redisClient.hgetAsync([this.queriesDefKey(), queryKeyHash]))) {
      return this.getResult(queryKeyHash);
    }
    const result = await this.redisClient.brpopAsync([resultListKey, this.continueWaitTimeout]);
    if (result) {
      await this.redisClient.lpushAsync([resultListKey, result[1]]);
      await this.redisClient.rpopAsync(resultListKey);
    }
    return result && JSON.parse(result[1]);
  }

  public async getResult(queryKey) {
    const resultListKey = this.resultListKey(queryKey);
    const result = await this.redisClient.rpopAsync(resultListKey);
    return result && JSON.parse(result);
  }

  public async getQueriesToCancel() {
    return (
      await this.getStalledQueries()
    ).concat(
      await this.getOrphanedQueries()
    );
  }

  public async getActiveAndToProcess(): Promise<GetActiveAndToProcessResponse> {
    const active = await this.getActiveQueries();
    const toProcess = await this.getToProcessQueries();

    return [
      active,
      toProcess
    ];
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
      addedToQueueTime: new Date().getTime(),
      queueId: options.queueId,
    };

    const tx = this.redisClient.multi()
      .zadd([this.toProcessRedisKey(), 'NX', keyScore, this.redisHash(queryKey)])
      .zadd([this.recentRedisKey(), orphanedTime, this.redisHash(queryKey)])
      .hsetnx([
        this.queriesDefKey(),
        this.redisHash(queryKey),
        JSON.stringify(data)
      ])
      .zcard(this.toProcessRedisKey());

    if (this.getQueueEventsBus) {
      tx.publish(
        this.getQueueEventsBus().eventsChannel,
        JSON.stringify({
          event: 'addedToQueue',
          redisQueuePrefix: this.redisQueuePrefix,
          queryKey: this.redisHash(queryKey),
          payload: data
        })
      );
    }

    const [added, _b, _c, queueSize] = await tx.execAsync();

    return [
      added,
      null,
      queueSize,
      data.addedToQueueTime
    ];
  }

  public async getToProcessQueries(): Promise<QueryKeysTuple[]> {
    const rows = await this.redisClient.zrangeAsync([this.toProcessRedisKey(), 0, -1]);

    return rows.map((queryKeyHash) => [queryKeyHash, null]);
  }

  public async getActiveQueries(): Promise<QueryKeysTuple[]> {
    const rows = await this.redisClient.zrangeAsync([this.activeRedisKey(), 0, -1]);

    return rows.map((queryKeyHash) => [queryKeyHash, null]);
  }

  public async getQueryAndRemove(queryKey: QueryKeyHash): Promise<[QueryDef]> {
    const [query, ...restResult] = await this.redisClient.multi()
      .hget([this.queriesDefKey(), this.redisHash(queryKey)])
      .zrem([this.activeRedisKey(), this.redisHash(queryKey)])
      .zrem([this.heartBeatRedisKey(), this.redisHash(queryKey)])
      .zrem([this.toProcessRedisKey(), this.redisHash(queryKey)])
      .zrem([this.recentRedisKey(), this.redisHash(queryKey)])
      .hdel([this.queriesDefKey(), this.redisHash(queryKey)])
      .del(this.queryProcessingLockKey(queryKey))
      .execAsync();

    return [JSON.parse(query), ...restResult] as any;
  }

  public async cancelQuery(queryKey) {
    const [query] = await this.getQueryAndRemove(queryKey);

    if (this.getQueueEventsBus) {
      await this.redisClient.publish(
        this.getQueueEventsBus().eventsChannel,
        JSON.stringify({
          event: 'cancelQuery',
          redisQueuePrefix: this.redisQueuePrefix,
          queryKey: this.redisHash(queryKey),
          payload: query
        })
      );
    }

    return query;
  }

  public async setResultAndRemoveQuery(queryKey, executionResult, processingId) {
    try {
      await this.redisClient.watchAsync(this.queryProcessingLockKey(queryKey));
      const currentProcessId = await this.redisClient.getAsync(this.queryProcessingLockKey(queryKey));
      if (processingId !== currentProcessId) {
        return false;
      }
      const tx = this.redisClient.multi()
        .lpush([this.resultListKey(queryKey), JSON.stringify(executionResult)])
        .zrem([this.activeRedisKey(), this.redisHash(queryKey)])
        .zrem([this.heartBeatRedisKey(), this.redisHash(queryKey)])
        .zrem([this.toProcessRedisKey(), this.redisHash(queryKey)])
        .zrem([this.recentRedisKey(), this.redisHash(queryKey)])
        .hdel([this.queriesDefKey(), this.redisHash(queryKey)])
        .del(this.queryProcessingLockKey(queryKey));

      if (this.getQueueEventsBus) {
        tx.publish(
          this.getQueueEventsBus().eventsChannel,
          JSON.stringify({
            event: 'setResultAndRemoveQuery',
            redisQueuePrefix: this.redisQueuePrefix,
            queryKey: this.redisHash(queryKey),
            payload: executionResult
          })
        );
      }
      return tx.execAsync();
    } finally {
      await this.redisClient.unwatchAsync();
    }
  }

  public async getOrphanedQueries(): Promise<QueryKeysTuple[]> {
    const rows = await this.redisClient.zrangebyscoreAsync(
      [this.recentRedisKey(), 0, new Date().getTime()]
    );

    return rows.map((queryKeyHash) => [queryKeyHash, null]);
  }

  public async getStalledQueries(): Promise<QueryKeysTuple[]> {
    const rows = await this.redisClient.zrangebyscoreAsync(
      [this.heartBeatRedisKey(), 0, (new Date().getTime() - this.heartBeatTimeout * 1000)]
    );

    return rows.map((queryKeyHash) => [queryKeyHash, null]);
  }

  public async getQueryStageState(onlyKeys: boolean): Promise<QueryStageStateResponse> {
    let request = this.redisClient.multi()
      .zrange([this.activeRedisKey(), 0, -1])
      .zrange([this.toProcessRedisKey(), 0, -1]);

    if (!onlyKeys) {
      request = request.hgetall(this.queriesDefKey());
    }

    const [active, toProcess, allQueryDefs] = await request.execAsync();
    const defs: Record<string, QueryDef> = R.map(q => JSON.parse(q), allQueryDefs || {});

    return [active, toProcess, defs];
  }

  public async getQueryDef(queryKey) {
    const query = await this.redisClient.hgetAsync([this.queriesDefKey(), queryKey]);
    return JSON.parse(query);
  }

  /**
   * Updates heart beat for the processing query by its `queryKey`.
   */
  public async updateHeartBeat(queryKey) {
    return this.redisClient.zaddAsync([this.heartBeatRedisKey(), new Date().getTime(), this.redisHash(queryKey)]);
  }

  public async getNextProcessingId() {
    const id = await this.redisClient.incrAsync(this.processingIdKey());
    return id && id.toString();
  }

  public async retrieveForProcessing(queryKey: QueryKeyHash, processingId: ProcessingId): Promise<RetrieveForProcessingResponse> {
    try {
      const lockKey = this.queryProcessingLockKey(queryKey);
      await this.redisClient.watchAsync(lockKey);

      const currentProcessId = await this.redisClient.getAsync(lockKey);
      if (currentProcessId) {
        return null;
      }

      const result =
        await this.redisClient.multi()
          .zadd([this.activeRedisKey(), 'NX', processingId, this.redisHash(queryKey)])
          .zremrangebyrank([this.activeRedisKey(), this.concurrency, -1])
          .zrange([this.activeRedisKey(), 0, this.concurrency - 1])
          .zcard(this.toProcessRedisKey())
          .hget(([this.queriesDefKey(), this.redisHash(queryKey)]))
          .set(lockKey, processingId, 'NX')
          .zadd([this.heartBeatRedisKey(), 'NX', new Date().getTime(), this.redisHash(queryKey)])
          .execAsync();

      if (result) {
        result[4] = JSON.parse(result[4]);

        if (this.getQueueEventsBus) {
          await this.redisClient.publish(
            this.getQueueEventsBus().eventsChannel,
            JSON.stringify({
              event: 'retrievedForProcessing',
              redisQueuePrefix: this.redisQueuePrefix,
              queryKey: this.redisHash(queryKey),
              payload: result[4]
            })
          );
        }
      }

      const [insertedCount, _b, activeKeys, queueSize, query, processingLockAcquired] = result;

      return [
        insertedCount,
        // this driver doesnt support queue id
        null,
        activeKeys,
        queueSize,
        query,
        processingLockAcquired
      ];
    } finally {
      await this.redisClient.unwatchAsync();
    }
  }

  public async freeProcessingLock(queryKey, processingId, activated) {
    try {
      const lockKey = this.queryProcessingLockKey(queryKey);
      await this.redisClient.watchAsync(lockKey);
      const currentProcessId = await this.redisClient.getAsync(lockKey);
      if (currentProcessId === processingId) {
        let removeCommand = this.redisClient.multi()
          .del(lockKey);
        if (activated) {
          removeCommand = removeCommand.zrem([this.activeRedisKey(), this.redisHash(queryKey)]);
        }
        await removeCommand
          .execAsync();
        return null;
      } else {
        return currentProcessId;
      }
    } finally {
      await this.redisClient.unwatchAsync();
    }
  }

  public async optimisticQueryUpdate(queryKey, toUpdate, processingId) {
    try {
      let query = await this.getQueryDef(queryKey);
      for (let i = 0; i < 10; i++) {
        if (query) {
          // eslint-disable-next-line no-await-in-loop
          await this.redisClient.watchAsync(this.queryProcessingLockKey(queryKey));
          const currentProcessId = await this.redisClient.getAsync(this.queryProcessingLockKey(queryKey));
          if (currentProcessId !== processingId) {
            return false;
          }
          let [beforeUpdate] = await this.redisClient
            .multi()
            .hget([this.queriesDefKey(), this.redisHash(queryKey)])
            .hset([this.queriesDefKey(), this.redisHash(queryKey), JSON.stringify({ ...query, ...toUpdate })])
            .execAsync();
          beforeUpdate = JSON.parse(beforeUpdate);
          if (JSON.stringify(query) === JSON.stringify(beforeUpdate)) {
            return true;
          }
          query = beforeUpdate;
        }
      }
      throw new Error(`Can't update ${queryKey} with ${JSON.stringify(toUpdate)}`);
    } finally {
      await this.redisClient.unwatchAsync();
    }
  }

  public release() {
    return this.redisClient.quit();
  }

  protected toProcessRedisKey() {
    return this.queueRedisKey('QUEUE');
  }

  protected recentRedisKey() {
    return this.queueRedisKey('RECENT');
  }

  protected activeRedisKey() {
    return this.queueRedisKey('ACTIVE');
  }

  protected heartBeatRedisKey() {
    return this.queueRedisKey('HEART_BEAT');
  }

  protected queryRedisKey(queryKey, suffix) {
    return `${this.redisQueuePrefix}_${this.redisHash(queryKey)}_${suffix}`;
  }

  protected queueRedisKey(suffix) {
    return `${this.redisQueuePrefix}_${suffix}`;
  }

  protected queriesDefKey() {
    return this.queueRedisKey('QUERIES');
  }

  protected processingIdKey() {
    return this.queueRedisKey('PROCESSING_COUNTER');
  }

  protected resultListKey(queryKey) {
    return this.queryRedisKey(queryKey, 'RESULT');
  }

  protected queryProcessingLockKey(queryKey) {
    return this.queryRedisKey(queryKey, 'LOCK');
  }

  public redisHash(queryKey) {
    return this.driver.redisHash(queryKey);
  }
}

interface RedisQueueDriverOptions extends QueueDriverOptions {
  redisPool: RedisPool
}

export class RedisQueueDriver extends BaseQueueDriver implements QueueDriverInterface {
  protected readonly redisPool: RedisPool;

  protected readonly options: RedisQueueDriverOptions;

  public constructor(options: RedisQueueDriverOptions) {
    super(options.processUid);
    this.redisPool = options.redisPool;
    this.options = options;
  }

  public async createConnection() {
    const redisClient = await this.redisPool.getClient();
    return new RedisQueueDriverConnection(this, {
      redisClient,
      ...this.options
    });
  }

  public release(connection: RedisQueueDriverConnection) {
    this.redisPool.release(connection.getRedisClient());
  }
}
