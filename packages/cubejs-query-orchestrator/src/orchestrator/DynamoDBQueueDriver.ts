/**
 * Uses single table design with the DynamoDBCacheDriver
 * Requires ENV VARs:
 *   CUBEJS_CACHE_TABLE
 * Table needs to have:
 * partitionKey: pk (string/hash)
 * sortKey: sk (string/hash)
 * Global secondary index
 *   GSI1:
 *     partitionKey: pk (string/hash as above)
 *     sortKey: sk (number/range)
 */

import { DocumentClient } from 'aws-sdk/clients/dynamodb';
import { Table, Entity } from 'dynamodb-toolbox';
import { BaseQueueDriver } from './BaseQueueDriver';

const DynamoDB = require('aws-sdk/clients/dynamodb');

// Need to specify a value for the single table design and we want it static
const QUEUE_SIZE_SORT_KEY = 'empty';
const PROCESSING_COUNTER_SORT_KEY = 'empty';

export class DynamoDBQueueDriverConnection {
  protected readonly driver: BaseQueueDriver;

  protected readonly redisQueuePrefix: string;

  protected readonly continueWaitTimeout: number;

  protected readonly orphanedTimeout: number;

  protected readonly heartBeatTimeout: number;

  protected readonly concurrency: number;

  protected readonly tableName: string;

  protected readonly table: Table;

  protected readonly queue: Entity<{}>;

  protected readonly queueSize: Entity<{}>;

  protected readonly processingCounter: Entity<{}>;

  public constructor(driver, options) {
    this.redisQueuePrefix = options.redisQueuePrefix;
    this.continueWaitTimeout = options.continueWaitTimeout;
    this.orphanedTimeout = options.orphanedTimeout;
    this.heartBeatTimeout = options.heartBeatTimeout;
    this.concurrency = options.concurrency;

    this.driver = driver;

    this.tableName = options.tableName ?? process.env.CUBEJS_CACHE_TABLE;

    const documentClient = new DynamoDB.DocumentClient();

    this.table = new Table({
      // Specify table name (used by DynamoDB)
      name: this.tableName,

      // Define partition key
      partitionKey: 'pk',
      sortKey: 'sk',

      indexes: {
        GSI1: { partitionKey: 'pk', sortKey: 'GSI1sk' },
      },

      // Add the DocumentClient
      DocumentClient: documentClient
    });

    this.queue = new Entity({
      // Specify entity name
      name: 'Queue',

      // Define attributes
      attributes: {
        key: { partitionKey: true }, // flag as partitionKey
        queryKey: { sortKey: true, type: 'string' }, // flag as sortKey
        inserted: { type: 'number', map: 'GSI1sk' },
        keyScore: { type: 'number' },
        value: { type: 'string' }
      },

      // Assign it to our table
      table: this.table
    });

    this.queueSize = new Entity({
      // Specify entity name
      name: 'QueueSize',

      // Define attributes
      attributes: {
        key: { partitionKey: true },
        updated: { hidden: true, sortKey: true },
        size: { type: 'number' },
      },

      // Assign it to our table
      table: this.table
    });

    this.processingCounter = new Entity({
      // Specify entity name
      name: 'ProcessingCounter',

      // Define attributes
      attributes: {
        key: { partitionKey: true }, // flag as partitionKey
        sk: { hidden: true, sortKey: true }, // flag as sortKey and mark hidden because we don't care
        id: { type: 'number' }
      },

      // Assign it to our table
      table: this.table
    });
  }

  private async getDynamoDBResultPromise(resultListKey) {
    return this.queue.query(resultListKey).then((res) => res);
  }

  public async getResultBlocking(queryKey) {
    // Check if queryKey is active query
    const exists = await this.queue.get({
      key: this.queriesDefKey(),
      queryKey: this.redisHash(queryKey)
    });

    if (!exists || !exists.Item) {
      return this.getResult(queryKey);
    }

    // First attempt at redis brpop emulation with dynamodb (copied from LocalQueueDriver)
    const resultListKey = this.resultListKey(queryKey);
    const timeoutPromise = (timeout) => new Promise((resolve) => setTimeout(() => resolve(null), timeout));

    // Sleep for continueWaitTimeout seconds
    await timeoutPromise(this.continueWaitTimeout * 1000);
    let result = await this.getDynamoDBResultPromise(resultListKey);

    // We got our data so parse and remove it
    if (result && result.Items && result.Items[0]) {
      const item = result.Items[0];
      result = JSON.parse(item.value);

      await this.queue.delete({
        key: resultListKey,
        queryKey: this.redisHash(queryKey)
      });

      return result;
    }

    // We did not get any data, return null and query queue will throw new ContinueWaitError();
    return null;
  }

  public async getResult(queryKey) {
    const resultListKey = this.resultListKey(queryKey);
    const result = await this.queue.get({ key: resultListKey, queryKey: this.redisHash(queryKey) });
    const data = result && result.Item && JSON.parse(result.Item.value);

    // We got our data so remove it
    if (result && result.Item) {
      await this.queue.delete({
        key: resultListKey,
        queryKey: this.redisHash(queryKey)
      });
    }

    return data;
  }

  public async addToQueue(keyScore, queryKey, time, queryHandler, query, priority, options) {
    const redisHash = this.redisHash(queryKey);

    try {
      await this.table.transactWrite(
        [
          this.queue.putTransaction({ 
            key: this.toProcessRedisKey(), 
            queryKey: redisHash, 
            keyScore, 
            inserted: time 
          }, { 
            conditions: { exists: false, attr: 'queryKey' } // prevents duplicate add but idk maybe we do not care?
          }),
          this.queue.putTransaction({ key: this.recentRedisKey(), queryKey: redisHash, inserted: time }),
          this.queue.putTransaction({
            key: this.queriesDefKey(),
            queryKey: redisHash,
            inserted: time,
            value: JSON.stringify({
              queryHandler,
              query,
              queryKey,
              stageQueryKey: options.stageQueryKey,
              priority,
              requestId: options.requestId,
              addedToQueueTime: new Date().getTime()
            })
          })
        ]
      );
    } catch (err) {
      console.error('### ERROR IN ADD TO QUEUE TRANSACTION');
      console.error(err);
      console.log(query);
      return [0, 0, 0, 0];
    }

    let queueSize;
    const queueSizeResult = await this.queueSize.update({
      key: this.queueSizeRedisKey(),
      sk: QUEUE_SIZE_SORT_KEY,
      size: { $add: 1 } // increment queue size by 1 })
    }, {
      returnValues: 'updated_new'
    }) as DocumentClient.UpdateItemOutput;

    if (queueSizeResult && queueSizeResult.Attributes) {
      queueSize = queueSizeResult.Attributes.size;
    }

    return [1, 1, 1, queueSize];
  }

  public async getToProcessQueries() {
    const queriesResult = await this.queue.query(this.toProcessRedisKey());
    return queriesResult && queriesResult.Items ? queriesResult.Items.map(item => item.queryKey) : [];
  }

  public async getActiveQueries() {
    const activeQueriesResult = await this.queue.query(this.activeRedisKey());
    return activeQueriesResult && activeQueriesResult.Items ? activeQueriesResult.Items.map(item => item.queryKey) : [];
  }

  public async getQueryAndRemove(queryKey) {
    const redisHash = this.redisHash(queryKey);

    const getQueryResult = await this.queue.get({
      key: this.queriesDefKey(),
      queryKey: redisHash
    });

    if (!getQueryResult || !getQueryResult.Item) return [];

    try {
      await this.table.transactWrite(
        [
          this.queue.deleteTransaction({ key: this.activeRedisKey(), queryKey: redisHash }),
          this.queue.deleteTransaction({ key: this.heartBeatRedisKey(), queryKey: redisHash }),
          this.queue.deleteTransaction({ key: this.toProcessRedisKey(), queryKey: redisHash }),
          this.queue.deleteTransaction({ key: this.recentRedisKey(), queryKey: redisHash }),
          this.queue.deleteTransaction({ key: this.queriesDefKey(), queryKey: redisHash }),
          this.queue.deleteTransaction({ key: this.queryProcessingLockKey(queryKey), queryKey: redisHash }),
          this.queueSize.updateTransaction({
            key: this.queueSizeRedisKey(),
            sk: QUEUE_SIZE_SORT_KEY,
            size: { $add: -1 } // decrement queue size by 1
          })
        ]
      );
    } catch (err) {
      console.error('### ERROR EXECUTING CLEANUP TRANSACTION ###');
      console.error(err);
    }

    return [JSON.parse(getQueryResult.Item.value)];
  }

  public async setResultAndRemoveQuery(queryKey, executionResult, processingId) {
    const redisHash = this.redisHash(queryKey);

    const lockResult = await this.queue.get({
      key: this.queryProcessingLockKey(queryKey),
      queryKey: redisHash
    });

    if (lockResult && lockResult.Item) {
      const currentProcessId = lockResult.Item.value;
      if (processingId !== currentProcessId.toString()) {
        return false;
      }
    }

    return this.table.transactWrite(
      [
        this.queue.putTransaction({
          key: this.resultListKey(queryKey),
          queryKey: redisHash,
          value: JSON.stringify(executionResult),
          inserted: new Date().getTime()
        }),
        this.queue.deleteTransaction({ key: this.activeRedisKey(), queryKey: redisHash }),
        this.queue.deleteTransaction({ key: this.heartBeatRedisKey(), queryKey: redisHash }),
        this.queue.deleteTransaction({ key: this.toProcessRedisKey(), queryKey: redisHash }),
        this.queue.deleteTransaction({ key: this.recentRedisKey(), queryKey: redisHash }),
        this.queue.deleteTransaction({ key: this.queriesDefKey(), queryKey: redisHash }),
        this.queue.deleteTransaction({ key: this.queryProcessingLockKey(queryKey), queryKey: redisHash }),
        this.queueSize.updateTransaction({
          key: this.queueSizeRedisKey(),
          sk: QUEUE_SIZE_SORT_KEY,
          size: { $add: -1 } // decrement queue size by 1
        })
      ]
    );
  }

  public async getOrphanedQueries() {
    const orphanedTime = new Date().getTime() - this.orphanedTimeout * 1000;
    const orphanedQueriesResult = await this.queue.query(this.recentRedisKey(), {
      limit: 50, // limit to 50 items
      index: 'GSI1', // query the GSI1 secondary index
      lt: orphanedTime // GSI1sk (inserted) is less than orphaned time
    });

    const queryKeys = orphanedQueriesResult.Items ? orphanedQueriesResult.Items.map(item => item.queryKey) : [];
    return queryKeys;
  }

  public async getStalledQueries() {
    const stalledTime = new Date().getTime() - this.heartBeatTimeout * 1000;
    const stalledQueriesResult = await this.queue.query(this.heartBeatRedisKey(), {
      limit: 50, // limit to 50 items
      index: 'GSI1', // query the GSI1 secondary index
      lt: stalledTime // GSI1sk (inserted) is less than stalled time
    });

    const queryKeys = stalledQueriesResult.Items ? stalledQueriesResult.Items.map(item => item.queryKey) : [];
    return queryKeys;
  }

  public async getQueryStageState(onlyKeys) {
    // DynamoDB does NOT support transactional queries
    const activeResult = await this.queue.query(this.activeRedisKey());
    const active = activeResult?.Items
      ? activeResult.Items.map((item: any) => item.queryKey)
      : [];

    const toProcessResult = await this.queue.query(this.toProcessRedisKey());
    const toProcess = toProcessResult?.Items
      ? toProcessResult.Items.map((item: any) => item.queryKey)
      : [];

    const allQueryDefs: any = {};
    if (!onlyKeys) {
      const queriesResult = await this.queue.query(this.queriesDefKey());
      // allQueryDefs = queriesResult ? queriesResult.Items : {};
      queriesResult?.Items?.forEach(q => {
        const value = JSON.parse(q.value);
        allQueryDefs[value.stageQueryKey] = value;
      });
    }

    return [active, toProcess, allQueryDefs];
  }

  public async getQueryDef(queryKey) {
    const queryDefResult = await this.queue.get({
      key: this.queriesDefKey(),
      queryKey: this.redisHash(queryKey)
    });

    return queryDefResult && queryDefResult.Item && JSON.parse(queryDefResult.Item.value);
  }

  public updateHeartBeat(queryKey) {
    return this.queue.update({
      key: this.heartBeatRedisKey(),
      queryKey: this.redisHash(queryKey),
      inserted: new Date().getTime()
    });
  }

  /**
   * Increments the processing id by 1 and returns the value
   */
  public async getNextProcessingId() {
    const updateResult = await this.processingCounter.update({
      key: this.processingIdKey(),
      sk: PROCESSING_COUNTER_SORT_KEY,
      id: { $add: 1 }, // increment id size by 1
    }, {
      returnValues: 'updated_new'
    }) as DocumentClient.UpdateItemOutput;

    const { id } = updateResult.Attributes;
    return id && id.toString();
  }

  public async retrieveForProcessing(queryKey, processingId) {
    const lockKey = this.queryProcessingLockKey(queryKey);

    let lockAcquired = false;
    const getLockResult = await this.queue.get({
      key: lockKey,
      queryKey: this.redisHash(queryKey)
    });

    if (!getLockResult || !getLockResult.Item) {
      await this.queue.update({
        key: lockKey,
        queryKey: this.redisHash(queryKey),
        value: processingId
      });

      lockAcquired = true;
    } else {
      return null;
    }

    // Query active keys based on concurrency
    const queryActiveToRemoveResult = await this.queue.query(this.activeRedisKey(), {
      limit: this.concurrency,
      index: 'GSI1' // Orders by GSIsk which is inserted time
    });

    const activeUpdateTransactionOptions = [];

    // If we already have this.concurrency amount of items, remove them
    if (queryActiveToRemoveResult.Items.length >= this.concurrency) {
      for (let i = 0; i < queryActiveToRemoveResult.Items.length; i++) {
        const query = queryActiveToRemoveResult.Items[i];
        activeUpdateTransactionOptions.push(
          this.queue.deleteTransaction({ key: this.activeRedisKey(), queryKey: query.queryKey })
        );
      }
    }

    // Add the active redis processing id and querykey
    activeUpdateTransactionOptions.push(
      this.queue.putTransaction({
        key: this.activeRedisKey(),
        queryKey: this.redisHash(queryKey),
        inserted: new Date().getTime()
      })
    );

    // Execute transaction
    await this.table.transactWrite(activeUpdateTransactionOptions);
    const added = 1;

    // Query active -> concurrency limit
    const queryActiveResult = await this.queue.query(this.activeRedisKey(), {
      limit: this.concurrency,
      index: 'GSI1' // Orders by GSIsk which is inserted time
    });

    // Get number of members in toProcess (queueSize)
    // Get the query to process
    let getTransactionResult: DocumentClient.TransactGetItemsOutput | undefined;

    try {
      getTransactionResult = await this.table.transactGet([
        this.queueSize.getTransaction({ key: this.queueSizeRedisKey(), sk: QUEUE_SIZE_SORT_KEY }),
        this.queue.getTransaction({ key: this.queriesDefKey(), queryKey: this.redisHash(queryKey) }),
      ]) as DocumentClient.TransactGetItemsOutput;
    } catch (err) {
      // console.error(err);
      getTransactionResult = undefined;
    }

    const queueSize = getTransactionResult?.Responses[0]?.Item.size ?? undefined;
    const queryData = getTransactionResult?.Responses[1]?.Item
      ? JSON.parse(getTransactionResult.Responses[1].Item.value)
      : undefined;

    // Add the heartbeat
    await this.queue.put({
      key: this.heartBeatRedisKey(),
      queryKey: this.redisHash(queryKey),
      inserted: new Date().getTime()
    });

    const activeKeys = queryActiveResult && queryActiveResult.Items
      ? queryActiveResult.Items.map(query => query.queryKey)
      : [];

    return [
      added, null, activeKeys, queueSize, queryData, lockAcquired
    ]; // TODO nulls
  }

  public async freeProcessingLock(queryKey, processingId, activated) {
    const lockKey = this.queryProcessingLockKey(queryKey);
    const currentProcessIdResult = await this.queue.get({
      key: lockKey,
      queryKey: this.redisHash(queryKey)
    });

    if (currentProcessIdResult
      && currentProcessIdResult.Item
      && currentProcessIdResult.Item.value === processingId.toString()
    ) {
      const removeTransaction = [
        this.queue.deleteTransaction({ key: lockKey, queryKey: this.redisHash(queryKey) })
      ];

      if (activated) {
        removeTransaction.push(
          this.queue.deleteTransaction({ key: this.activeRedisKey(), queryKey: this.redisHash(queryKey) })
        );
      }

      await this.table.transactWrite(removeTransaction);
      return null;
    }

    return currentProcessIdResult && currentProcessIdResult.Item && currentProcessIdResult.Item.value;
  }

  public async optimisticQueryUpdate(queryKey, toUpdate, processingId) {
    let query = await this.getQueryDef(queryKey);
    for (let i = 0; i < 10; i++) {
      if (query) {
        // Check for lock
        const currentProcessIdResult = await this.queue.get({
          key: this.queryProcessingLockKey(queryKey),
          queryKey: this.redisHash(queryKey)
        });

        if (currentProcessIdResult.Item.value !== processingId.toString()) {
          return false;
        }

        const updateResult = await this.queue.update({
          key: this.queriesDefKey(),
          queryKey: this.redisHash(queryKey)
        }, { returnValues: 'all_old' }) as DocumentClient.UpdateItemOutput;

        const beforeUpdate = JSON.parse(updateResult.Attributes.value);
        if (JSON.stringify(query) === JSON.stringify(beforeUpdate)) {
          return true;
        }

        query = beforeUpdate;
      }
    }

    throw new Error(`Can't update ${queryKey} with ${JSON.stringify(toUpdate)}`);
  }

  // eslint-disable-next-line @typescript-eslint/no-empty-function
  public release() {
  }

  private queueRedisKey(suffix) {
    return `${this.redisQueuePrefix}_${suffix}`;
  }

  private queryRedisKey(queryKey, suffix) {
    return `${this.redisQueuePrefix}_${this.redisHash(queryKey)}_${suffix}`;
  }

  private toProcessRedisKey() {
    return this.queueRedisKey('QUEUE');
  }

  private queueSizeRedisKey() {
    return this.queueRedisKey('QUEUE_SIZE');
  }

  private recentRedisKey() {
    return this.queueRedisKey('RECENT');
  }

  private activeRedisKey() {
    return this.queueRedisKey('ACTIVE');
  }

  private heartBeatRedisKey() {
    return this.queueRedisKey('HEART_BEAT');
  }

  private queriesDefKey() {
    return this.queueRedisKey('QUERIES');
  }

  private processingIdKey() {
    return this.queueRedisKey('PROCESSING_COUNTER');
  }

  private resultListKey(queryKey) {
    return this.queryRedisKey(queryKey, 'RESULT');
  }

  private queryProcessingLockKey(queryKey) {
    return this.queryRedisKey(queryKey, 'LOCK');
  }

  private redisHash(queryKey) {
    return this.driver.redisHash(queryKey);
  }
}

export class DynamoDBQueueDriver extends BaseQueueDriver {
  protected readonly options;

  public constructor(options) {
    super();
    this.options = options;
  }

  public async createConnection() {
    return new DynamoDBQueueDriverConnection(this, {
      ...this.options
    });
  }

  public release(client) {
    client.release();
  }
}
