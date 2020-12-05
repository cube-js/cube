/**
 * Uses single table design with the DynamoDBCacheDriver
 * 
 * Requires ENV VARs:
 *   CUBEJS_CACHE_TABLE
 * 
 * Table needs to have:
 * partitionKey: pk (string/hash)
 * sortKey: sk (string/hash)
 * Global secondary index
 *   GSI1: 
 *     partitionKey: pk (string/hash as above)
 *     sortKey: sk (number/range)
 */

import R from 'ramda';
import { BaseQueueDriver } from './BaseQueueDriver';

const { Table, Entity } = require('dynamodb-toolbox');

const DynamoDB = require('aws-sdk/clients/dynamodb');
const DocumentClient = new DynamoDB.DocumentClient();

// Need to specify a value for the single table design and we want it static
const QUEUE_SIZE_SORT_KEY = 'empty';

export class DynamoDBQueueDriverConnection {
  public readonly driver: DynamoDBQueueDriver;
  public readonly redisQueuePrefix: string;
  public readonly continueWaitTimeout: number;
  public readonly orphanedTimeout: number;
  public readonly heartBeatTimeout: number;
  public readonly concurrency: number;

  private readonly tableName: string;
  private readonly table: typeof Table;

  private readonly queue: typeof Entity;
  private readonly queueSize: typeof Entity; // TODO: Do we need this?
  private readonly processingCounter: typeof Entity;

  constructor(driver: DynamoDBQueueDriver, options: any) {
    this.redisQueuePrefix = options.redisQueuePrefix;
    this.continueWaitTimeout = options.continueWaitTimeout;
    this.orphanedTimeout = options.orphanedTimeout;
    this.heartBeatTimeout = options.heartBeatTimeout;
    this.concurrency = options.concurrency;

    this.driver = driver;

    this.tableName = options.tableName ?? process.env.CUBEJS_CACHE_TABLE;

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
      DocumentClient
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
      name: 'Queue',

      // Define attributes
      attributes: {
        key: { partitionKey: true }, // flag as partitionKey
        updated: { hidden: true, sortKey: true, type: 'string' }, // flag as sortKey and mark hidden because we don't care
        id: { type: 'number' }
      },

      // Assign it to our table
      table: this.table
    });
  }

  async getDynamoDBResultPromise(resultListKey) {
    return this.queue.query(resultListKey)
      .then((res) => {
        return res;
      })
  }

  async getResultBlocking(queryKey) {
    const resultListKey = this.resultListKey(queryKey);

    // Check if queryKey is active query
    const exists = await this.queue.get({
      key: this.queriesDefKey(),
      queryKey: this.redisHash(queryKey)
    })

    console.log('EXISTS');
    console.log(exists);

    if (!exists || !exists.Items || exists.Items.length < 1) {
      return this.getResult(resultListKey);
    }

    // First attempt at redis brpop emulation with dynamodb (copied from LocalQueueDriver)
    const timeoutPromise = (timeout) => new Promise((resolve) => setTimeout(() => resolve(null), timeout));
    let result = await Promise.race([
      this.getDynamoDBResultPromise(resultListKey),
      timeoutPromise(this.continueWaitTimeout * 1000),
    ]);

    // We got our data so remove it
    if (result && result.Items && result.Items[0]) {
      const item = result.Items[0];
      result = JSON.parse(item.value);

      // TODO: This is wrong atm - figure out which keys to use
      this.queue.delete({
        key: resultListKey,
        queryKey: queryKey
      });
    }

    return result;
  }

  public async getResult(resultListKey: string) {
    const result = await this.queue.get({ key: resultListKey })
    const data = result && result.Item && JSON.parse(result.Item.value);

    // We got our data so remove it
    if (result && result.Item) {
      this.queue.delete({
        key: resultListKey,
        inserted: result.Item.inserted
      });
    }

    return data;
  }

  addToQueue(keyScore, queryKey, time, queryHandler, query, priority, options) {
    const transactionOptions = {
      TransactItems: [
        {
          Update: this.queue.updateParams({
            key: this.toProcessRedisKey() + this.redisHash(queryKey),
            queryKey: this.redisHash(queryKey),
            order: keyScore,
            inserted: time
          })
        },
        {
          Update: this.queue.updateParams({
            key: this.recentRedisKey() + this.redisHash(queryKey),
            queryKey: this.redisHash(queryKey),
            order: time,
            inserted: time,
          })
        },
        {
          Update: this.queue.updateParams({
            key: this.queriesDefKey() + this.redisHash(queryKey),
            queryKey: this.redisHash(queryKey),
            order: time,
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
        },
        {
          Update: this.queueSize.updateParams({
            key: this.queueSizeRedisKey(),
            sk: QUEUE_SIZE_SORT_KEY,
            size: { $add: 1 } // increment queue size by 1
          })
        },
      ]
    }

    const result = await this.executeTransactWrite(transactionOptions);
    console.log('Transaction result');
    console.log(result);

    let queueSize = undefined;
    const queueSizeResult = await this.queueSize.get({ key: this.queueSizeRedisKey() });
    if (queueSizeResult && queueSizeResult.Item) {
      queueSize = queueSizeResult.Item.size;
    }

    return [1, 1, 1, queueSize];
  }

  getToProcessQueries() {
    const queriesResult = await this.queue.query(
      this.toProcessRedisKey(), // partition key
    );
    console.log(queriesResult);

    return queriesResult.Items;
  }

  getActiveQueries() {
    const activeQueriesResult = await this.queue.query(this.activeRedisKey());
    console.log(activeQueriesResult);

    return activeQueriesResult.Items;
  }

  async getQueryAndRemove(queryKey) {
    const redisHash = this.redisHash(queryKey);

    const getQueryResult = this.queue.get({
      key: this.queriesDefKey(),
      queryKey: redisHash
    })

    if (!getQueryResult || !getQueryResult.Item) return;

    const transactionOptions = {
      TransactItems: [
        {
          Delete: this.queue.deleteParams({
            key: this.activeRedisKey(),
            queryKey: redisHash
          })
        },
        {
          Delete: this.queue.deleteParams({
            key: this.heartBeatRedisKey(),
            queryKey: redisHash
          })
        },
        {
          Delete: this.queue.deleteParams({
            key: this.toProcessRedisKey(),
            queryKey: redisHash
          })
        },
        {
          Delete: this.queue.deleteParams({
            key: this.recentRedisKey(),
            queryKey: redisHash
          })
        },
        {
          Delete: this.queue.deleteParams({
            key: this.queriesDefKey(),
            queryKey: redisHash
          })
        },
        {
          Delete: this.queue.deleteParams({
            key: this.queryProcessingLockKey(queryKey),
            queryKey: redisHash
          })
        }
      ]
    }

    const transactionResult = await this.executeTransactWrite(transactionOptions);

    // TODO: Figure out what this data is
    return [JSON.parse(getQueryResult.Item), 'something']
  }

  async setResultAndRemoveQuery(queryKey, executionResult, processingId) {
    const redisHash = this.redisHash(queryKey);

    const transactionOptions = {
      TransactItems: [
        {
          Put: this.queue.updateParams({
            key: this.resultListKey(queryKey),
            queryKey: redisHash,
            inserted: new Date().getTime()
          })
        },
        {
          Delete: this.queue.deleteParams({
            key: this.activeRedisKey(),
            queryKey: redisHash
          })
        },
        {
          Delete: this.queue.deleteParams({
            key: this.heartBeatRedisKey(),
            queryKey: redisHash
          })
        },
        {
          Delete: this.queue.deleteParams({
            key: this.toProcessRedisKey(),
            queryKey: redisHash
          })
        },
        {
          Delete: this.queue.deleteParams({
            key: this.recentRedisKey(),
            queryKey: redisHash
          })
        },
        {
          Delete: this.queue.deleteParams({
            key: this.queriesDefKey(),
            queryKey: redisHash
          })
        },
        {
          Delete: this.queue.deleteParams({
            key: this.queryProcessingLockKey(queryKey),
            queryKey: redisHash
          })
        }
      ]
    }

    const result = await this.executeTransactWrite(transactionOptions);
    console.log('Transaction result');
    console.log(result);

    return result;
  }

  getOrphanedQueries() {
    const orphanedTime = new Date().getTime() - this.orphanedTimeout * 1000;
    const orphanedQueriesResult = await this.queue.query(
      this.recentRedisKey(),
      {
        limit: 100, // limit to 100 items - TODO: validate this number
        index: 'GSI1', // query the GSI1 secondary index
        lt: orphanedTime // GSI1sk (inserted) is less than orphaned time
      }
    )

    // TODO: Sort by score?
    return orphanedQueriesResult.Items;
  }

  getStalledQueries() {
    const stalledTime = new Date().getTime() - this.heartBeatTimeout * 1000;
    const stalledQueriesResult = await this.queue.query(
      this.heartBeatRedisKey(),
      {
        limit: 100, // limit to 100 items - TODO: validate this number
        index: 'GSI1', // query the GSI1 secondary index
        lt: stalledTime // GSI1sk (inserted) is less than stalled time
      }
    )

    // TODO: Sort by score?
    return stalledQueriesResult.Items;
  }

  async getQueryStageState(onlyKeys) {
    // DynamoDB does NOT support transactional queries
    const activeResult = this.queue.query(this.activeRedisKey());
    const active = activeResult ? activeResult.Items : [];

    const toProcessResult = this.queue.query(this.toProcessRedisKey());
    const toProcess = toProcessResult ? toProcessResult.Items : [];

    let allQueryDefs = [];
    if (!onlyKeys) {
      const queriesResult = this.queue.query(this.queriesDefKey());
      allQueryDefs = queriesResult ? queriesResult.Items : [];
    }

    // const [active, toProcess, allQueryDefs] = await request.execAsync();
    return [active, toProcess, R.map(q => JSON.parse(q), allQueryDefs || {})];
  }

  async getQueryDef(queryKey) {
    const queryDefResult = await this.queue.get({
      key: this.queriesDefKey(),
      queryKey: this.redisHash(queryKey)
    })

    return queryDefResult && JSON.parse(queryDefResult.Item.value);
  }

  updateHeartBeat(queryKey) {
    return await this.queue.update({
      key: this.heartBeatRedisKey(),
      queryKey: this.redisHash(queryKey),
      inserted: new Date().getTime()
    });
  }

  async getNextProcessingId() {
    const updateResult = await this.processingCounter.update({
      key: this.processingIdKey(),
      updated: new Date().getTime().toString(),
      id: { $add: 1 }, // increment id size by 1
    }, {
      returnValues: 'updated_new'
    })

    const id = updateResult.Attributes.value;
    return id && id.toString();
  }

  async retrieveForProcessing(queryKey, processingId) {
    try {
      const lockKey = this.queryProcessingLockKey(queryKey);
      await this.redisClient.watchAsync(lockKey);

      const currentProcessId = await this.redisClient.getAsync(lockKey);

      if (currentProcessId) {
        return null;
      }

      const result = await this.redisClient.multi()
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
      }
      return result;
    } finally {
      await this.redisClient.unwatchAsync();
    }
  }

  async freeProcessingLock(queryKey, processingId, activated) {
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

  async optimisticQueryUpdate(queryKey, toUpdate, processingId) {
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
    }
  }

  // https://github.com/aws/aws-sdk-js/issues/2464#issuecomment-503524701
  executeTransactWrite(params) {
    const transactionRequest = this.table.DocumentClient.transactWrite(params);
    let cancellationReasons;
    transactionRequest.on('extractError', (response) => {
      try {
        cancellationReasons = JSON.parse(response.httpResponse.body.toString()).CancellationReasons;
      } catch (err) {
        // suppress this just in case some types of errors aren't JSON parseable
        console.error('Error extracting cancellation error', err);
      }
    });
    return new Promise((resolve, reject) => {
      transactionRequest.send((err, response) => {
        if (err) {
          console.error('Error performing transactWrite', { cancellationReasons, err });
          return reject(err);
        }
        return resolve(response);
      });
    });
  }

  /**
   * Nothing to release 
   */
  release() {
  }

  queueRedisKey(suffix) {
    return `${this.redisQueuePrefix}_${suffix}`;
  }

  queryRedisKey(queryKey, suffix) {
    return `${this.redisQueuePrefix}_${this.redisHash(queryKey)}_${suffix}`;
  }

  toProcessRedisKey() {
    return this.queueRedisKey('QUEUE');
  }

  queueSizeRedisKey() {
    return this.queueRedisKey('QUEUE_SIZE');
  }

  recentRedisKey() {
    return this.queueRedisKey('RECENT');
  }

  activeRedisKey() {
    return this.queueRedisKey('ACTIVE');
  }

  heartBeatRedisKey() {
    return this.queueRedisKey('HEART_BEAT');
  }

  queriesDefKey() {
    return this.queueRedisKey('QUERIES');
  }

  processingIdKey() {
    return this.queueRedisKey('PROCESSING_COUNTER');
  }

  resultListKey(queryKey) {
    return this.queryRedisKey(queryKey, 'RESULT');
  }

  queryProcessingLockKey(queryKey) {
    return this.queryRedisKey(queryKey, 'LOCK');
  }

  redisHash(queryKey) {
    return this.driver.redisHash(queryKey);
  }
}

export class DynamoDBQueueDriver extends BaseQueueDriver {
  public readonly options: any;

  constructor(options) {
    super();
    this.options = options;
  }

  async createConnection() {
    return new DynamoDBQueueDriverConnection(this, {
      ...this.options
    });
  }

  release(client) {
    client.release();
  }
}
