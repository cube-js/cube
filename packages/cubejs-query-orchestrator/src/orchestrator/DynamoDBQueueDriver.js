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
const PROCESSING_COUNTER_SORT_KEY = 'empty';

export class DynamoDBQueueDriverConnection {
  driver;
  redisQueuePrefix;
  continueWaitTimeout;
  orphanedTimeout;
  heartBeatTimeout;
  concurrency;

  tableName;
  table;

  queue;
  queueSize;
  processingCounter;

  constructor(driver, options) {
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

  async getDynamoDBResultPromise(resultListKey) {
    return this.queue.query(resultListKey)
      .then((res) => {
        return res;
      })
  }

  async getResultBlocking(queryKey) {
    const resultListKey = this.resultListKey(queryKey);

    console.log('## CHECK IF EXISTS');
    console.log('## KEY:', this.queriesDefKey());
    console.log('## QUERY KEY:', this.redisHash(queryKey));

    // Check if queryKey is active query
    const exists = await this.queue.get({
      key: this.queriesDefKey(),
      queryKey: this.redisHash(queryKey)
    })

    console.log('EXISTS');
    console.log(exists);

    if (!exists || !exists.Item) {
      return this.getResult(queryKey);
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

  async getResult(queryKey) {
    const result = await this.queue.get({ key: this.resultListKey(queryKey), queryKey: this.redisHash(queryKey) });
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

  /**
   * 
   * @param {number} keyScore 
   * @param {string} queryKey 
   * @param {number} time 
   * @param {string} queryHandler 
   * @param {string} query 
   * @param {number} priority 
   * @param {any} options 
   */
  async addToQueue(keyScore, queryKey, time, queryHandler, query, priority, options) {
    const transactionOptions = {
      TransactItems: [
        {
          Update: this.queue.updateParams({
            key: this.toProcessRedisKey(),
            queryKey: this.redisHash(queryKey),
            keyScore,
            inserted: time
          })
        },
        {
          Update: this.queue.updateParams({
            key: this.recentRedisKey(),
            queryKey: this.redisHash(queryKey),
            inserted: time,
          })
        },
        {
          Update: this.queue.updateParams({
            key: this.queriesDefKey(),
            queryKey: this.redisHash(queryKey),
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

    await this.executeTransactWrite(transactionOptions);

    let queueSize = undefined;
    const queueSizeResult = await this.queueSize.get({ key: this.queueSizeRedisKey(), sk: QUEUE_SIZE_SORT_KEY });
    if (queueSizeResult && queueSizeResult.Item) {
      queueSize = queueSizeResult.Item.size;
    }

    return [1, 1, 1, queueSize];
  }

  async getToProcessQueries() {
    const queriesResult = await this.queue.query(
      this.toProcessRedisKey(), // partition key
    );
    console.log(queriesResult);

    return queriesResult.Items;
  }

  async getActiveQueries() {
    const activeQueriesResult = await this.queue.query(this.activeRedisKey());
    console.log(activeQueriesResult);

    return activeQueriesResult.Items;
  }

  async getQueryAndRemove(queryKey) {
    const redisHash = this.redisHash(queryKey);

    const getQueryResult = await this.queue.get({
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
    return [JSON.parse(getQueryResult.Item), 'transactionResult']
  }

  async setResultAndRemoveQuery(queryKey, executionResult, processingId) {
    const redisHash = this.redisHash(queryKey);

    // await this.redisClient.watchAsync(this.queryProcessingLockKey(queryKey));
    //   const currentProcessId = await this.redisClient.getAsync(this.queryProcessingLockKey(queryKey));
    //   if (processingId !== currentProcessId) {
    //     return false;
    //   }

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

    return await this.executeTransactWrite(transactionOptions);
  }

  async getOrphanedQueries() {
    const orphanedTime = new Date().getTime() - this.orphanedTimeout * 1000;
    const orphanedQueriesResult = await this.queue.query(
      this.recentRedisKey(),
      {
        limit: 100, // limit to 100 items - TODO: validate this number
        index: 'GSI1', // query the GSI1 secondary index
        lt: orphanedTime // GSI1sk (inserted) is less than orphaned time
      }
    )

    const queryKeys = orphanedQueriesResult.Items ? orphanedQueriesResult.Items.map(item => item.queryKey) : [];
    return queryKeys;
  }

  async getStalledQueries() {
    const stalledTime = new Date().getTime() - this.heartBeatTimeout * 1000;
    const stalledQueriesResult = await this.queue.query(
      this.heartBeatRedisKey(),
      {
        limit: 100, // limit to 100 items - TODO: validate this number
        index: 'GSI1', // query the GSI1 secondary index
        lt: stalledTime // GSI1sk (inserted) is less than stalled time
      }
    )

    const queryKeys = stalledQueriesResult.Items ? stalledQueriesResult.Items.map(item => item.queryKey) : [];
    return queryKeys;
  }

  async getQueryStageState(onlyKeys) {
    // DynamoDB does NOT support transactional queries
    const activeResult = await this.queue.query(this.activeRedisKey());
    const active = activeResult ? activeResult.Items : [];

    const toProcessResult = await this.queue.query(this.toProcessRedisKey());
    const toProcess = toProcessResult ? toProcessResult.Items : [];

    let allQueryDefs = undefined;
    if (!onlyKeys) {
      const queriesResult = await this.queue.query(this.queriesDefKey());
      allQueryDefs = queriesResult ? queriesResult.Items : [];
    }

    // const [active, toProcess, allQueryDefs] = await request.execAsync();
    return [active, toProcess, R.map(q => JSON.parse(q.value), allQueryDefs || {})];
  }

  async getQueryDef(queryKey) {
    const queryDefResult = await this.queue.get({
      key: this.queriesDefKey(),
      queryKey: this.redisHash(queryKey)
    })

    return queryDefResult && JSON.parse(queryDefResult.Item.value);
  }

  updateHeartBeat(queryKey) {
    return this.queue.update({
      key: this.heartBeatRedisKey(),
      queryKey: this.redisHash(queryKey),
      inserted: new Date().getTime()
    });
  }

  /**
   * Increments the processing id by 1 and returns the value
   */
  async getNextProcessingId() {
    const updateResult = await this.processingCounter.update({
      key: this.processingIdKey(),
      sk: PROCESSING_COUNTER_SORT_KEY,
      id: { $add: 1 }, // increment id size by 1
    }, {
      returnValues: 'updated_new'
    })

    const id = updateResult.Attributes.id;
    return id && id.toString();
  }

  async retrieveForProcessing(queryKey, processingId) {
    const lockKey = this.queryProcessingLockKey(queryKey);

    let lockAcquired = false;
    const getLockResult = await this.queue.get({
      key: lockKey,
      queryKey: this.redisHash(queryKey)
    })

    if (!getLockResult || !getLockResult.Item) {
      await this.queue.update({
        key: lockKey,
        queryKey: this.redisHash(queryKey),
        value: processingId
      })
      lockAcquired = true;
    } else {
      return null;
    }

    // Query active keys based on concurrency
    const queryActiveToRemoveResult = await this.queue.query(
      this.activeRedisKey(),
      {
        limit: this.concurrency,
        index: 'GSI1' // Orders by GSIsk which is inserted time
      }
    )

    let activeUpdateTransactionOptions = {
      TransactItems: []
    }

    // If we already have this.concurrency amount of items, remove them
    if (queryActiveToRemoveResult.Items.length >= this.concurrency) {
      for (const query of queryActiveToRemoveResult.Items) {
        const toRemove = {
          Delete: this.queue.deleteParams({
            key: this.activeRedisKey(),
            queryKey: query.queryKey
          })
        }

        activeUpdateTransactionOptions.TransactItems.push(toRemove);
      }
    }

    // Add the active redis processing id and querykey
    const addActiveQuery = {
      Put: this.queue.putParams({
        key: this.activeRedisKey(),
        queryKey: this.redisHash(queryKey),
        inserted: new Date().getTime()
      })
    };
    activeUpdateTransactionOptions.TransactItems.push(addActiveQuery);

    // Execute transaction
    await this.executeTransactWrite(activeUpdateTransactionOptions);
    const added = 1;

    // Query active -> concurrency limit
    const queryActiveResult = await this.queue.query(
      this.activeRedisKey(),
      {
        limit: this.concurrency,
        index: 'GSI1' // Orders by GSIsk which is inserted time
      }
    )

    // Get number of members in toProcess (queueSize)
    // Get the query to process
    const getItemsTransactionOptions = {
      TransactItems: [
        { Get: this.queueSize.getParams({ key: this.queueSizeRedisKey(), sk: QUEUE_SIZE_SORT_KEY }) },
        { Get: this.queue.getParams({ key: this.queriesDefKey(), queryKey: this.redisHash(queryKey) }) }
      ]
    }
    const getTransactionResult = await this.executeTransactGet(getItemsTransactionOptions);
    const queueSize = getTransactionResult.Responses[0].Item.size ?? undefined;
    const queryData = JSON.parse(getTransactionResult.Responses[1].Item.value);

    // Add the heartbeat
    await this.queue.put({
      key: this.heartBeatRedisKey(),
      queryKey: this.redisHash(queryKey),
      inserted: new Date().getTime()
    })

    return [
      added, null, queryActiveResult.Items, queueSize, queryData, lockAcquired
    ]; // TODO nulls
  }

  async freeProcessingLock(queryKey, processingId, activated) {
    const lockKey = this.queryProcessingLockKey(queryKey);
    const currentProcessIdResult = await this.queue.get({
      key: lockKey,
      queryKey: queryKey
    })

    if (currentProcessIdResult
      && currentProcessIdResult.Item
      && currentProcessIdResult.Item.value === processingId.toString()) {
      const removeTransaction = {
        TransactItems: [
          { Delete: this.queue.deleteParams({ key: lockKey, queryKey: this.redisHash(queryKey) }) }
        ]
      }

      if (activated) {
        removeTransaction.TransactItems.push({
          Delete: this.queue.deleteParams({ key: this.activeRedisKey(), queryKey: this.redisHash(queryKey) })
        })
      }

      await this.executeTransactWrite(removeTransaction);
      return null;
    }

    return currentProcessIdResult.Item.value;
  }

  async optimisticQueryUpdate(queryKey, toUpdate, processingId) {
    let query = await this.getQueryDef(queryKey);
    for (let i = 0; i < 10; i++) {
      if (query) {

        // Check for lock
        const currentProcessIdResult = await this.queue.get({
          key: this.queryProcessingLockKey(queryKey),
          queryKey: this.redisHash(queryKey)
        })

        if (currentProcessIdResult.Item.value !== processingId.toString()) {
          return false;
        }

        const updateResult = await this.queue.update({
          key: this.queriesDefKey(),
          queryKey: this.redisHash(queryKey)
        }, { returnValues: 'all_old' });

        const beforeUpdate = JSON.parse(updateResult.Attributes.value);
        if (JSON.stringify(query) === JSON.stringify(beforeUpdate)) {
          return true;
        }

        query = beforeUpdate;
      }

      throw new Error(`Can't update ${queryKey} with ${JSON.stringify(toUpdate)}`);
    }
  }

  // https://github.com/aws/aws-sdk-js/issues/2464#issuecomment-503524701
  executeTransactWrite(params) {
    const transactionRequest = this.table.DocumentClient.transactWrite(params);
    return this.__executeTransaction(transactionRequest);
  }

  executeTransactGet(params) {
    const transactionRequest = this.table.DocumentClient.transactGet(params);
    return this.__executeTransaction(transactionRequest);
  }

  __executeTransaction(transactionRequest) {
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
          console.error('Error performing transactGet', { cancellationReasons, err });
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
  options;

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
