const crypto = require('crypto');
const R = require('ramda');
const redis = require('redis');

const QueryCache = require('./QueryCache');
const ContinueWaitError = require('./ContinueWaitError');

function version(cacheKey) {
  let result = '';
  const hashCharset = 'abcdefghijklmnopqrstuvwxyz012345';
  const digestBuffer = crypto.createHash('md5').update(JSON.stringify(cacheKey)).digest();

  let residue = 0;
  let shiftCounter = 0;
  for (let i = 0; i < 5; i++) {
    const byte = digestBuffer.readUInt8(i);
    shiftCounter += 8;
    residue = (byte << (shiftCounter - 8)) | residue;
    while (residue >> 5) {
      result += hashCharset.charAt(residue % 32);
      shiftCounter -= 5;
      residue = residue >> 5;
    }
  }
  result += hashCharset.charAt(residue % 32);
  return result;
}

const tablesToVersionEntries = (schema, tables) => {
  return R.sortBy(
    table => -table.last_updated_at,
    tables.map(table => {
      const match = table.table_name.match(/(.+)_(.+)_(.+)_(.+)/);
      if (match) {
        return {
          table_name: `${schema}.${match[1]}`,
          content_version: match[2],
          structure_version: match[3],
          last_updated_at: parseInt(match[4], 10)
        }
      }
    }).filter(R.identity)
  )
};

class PreAggregationLoadCache {
  constructor(redisPrefix, clientFactory, queryCache, preAggregations) {
    this.redisPrefix = redisPrefix;
    this.driverFactory = clientFactory;
    this.queryCache = queryCache;
    this.preAggregations = preAggregations;
    this.queryResults = {};
    this.redisClient = preAggregations.redisClient;
  }

  async tablesFromCache(schema) {
    let tables = JSON.parse(await this.redisClient.getAsync(this.tablesRedisKey()));
    if (!tables) {
      const client = await this.driverFactory();
      tables = await client.getTablesQuery(schema);
      await this.redisClient.setAsync(this.tablesRedisKey(), JSON.stringify(tables), 'EX', 120);
    }
    return tables;
  }

  tablesRedisKey() {
    return `SQL_PRE_AGGREGATIONS_TABLES_${this.redisPrefix}`;
  }

  async getTablesQuery(schema) {
    if (!this.tables) {
      this.tables = await this.tablesFromCache(schema);
    }
    return this.tables;
  }

  async getVersionEntries(schema) {
    if (!this.versionEnries) {
      this.versionEnries = tablesToVersionEntries(schema, await this.getTablesQuery(schema));
    }
    return this.versionEnries;
  }

  async keyQueryResult(keyQuery) {
    if (!this.queryResults[this.queryCache.queryRedisKey(keyQuery)]) {
      this.queryResults[this.queryCache.queryRedisKey(keyQuery)] = await this.queryCache.cacheQueryResult(
        Array.isArray(keyQuery) ? keyQuery[0] : keyQuery,
        Array.isArray(keyQuery) ? keyQuery[1] : [],
        keyQuery,
        60 * 60,
        { renewalThreshold: 5 * 60, renewalKey: keyQuery }
      )
    }
    return this.queryResults[this.queryCache.queryRedisKey(keyQuery)];
  }

  async getQueryStage(stageQueryKey) {
    const queue = this.preAggregations.getQueue();
    if (!this.queryStageState) {
      this.queryStageState = await queue.fetchQueryStageState();
    }
    return queue.getQueryStage(stageQueryKey, undefined, this.queryStageState);
  }

  async reset() {
    this.tables = undefined;
    this.queryStageState = undefined;
    this.versionEnries = undefined;
    await this.redisClient.delAsync(this.tablesRedisKey());
  }
}

class PreAggregationLoader {
  constructor(
    redisPrefix,
    clientFactory,
    logger,
    queryCache,
    preAggregations,
    preAggregation,
    preAggregationsTablesToTempTables,
    loadCache,
    options
  ) {
    options = options || {};
    this.redisPrefix = redisPrefix;
    this.driverFactory = clientFactory;
    this.logger = logger;
    this.queryCache = queryCache;
    this.preAggregations = preAggregations;
    this.preAggregation = preAggregation;
    this.preAggregationsTablesToTempTables = preAggregationsTablesToTempTables;
    this.loadCache = loadCache;
    this.waitForRenew = options.waitForRenew;
  }

  async loadPreAggregation() {
    const invalidationKeys = await Promise.all(
      (this.preAggregation.invalidateKeyQueries || []).map(keyQuery =>
        this.loadCache.keyQueryResult(keyQuery)
      )
    );
    const contentVersion = version([this.preAggregation.loadSql, invalidationKeys]);
    const structureVersion = version(this.preAggregation.loadSql);

    const versionEntries = await this.loadCache.getVersionEntries(this.preAggregation.preAggregationsSchema);

    const getVersionEntryByContentVersion = (versionEntries) => versionEntries.find(
      v => v.table_name === this.preAggregation.tableName && v.content_version === contentVersion
    );

    const versionEntryByContentVersion = getVersionEntryByContentVersion(versionEntries);
    if (versionEntryByContentVersion) {
      return this.targetTableName(versionEntryByContentVersion);
    }

    if (
      !this.waitForRenew &&
      await this.loadCache.getQueryStage(PreAggregations.preAggregationQueryCacheKey(this.preAggregation))
    ) {
      const versionEntryByStructureVersion = versionEntries.find(
        v => v.table_name === this.preAggregation.tableName && v.structure_version === structureVersion
      );
      if (versionEntryByStructureVersion) {
        return this.targetTableName(versionEntryByStructureVersion);
      }
    }

    if (!versionEntries.length) {
      const client = await this.driverFactory();
      await client.createSchemaIfNotExists(this.preAggregation.preAggregationsSchema);
    }
    const versionEntry = versionEntries.find(e => e.table_name === this.preAggregation.tableName); // TODO can be array instead of last
    const newVersionEntry = {
      table_name: this.preAggregation.tableName,
      structure_version: structureVersion,
      content_version: contentVersion,
      last_updated_at: new Date().getTime()
    };

    const mostRecentTargetTableName = async () => {
      await this.loadCache.reset();
      return this.targetTableName(
        getVersionEntryByContentVersion(
          await this.loadCache.getVersionEntries(this.preAggregation.preAggregationsSchema)
        )
      );
    };

    if (versionEntry) {
      if (versionEntry.structure_version !== newVersionEntry.structure_version) {
        this.logger('Invalidating pre-aggregation structure', { preAggregation: this.preAggregation });
        await this.executeInQueue(invalidationKeys, 10, newVersionEntry);
        return mostRecentTargetTableName();
      } else if (versionEntry.content_version !== newVersionEntry.content_version) {
        if (this.waitForRenew) {
          this.logger('Waiting for pre-aggregation renew', { preAggregation: this.preAggregation });
          await this.executeInQueue(invalidationKeys, 0, newVersionEntry);
          return mostRecentTargetTableName();
        } else {
          if (
            this.preAggregations.refreshErrors[newVersionEntry.table_name] &&
            this.preAggregations.refreshErrors[newVersionEntry.table_name][newVersionEntry.content_version] &&
            this.preAggregations.refreshErrors[newVersionEntry.table_name][newVersionEntry.content_version].counter > 10) {
            throw this.preAggregations.refreshErrors[newVersionEntry.table_name][newVersionEntry.content_version].error;
          } else {
            this.scheduleRefresh(invalidationKeys, newVersionEntry);
          }
        }
      }
    } else {
      this.logger('Creating pre-aggregation from scratch', { preAggregation: this.preAggregation });
      await this.executeInQueue(invalidationKeys, 10, newVersionEntry);
      return mostRecentTargetTableName();
    }
    return this.targetTableName(versionEntry);
  }

  scheduleRefresh(invalidationKeys, newVersionEntry) {
    this.logger('Refreshing pre-aggregation content', { preAggregation: this.preAggregation });
    this.executeInQueue(invalidationKeys, 0, newVersionEntry)
      .then(() => {
        delete this.preAggregations.refreshErrors[newVersionEntry.table_name];
      })
      .catch(e => {
        if (!(e instanceof ContinueWaitError)) {
          this.preAggregations.refreshErrors[newVersionEntry.table_name] = this.preAggregations.refreshErrors[newVersionEntry.table_name] || {};
          if (!this.preAggregations.refreshErrors[newVersionEntry.table_name][newVersionEntry.content_version]) {
            this.preAggregations.refreshErrors[newVersionEntry.table_name][newVersionEntry.content_version] = { error: e, counter: 1 };
          } else {
            this.preAggregations.refreshErrors[newVersionEntry.table_name][newVersionEntry.content_version].error = e;
            this.preAggregations.refreshErrors[newVersionEntry.table_name][newVersionEntry.content_version].counter += 1;
          }
          this.logger('Error refreshing pre-aggregation', { error: (e.stack || e), preAggregation: this.preAggregation })
        }
      })
  }

  executeInQueue(invalidationKeys, priority, newVersionEntry) {
    return this.preAggregations.getQueue().executeInQueue(
      'query',
      [this.preAggregation.loadSql, invalidationKeys],
      {
        preAggregation: this.preAggregation,
        preAggregationsTablesToTempTables: this.preAggregationsTablesToTempTables,
        newVersionEntry
      },
      priority,
      { stageQueryKey: PreAggregations.preAggregationQueryCacheKey(this.preAggregation) }
    );
  }

  targetTableName(versionEntry) {
    return `${versionEntry.table_name}_${versionEntry.content_version}_${versionEntry.structure_version}_${versionEntry.last_updated_at}`;
  }

  refresh(newVersionEntry) {
    return (client) => {
      const [loadSql, params] =
        Array.isArray(this.preAggregation.loadSql) ? this.preAggregation.loadSql : [this.preAggregation.loadSql, []];
      const queryPromise = client.loadPreAggregationIntoTable(
        this.targetTableName(newVersionEntry),
        QueryCache.replacePreAggregationTableNames(loadSql, this.preAggregationsTablesToTempTables)
          .replace(
            this.preAggregation.tableName,
            this.targetTableName(newVersionEntry)
          ),
        params
      );
      const resultPromise = queryPromise
        .then(() => this.dropOrphanedTables(client, this.targetTableName(newVersionEntry)));
      resultPromise.cancel = queryPromise.cancel;
      return resultPromise;
    }
  }

  async dropOrphanedTables(client, justCreatedTable) {
    this.flushUsedTables();
    const actualTables = await client.getTablesQuery(this.preAggregation.preAggregationsSchema);
    const versionEntries = tablesToVersionEntries(this.preAggregation.preAggregationsSchema, actualTables);
    const versionEntriesToSave = R.pipe(
      R.groupBy(v => v.table_name),
      R.toPairs,
      R.map(p => p[1][0])
    )(versionEntries);
    const tablesToSave =
      Object.keys(this.preAggregations.tablesUsedInQuery)
        .concat(versionEntriesToSave.map(v => this.targetTableName(v)))
        .concat([justCreatedTable]);
    const toDrop = actualTables
      .map(t => `${this.preAggregation.preAggregationsSchema}.${t.table_name}`)
      .filter(t => tablesToSave.indexOf(t) === -1);
    this.logger('Dropping orphaned tables', { tablesToDrop: JSON.stringify(toDrop) });
    await Promise.all(toDrop.map(table => client.dropTable(table)));
    await this.loadCache.reset();
  }

  flushUsedTables() {
    this.preAggregations.tablesUsedInQuery = R.filter(
      timeStamp => new Date().getTime() - timeStamp.getTime() < 10 * 60 * 1000,
      this.preAggregations.tablesUsedInQuery
    )
  }
}

class PreAggregations {
  constructor(redisPrefix, clientFactory, logger, queryCache, options) {
    this.options = options || {};
    this.redisPrefix = redisPrefix;
    this.driverFactory = clientFactory;
    this.logger = logger;
    this.queryCache = queryCache;
    this.refreshErrors = {}; // TODO should be in redis
    this.tablesUsedInQuery = {}; // TODO should be in redis
    this.redisClient = redis.createClient(process.env.REDIS_URL);
  }

  loadAllPreAggregationsIfNeeded (queryBody) {
    const preAggregations = queryBody.preAggregations || [];
    const loadCache = new PreAggregationLoadCache(this.redisPrefix, this.driverFactory, this.queryCache, this);
    return preAggregations.map(p =>
      (preAggregationsTablesToTempTables) => {
        const loader = new PreAggregationLoader(
          this.redisPrefix,
          this.driverFactory,
          this.logger,
          this.queryCache,
          this,
          p,
          preAggregationsTablesToTempTables,
          loadCache,
          { waitForRenew: queryBody.renewQuery }
        );
        const preAggregationPromise = () => {
          return loader.loadPreAggregation().then(tempTableName => {
            this.tablesUsedInQuery[tempTableName] = new Date();
            return [p.tableName, tempTableName];
          });
        };
        return preAggregationPromise().then(res => preAggregationsTablesToTempTables.concat([res]));
      }
    ).reduce((promise, fn) => promise.then(fn), Promise.resolve([]));
  }

  getQueue() {
    if (!this.queue) {
      this.queue = QueryCache.createQueue(`SQL_PRE_AGGREGATIONS_${this.redisPrefix}`, this.driverFactory, (client, q) => {
        const { preAggregation, preAggregationsTablesToTempTables, newVersionEntry } = q;
        const loader = new PreAggregationLoader(
          this.redisPrefix,
          this.driverFactory,
          this.logger,
          this.queryCache,
          this,
          preAggregation,
          preAggregationsTablesToTempTables,
          new PreAggregationLoadCache(this.redisPrefix, this.driverFactory, this.queryCache, this)
        );
        return loader.refresh(newVersionEntry)(client);
      }, { concurrency: 1, logger: this.logger, ...this.options.queueOptions });
    }
    return this.queue;
  }

  static preAggregationQueryCacheKey(preAggregation) {
    return preAggregation.tableName;
  }
}

module.exports = PreAggregations;
