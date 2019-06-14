const crypto = require('crypto');
const R = require('ramda');
const RedisCacheDriver = require('./RedisCacheDriver');
const LocalCacheDriver = require('./LocalCacheDriver');

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
      const match = (table.table_name || table.TABLE_NAME).match(/(.+)_(.+)_(.+)_(.+)/);
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
    this.cacheDriver = preAggregations.cacheDriver;
    this.externalDriverFactory = preAggregations.externalDriverFactory;
  }

  async tablesFromCache(preAggregation, forceRenew) {
    let tables = forceRenew ? null : await this.cacheDriver.get(this.tablesRedisKey(preAggregation));
    if (!tables) {
      if (this.fetchTablesPromise) {
        tables = await this.fetchTablesPromise;
      } else {
        this.fetchTablesPromise = this.fetchTables(preAggregation);
        try {
          tables = await this.fetchTablesPromise;
        } finally {
          this.fetchTablesPromise = null;
        }
      }
    }
    return tables;
  }

  async fetchTables(preAggregation) {
    if (preAggregation.external && !this.externalDriverFactory) {
      throw new Error(`externalDriverFactory should be set in order to use external pre-aggregations`);
    }
    const client = preAggregation.external ?
      await this.externalDriverFactory() :
      await this.driverFactory();
    const newTables = await client.getTablesQuery(preAggregation.preAggregationsSchema);
    await this.cacheDriver.set(
      this.tablesRedisKey(preAggregation),
      newTables,
      this.preAggregations.options.preAggregationsSchemaCacheExpire || 60 * 60
    );
    return newTables;
  }

  tablesRedisKey(preAggregation) {
    return `SQL_PRE_AGGREGATIONS_TABLES_${this.redisPrefix}${preAggregation.external ? '_EXT' : ''}`;
  }

  async getTablesQuery(preAggregation) {
    if (!this.tables) {
      this.tables = await this.tablesFromCache(preAggregation);
    }
    return this.tables;
  }

  async getVersionEntries(preAggregation) {
    if (!this.versionEnries) {
      this.versionEnries = tablesToVersionEntries(
        preAggregation.preAggregationsSchema,
        await this.getTablesQuery(preAggregation)
      );
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
      );
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

  async reset(preAggregation) {
    this.tables = undefined;
    this.queryStageState = undefined;
    this.versionEnries = undefined;
    await this.tablesFromCache(preAggregation, true);
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
    this.externalDriverFactory = preAggregations.externalDriverFactory;
  }

  async loadPreAggregation() {
    const invalidationKeys = await Promise.all(
      (this.preAggregation.invalidateKeyQueries || [])
        .map(keyQuery => this.loadCache.keyQueryResult(keyQuery))
    );
    const contentVersion = version([this.preAggregation.loadSql, invalidationKeys]);
    const structureVersion = version(this.preAggregation.loadSql);

    const versionEntries = await this.loadCache.getVersionEntries(this.preAggregation);

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
      const client = this.preAggregation.external ?
        await this.externalDriverFactory() :
        await this.driverFactory();
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
      await this.loadCache.reset(this.preAggregation);
      return this.targetTableName(
        getVersionEntryByContentVersion(
          await this.loadCache.getVersionEntries(this.preAggregation)
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
      let queryPromise = null;
      const refreshImpl = async () => {
        if (this.preAggregation.external) { // TODO optimize
          await client.createSchemaIfNotExists(this.preAggregation.preAggregationsSchema);
        }
        queryPromise = client.loadPreAggregationIntoTable(
          this.targetTableName(newVersionEntry),
          QueryCache.replacePreAggregationTableNames(loadSql, this.preAggregationsTablesToTempTables)
            .replace(
              this.preAggregation.tableName,
              this.targetTableName(newVersionEntry)
            ),
          params
        );
        await queryPromise;
        if (this.preAggregation.external) {
          await this.loadExternalPreAggregation(client, newVersionEntry);
        }
        await this.loadCache.reset(this.preAggregation);
        await this.dropOrphanedTables(client, this.targetTableName(newVersionEntry));
        if (!this.preAggregation.external) {
          await this.loadCache.reset(this.preAggregation);
        }
      };

      const resultPromise = refreshImpl();
      resultPromise.cancel = () => queryPromise.cancel(); // TODO cancel for external upload
      return resultPromise;
    };
  }

  async loadExternalPreAggregation(client, newVersionEntry) {
    if (!client.downloadTable) {
      throw new Error(`Can't load external pre-aggregation: source driver doesn't support downloadTable()`);
    }
    const table = this.targetTableName(newVersionEntry);
    const tableData = await client.downloadTable(table);
    const columns = await client.tableColumnTypes(table);
    const externalDriver = await this.externalDriverFactory();
    if (!externalDriver.uploadTable) {
      throw new Error(`Can't load external pre-aggregation: destination driver doesn't support uploadTable()`);
    }
    await externalDriver.uploadTable(table, columns, tableData);
    await this.loadCache.reset(this.preAggregation);
    await this.dropOrphanedTables(externalDriver, table);
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
      .map(t => `${this.preAggregation.preAggregationsSchema}.${t.table_name || t.TABLE_NAME}`)
      .filter(t => tablesToSave.indexOf(t) === -1);
    this.logger('Dropping orphaned tables', { tablesToDrop: JSON.stringify(toDrop) });
    await Promise.all(toDrop.map(table => client.dropTable(table)));
  }

  flushUsedTables() {
    this.preAggregations.tablesUsedInQuery = R.filter(
      timeStamp => new Date().getTime() - timeStamp.getTime() < 10 * 60 * 1000,
      this.preAggregations.tablesUsedInQuery
    );
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
    this.cacheDriver = options.cacheAndQueueDriver === 'redis' ?
      new RedisCacheDriver() :
      new LocalCacheDriver();
    this.externalDriverFactory = options.externalDriverFactory;
  }

  loadAllPreAggregationsIfNeeded(queryBody) {
    const preAggregations = queryBody.preAggregations || [];
    const loadCache = new PreAggregationLoadCache(this.redisPrefix, this.driverFactory, this.queryCache, this);
    return preAggregations.map(p => (preAggregationsTablesToTempTables) => {
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
      const preAggregationPromise = () => loader.loadPreAggregation().then(tempTableName => {
        this.tablesUsedInQuery[tempTableName] = new Date();
        return [p.tableName, tempTableName];
      });
      return preAggregationPromise().then(res => preAggregationsTablesToTempTables.concat([res]));
    }).reduce((promise, fn) => promise.then(fn), Promise.resolve([]));
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
      }, {
        concurrency: 1,
        logger: this.logger,
        cacheAndQueueDriver: this.options.cacheAndQueueDriver,
        ...this.options.queueOptions
      });
    }
    return this.queue;
  }

  static preAggregationQueryCacheKey(preAggregation) {
    return preAggregation.tableName;
  }
}

module.exports = PreAggregations;
