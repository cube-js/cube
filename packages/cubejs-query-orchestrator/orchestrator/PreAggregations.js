const crypto = require('crypto');
const R = require('ramda');
const { cancelCombinator } = require('../driver/utils');
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
    // eslint-disable-next-line operator-assignment,no-bitwise
    residue = (byte << (shiftCounter - 8)) | residue;
    // eslint-disable-next-line no-bitwise
    while (residue >> 5) {
      result += hashCharset.charAt(residue % 32);
      shiftCounter -= 5;
      // eslint-disable-next-line operator-assignment,no-bitwise
      residue = residue >> 5;
    }
  }
  result += hashCharset.charAt(residue % 32);
  return result;
}

const tablesToVersionEntries = (schema, tables) => R.sortBy(
  table => -table.last_updated_at,
  tables.map(table => {
    const match = (table.table_name || table.TABLE_NAME).match(/(.+)_(.+)_(.+)_(.+)/);
    if (match) {
      return {
        table_name: `${schema}.${match[1]}`,
        content_version: match[2],
        structure_version: match[3],
        last_updated_at: parseInt(match[4], 10)
      };
    }
    return null;
  }).filter(R.identity)
);

class PreAggregationLoadCache {
  constructor(redisPrefix, clientFactory, queryCache, preAggregations, options) {
    options = options || {};
    this.redisPrefix = redisPrefix;
    this.driverFactory = clientFactory;
    this.queryCache = queryCache;
    this.preAggregations = preAggregations;
    this.queryResults = {};
    this.cacheDriver = preAggregations.cacheDriver;
    this.externalDriverFactory = preAggregations.externalDriverFactory;
    this.requestId = options.requestId;
  }

  async tablesFromCache(preAggregation, forceRenew) {
    let tables = forceRenew ? null : await this.cacheDriver.get(this.tablesRedisKey(preAggregation));
    if (!tables) {
      tables = await this.preAggregations.getLoadCacheQueue().executeInQueue(
        'query',
        `Fetch tables for ${preAggregation.preAggregationsSchema}`,
        {
          preAggregation, requestId: this.requestId
        },
        0,
        { requestId: this.requestId }
      );
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
    if (!this.versionEntries) {
      this.versionEntries = tablesToVersionEntries(
        preAggregation.preAggregationsSchema,
        await this.getTablesQuery(preAggregation)
      );
    }
    return this.versionEntries;
  }

  async keyQueryResult(keyQuery, waitForRenew, priority, renewalThreshold) {
    if (!this.queryResults[this.queryCache.queryRedisKey(keyQuery)]) {
      this.queryResults[this.queryCache.queryRedisKey(keyQuery)] = await this.queryCache.cacheQueryResult(
        Array.isArray(keyQuery) ? keyQuery[0] : keyQuery,
        Array.isArray(keyQuery) ? keyQuery[1] : [],
        keyQuery,
        60 * 60,
        {
          renewalThreshold:
            this.queryCache.options.refreshKeyRenewalThreshold ||
            renewalThreshold ||
            2 * 60,
          renewalKey: keyQuery,
          waitForRenew,
          priority,
          requestId: this.requestId
        }
      );
    }
    return this.queryResults[this.queryCache.queryRedisKey(keyQuery)];
  }

  hasKeyQueryResult(keyQuery) {
    return !!this.queryResults[this.queryCache.queryRedisKey(keyQuery)];
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
    this.versionEntries = undefined;
    await this.cacheDriver.remove(this.tablesRedisKey(preAggregation));
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
    this.requestId = options.requestId;
    this.structureVersionPersistTime = preAggregations.structureVersionPersistTime;
  }

  async loadPreAggregation() {
    const notLoadedKey = (this.preAggregation.invalidateKeyQueries || [])
      .find(keyQuery => !this.loadCache.hasKeyQueryResult(keyQuery));
    if (notLoadedKey && !this.waitForRenew) {
      const structureVersion = this.structureVersion();

      const getVersionsStarted = new Date();
      const versionEntries = await this.loadCache.getVersionEntries(this.preAggregation);
      this.logger('Load PreAggregations Tables', {
        preAggregation: this.preAggregation,
        requestId: this.requestId,
        duration: (new Date().getTime() - getVersionsStarted.getTime())
      });

      const versionEntryByStructureVersion = versionEntries.find(
        v => v.table_name === this.preAggregation.tableName && v.structure_version === structureVersion
      );
      if (versionEntryByStructureVersion) {
        this.loadPreAggregationWithKeys().catch(e => {
          if (!(e instanceof ContinueWaitError)) {
            this.logger('Error loading pre-aggregation', {
              error: (e.stack || e),
              preAggregation: this.preAggregation,
              requestId: this.requestId
            });
          }
        });
        return this.targetTableName(versionEntryByStructureVersion);
      } else {
        return this.loadPreAggregationWithKeys();
      }
    } else {
      return {
        targetTableName: await this.loadPreAggregationWithKeys(),
        refreshKeyValues: await this.getInvalidationKeyValues()
      };
    }
  }

  async loadPreAggregationWithKeys() {
    const invalidationKeys = await this.getInvalidationKeyValues();
    const contentVersion = this.contentVersion(invalidationKeys);
    const structureVersion = this.structureVersion();

    const versionEntries = await this.loadCache.getVersionEntries(this.preAggregation);

    const getVersionEntryByContentVersion = (entries) => entries.find(
      v => v.table_name === this.preAggregation.tableName && v.content_version === contentVersion
    );

    const versionEntryByContentVersion = getVersionEntryByContentVersion(versionEntries);
    if (versionEntryByContentVersion) {
      return this.targetTableName(versionEntryByContentVersion);
    }

    // TODO this check can be redundant due to structure version is already checked in loadPreAggregation()
    if (
      !this.waitForRenew &&
      // eslint-disable-next-line no-use-before-define
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
    // TODO can be array instead of last
    const versionEntry = versionEntries.find(e => e.table_name === this.preAggregation.tableName);
    const newVersionEntry = {
      table_name: this.preAggregation.tableName,
      structure_version: structureVersion,
      content_version: contentVersion,
      last_updated_at: new Date().getTime()
    };

    const mostRecentTargetTableName = async () => {
      await this.loadCache.reset(this.preAggregation);
      const lastVersion = getVersionEntryByContentVersion(
        await this.loadCache.getVersionEntries(this.preAggregation)
      );
      if (!lastVersion) {
        throw new Error(`Pre-aggregation table is not found for ${this.preAggregation.tableName} after it was successfully created. It usually means database silently truncates table names due to max name length.`);
      }
      return this.targetTableName(lastVersion);
    };

    if (versionEntry) {
      if (versionEntry.structure_version !== newVersionEntry.structure_version) {
        this.logger('Invalidating pre-aggregation structure', {
          preAggregation: this.preAggregation,
          requestId: this.requestId,
          queryKey: this.preAggregationQueryKey(invalidationKeys),
          newVersionEntry
        });
        await this.executeInQueue(invalidationKeys, this.priority(10), newVersionEntry);
        return mostRecentTargetTableName();
      } else if (versionEntry.content_version !== newVersionEntry.content_version) {
        if (this.waitForRenew) {
          this.logger('Waiting for pre-aggregation renew', {
            preAggregation: this.preAggregation,
            requestId: this.requestId,
            queryKey: this.preAggregationQueryKey(invalidationKeys),
            newVersionEntry
          });
          await this.executeInQueue(invalidationKeys, this.priority(0), newVersionEntry);
          return mostRecentTargetTableName();
        } else {
          this.scheduleRefresh(invalidationKeys, newVersionEntry);
        }
      }
    } else {
      this.logger('Creating pre-aggregation from scratch', {
        preAggregation: this.preAggregation,
        requestId: this.requestId,
        queryKey: this.preAggregationQueryKey(invalidationKeys),
        newVersionEntry
      });
      await this.executeInQueue(invalidationKeys, this.priority(10), newVersionEntry);
      return mostRecentTargetTableName();
    }
    return this.targetTableName(versionEntry);
  }

  contentVersion(invalidationKeys) {
    return version(
      this.preAggregation.indexesSql && this.preAggregation.indexesSql.length ?
        [this.preAggregation.loadSql, this.preAggregation.indexesSql, invalidationKeys] :
        [this.preAggregation.loadSql, invalidationKeys]
    );
  }

  structureVersion() {
    return version(
      this.preAggregation.indexesSql && this.preAggregation.indexesSql.length ?
        [this.preAggregation.loadSql, this.preAggregation.indexesSql] :
        this.preAggregation.loadSql
    );
  }

  priority(defaultValue) {
    return this.preAggregation.priority != null ? this.preAggregation.priority : defaultValue;
  }

  getInvalidationKeyValues() {
    return Promise.all(
      (this.preAggregation.invalidateKeyQueries || [])
        .map(
          (keyQuery, i) => this.loadCache.keyQueryResult(
            keyQuery,
            this.waitForRenew,
            this.priority(10),
            (this.preAggregation.refreshKeyRenewalThresholds || [])[i]
          )
        )
    );
  }

  scheduleRefresh(invalidationKeys, newVersionEntry) {
    this.logger('Refreshing pre-aggregation content', {
      preAggregation: this.preAggregation,
      requestId: this.requestId,
      queryKey: this.preAggregationQueryKey(invalidationKeys),
      newVersionEntry
    });
    this.executeInQueue(invalidationKeys, this.priority(0), newVersionEntry)
      .catch(e => {
        if (!(e instanceof ContinueWaitError)) {
          this.logger('Error refreshing pre-aggregation', {
            error: (e.stack || e), preAggregation: this.preAggregation, requestId: this.requestId
          });
        }
      });
  }

  executeInQueue(invalidationKeys, priority, newVersionEntry) {
    return this.preAggregations.getQueue().executeInQueue(
      'query',
      this.preAggregationQueryKey(invalidationKeys),
      {
        preAggregation: this.preAggregation,
        preAggregationsTablesToTempTables: this.preAggregationsTablesToTempTables,
        newVersionEntry,
        requestId: this.requestId,
        invalidationKeys
      },
      priority,
      // eslint-disable-next-line no-use-before-define
      { stageQueryKey: PreAggregations.preAggregationQueryCacheKey(this.preAggregation), requestId: this.requestId }
    );
  }

  preAggregationQueryKey(invalidationKeys) {
    return [this.preAggregation.loadSql, invalidationKeys];
  }

  targetTableName(versionEntry) {
    return `${versionEntry.table_name}_${versionEntry.content_version}_${versionEntry.structure_version}_${versionEntry.last_updated_at}`;
  }

  refresh(newVersionEntry, invalidationKeys) {
    return (client) => {
      let refreshStrategy = this.refreshImplStoreInSourceStrategy;
      if (this.preAggregation.external) {
        const readOnly =
          client.config && client.config.readOnly ||
          client.readOnly && (typeof client.readOnly === 'boolean' ? client.readOnly : client.readOnly());
        refreshStrategy = readOnly ?
          this.refreshImplStreamExternalStrategy : this.refreshImplTempTableExternalStrategy;
      }
      return cancelCombinator(
        saveCancelFn => refreshStrategy.bind(this)(client, newVersionEntry, saveCancelFn, invalidationKeys)
      );
    };
  }

  logExecutingSql(invalidationKeys, query, params, targetTableName, newVersionEntry) {
    this.logger('Executing Load Pre Aggregation SQL', {
      queryKey: this.preAggregationQueryKey(invalidationKeys),
      query,
      values: params,
      targetTableName,
      requestId: this.requestId,
      newVersionEntry,
    });
  }

  async refreshImplStoreInSourceStrategy(client, newVersionEntry, saveCancelFn, invalidationKeys) {
    const [loadSql, params] =
        Array.isArray(this.preAggregation.loadSql) ? this.preAggregation.loadSql : [this.preAggregation.loadSql, []];
    const targetTableName = this.targetTableName(newVersionEntry);
    const query = QueryCache.replacePreAggregationTableNames(loadSql, this.preAggregationsTablesToTempTables)
      .replace(
        this.preAggregation.tableName,
        targetTableName
      );
    this.logExecutingSql(invalidationKeys, query, params, targetTableName, newVersionEntry);
    await saveCancelFn(client.loadPreAggregationIntoTable(
      targetTableName,
      query,
      params
    ));
    await this.createIndexes(client, newVersionEntry, saveCancelFn);
    await this.loadCache.reset(this.preAggregation);
    await this.dropOrphanedTables(client, targetTableName, saveCancelFn);
    await this.loadCache.reset(this.preAggregation);
  }

  async refreshImplTempTableExternalStrategy(client, newVersionEntry, saveCancelFn, invalidationKeys) {
    const [loadSql, params] =
        Array.isArray(this.preAggregation.loadSql) ? this.preAggregation.loadSql : [this.preAggregation.loadSql, []];
    await client.createSchemaIfNotExists(this.preAggregation.preAggregationsSchema);
    const targetTableName = this.targetTableName(newVersionEntry);
    const query = QueryCache.replacePreAggregationTableNames(loadSql, this.preAggregationsTablesToTempTables)
      .replace(
        this.preAggregation.tableName,
        targetTableName
      );
    this.logExecutingSql(invalidationKeys, query, params, targetTableName, newVersionEntry);
    await saveCancelFn(client.loadPreAggregationIntoTable(
      targetTableName,
      query,
      params
    ));
    const tableData = await this.downloadTempExternalPreAggregation(client, newVersionEntry, saveCancelFn);
    await this.uploadExternalPreAggregation(tableData, newVersionEntry, saveCancelFn);
    await this.loadCache.reset(this.preAggregation);
    await this.dropOrphanedTables(client, targetTableName, saveCancelFn);
  }

  async refreshImplStreamExternalStrategy(client, newVersionEntry, saveCancelFn, invalidationKeys) {
    const [sql, params] =
        Array.isArray(this.preAggregation.sql) ? this.preAggregation.sql : [this.preAggregation.sql, []];
    if (!client.downloadQueryResults) {
      throw new Error(`Can't load external pre-aggregation: source driver doesn't support downloadQueryResults()`);
    }

    this.logExecutingSql(invalidationKeys, sql, params, this.targetTableName(newVersionEntry), newVersionEntry);
    this.logger('Downloading external pre-aggregation via query', {
      preAggregation: this.preAggregation,
      requestId: this.requestId
    });
    const tableData = await saveCancelFn(client.downloadQueryResults(sql, params));
    await this.uploadExternalPreAggregation(tableData, newVersionEntry, saveCancelFn);
    await this.loadCache.reset(this.preAggregation);
  }

  async downloadTempExternalPreAggregation(client, newVersionEntry, saveCancelFn) {
    if (!client.downloadTable) {
      throw new Error(`Can't load external pre-aggregation: source driver doesn't support downloadTable()`);
    }
    const table = this.targetTableName(newVersionEntry);
    this.logger('Downloading external pre-aggregation', {
      preAggregation: this.preAggregation,
      requestId: this.requestId
    });
    const tableData = await saveCancelFn(client.downloadTable(table));
    tableData.types = await saveCancelFn(client.tableColumnTypes(table));
    return tableData;
  }

  async uploadExternalPreAggregation(tableData, newVersionEntry, saveCancelFn) {
    const table = this.targetTableName(newVersionEntry);
    const externalDriver = await this.externalDriverFactory();
    if (!externalDriver.uploadTable) {
      throw new Error(`Can't load external pre-aggregation: destination driver doesn't support uploadTable()`);
    }
    this.logger('Uploading external pre-aggregation', {
      preAggregation: this.preAggregation,
      requestId: this.requestId
    });
    await saveCancelFn(externalDriver.uploadTable(table, tableData.types, tableData));
    await this.createIndexes(externalDriver, newVersionEntry, saveCancelFn);
    await this.loadCache.reset(this.preAggregation);
    await this.dropOrphanedTables(externalDriver, table, saveCancelFn);
  }

  async createIndexes(driver, newVersionEntry, saveCancelFn) {
    if (!this.preAggregation.indexesSql || !this.preAggregation.indexesSql.length) {
      return;
    }
    for (let i = 0; i < this.preAggregation.indexesSql.length; i++) {
      const { sql, indexName } = this.preAggregation.indexesSql[i];
      const [query, params] = sql;
      const indexVersionEntry = {
        ...newVersionEntry,
        table_name: indexName
      };
      this.logger('Creating pre-aggregation index', {
        preAggregation: this.preAggregation,
        requestId: this.requestId,
        sql
      });
      await saveCancelFn(driver.query(
        QueryCache.replacePreAggregationTableNames(
          query,
          this.preAggregationsTablesToTempTables.concat([
            [this.preAggregation.tableName, { targetTableName: this.targetTableName(newVersionEntry) }],
            [indexName, { targetTableName: this.targetTableName(indexVersionEntry) }]
          ])
        ),
        params
      ));
    }
  }

  async dropOrphanedTables(client, justCreatedTable, saveCancelFn) {
    await this.preAggregations.addTableUsed(justCreatedTable);
    const actualTables = await client.getTablesQuery(this.preAggregation.preAggregationsSchema);
    const versionEntries = tablesToVersionEntries(this.preAggregation.preAggregationsSchema, actualTables);
    const versionEntriesToSave = R.pipe(
      R.groupBy(v => v.table_name),
      R.toPairs,
      R.map(p => p[1][0])
    )(versionEntries);

    const structureVersionsToSave = R.pipe(
      R.filter(v => new Date().getTime() - v.last_updated_at < this.structureVersionPersistTime * 1000),
      R.groupBy(v => `${v.table_name}_${v.structure_version}`),
      R.toPairs,
      R.map(p => p[1][0])
    )(versionEntries);

    const tablesToSave =
      (await this.preAggregations.tablesUsed())
        .concat(structureVersionsToSave.map(v => this.targetTableName(v)))
        .concat(versionEntriesToSave.map(v => this.targetTableName(v)))
        .concat([justCreatedTable]);
    const toDrop = actualTables
      .map(t => `${this.preAggregation.preAggregationsSchema}.${t.table_name || t.TABLE_NAME}`)
      .filter(t => tablesToSave.indexOf(t) === -1);
    this.logger('Dropping orphaned tables', {
      tablesToDrop: JSON.stringify(toDrop),
      requestId: this.requestId
    });
    await Promise.all(toDrop.map(table => saveCancelFn(client.dropTable(table))));
  }
}

class PreAggregations {
  constructor(redisPrefix, clientFactory, logger, queryCache, options) {
    this.options = options || {};
    this.redisPrefix = redisPrefix;
    this.driverFactory = clientFactory;
    this.logger = logger;
    this.queryCache = queryCache;
    this.cacheDriver = options.cacheAndQueueDriver === 'redis' ?
      new RedisCacheDriver({ pool: options.redisPool }) :
      new LocalCacheDriver();
    this.externalDriverFactory = options.externalDriverFactory;
    this.structureVersionPersistTime = options.structureVersionPersistTime || 60 * 60 * 24 * 30;
    this.usedTablePersistTime = options.usedTablePersistTime || 600;
  }

  tablesUsedRedisKey(tableName) {
    return `SQL_PRE_AGGREGATIONS_${this.redisPrefix}_TABLES_USED_${tableName}`;
  }

  async addTableUsed(tableName) {
    return this.cacheDriver.set(this.tablesUsedRedisKey(tableName), true, this.usedTablePersistTime);
  }

  async tablesUsed() {
    return (await this.cacheDriver.keysStartingWith(this.tablesUsedRedisKey('')))
      .map(k => k.replace(this.tablesUsedRedisKey(''), ''));
  }

  loadAllPreAggregationsIfNeeded(queryBody) {
    const preAggregations = queryBody.preAggregations || [];
    const loadCache = new PreAggregationLoadCache(this.redisPrefix, this.driverFactory, this.queryCache, this, {
      requestId: queryBody.requestId
    });
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
        { waitForRenew: queryBody.renewQuery, requestId: queryBody.requestId }
      );
      const preAggregationPromise = () => loader.loadPreAggregation().then(async targetTableName => {
        const usedPreAggregation = typeof targetTableName === 'string' ? { targetTableName } : targetTableName;
        await this.addTableUsed(usedPreAggregation.targetTableName);
        return [p.tableName, usedPreAggregation];
      });
      return preAggregationPromise().then(res => preAggregationsTablesToTempTables.concat([res]));
    }).reduce((promise, fn) => promise.then(fn), Promise.resolve([]));
  }

  getQueue() {
    if (!this.queue) {
      this.queue = QueryCache.createQueue(`SQL_PRE_AGGREGATIONS_${this.redisPrefix}`, this.driverFactory, (client, q) => {
        const {
          preAggregation, preAggregationsTablesToTempTables, newVersionEntry, requestId, invalidationKeys
        } = q;
        const loader = new PreAggregationLoader(
          this.redisPrefix,
          this.driverFactory,
          this.logger,
          this.queryCache,
          this,
          preAggregation,
          preAggregationsTablesToTempTables,
          new PreAggregationLoadCache(this.redisPrefix, this.driverFactory, this.queryCache, this, { requestId }),
          { requestId }
        );
        return loader.refresh(newVersionEntry, invalidationKeys)(client);
      }, {
        concurrency: 1,
        logger: this.logger,
        cacheAndQueueDriver: this.options.cacheAndQueueDriver,
        redisPool: this.options.redisPool,
        ...this.options.queueOptions
      });
    }
    return this.queue;
  }

  getLoadCacheQueue() {
    if (!this.loadCacheQueue) {
      this.loadCacheQueue = QueryCache.createQueue(`SQL_PRE_AGGREGATIONS_CACHE_${this.redisPrefix}`, this.driverFactory, (client, q) => {
        const {
          preAggregation,
          requestId
        } = q;
        const loadCache = new PreAggregationLoadCache(this.redisPrefix, this.driverFactory, this.queryCache, this,
          { requestId });
        return loadCache.fetchTables(preAggregation);
      }, {
        concurrency: 4,
        logger: this.logger,
        cacheAndQueueDriver: this.options.cacheAndQueueDriver,
        redisPool: this.options.redisPool,
        ...this.options.loadCacheQueueOptions
      });
    }
    return this.loadCacheQueue;
  }

  static preAggregationQueryCacheKey(preAggregation) {
    return preAggregation.tableName;
  }
}

module.exports = PreAggregations;
