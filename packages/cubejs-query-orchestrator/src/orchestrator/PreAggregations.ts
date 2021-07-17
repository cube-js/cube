import crypto from 'crypto';
import R from 'ramda';
import {
  addSecondsToLocalTimestamp,
  extractDate,
  FROM_PARTITION_RANGE,
  getEnv,
  inDbTimeZone,
  timeSeries,
  TO_PARTITION_RANGE,
} from '@cubejs-backend/shared';

import { cancelCombinator, SaveCancelFn } from '../driver/utils';
import { RedisCacheDriver } from './RedisCacheDriver';
import { LocalCacheDriver } from './LocalCacheDriver';
import { QueryCache, QueryTuple, QueryWithParams } from './QueryCache';
import { ContinueWaitError } from './ContinueWaitError';
import { DriverFactory, DriverFactoryByDataSource } from './DriverFactory';
import { CacheDriverInterface } from './cache-driver.interface';
import { BaseDriver, DownloadTableData, StreamOptions, UnloadOptions } from '../driver';
import { QueryQueue } from './QueryQueue';
import { DriverInterface } from '../driver/driver.interface';
import { LargeStreamWarning } from './StreamObjectsCounter';

function encodeTimeStamp(time) {
  return Math.floor(time / 1000).toString(32);
}

function decodeTimeStamp(time) {
  return parseInt(time, 32) * 1000;
}

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

function getStructureVersion(preAggregation) {
  return version(
    preAggregation.indexesSql && preAggregation.indexesSql.length ?
      [preAggregation.loadSql, preAggregation.indexesSql] :
      preAggregation.loadSql
  );
}

type VersionEntry = {
  // eslint-disable-next-line camelcase
  table_name: string,
  // eslint-disable-next-line camelcase
  content_version: string,
  // eslint-disable-next-line camelcase
  structure_version: string,
  // eslint-disable-next-line camelcase
  last_updated_at: number,
  // eslint-disable-next-line camelcase
  naming_version?: number
};

type TableCacheEntry = {
  // eslint-disable-next-line camelcase
  table_name?: string;
  TABLE_NAME?: string;
};

type QueryDateRange = [string, string];

type IndexDescription = {
  sql: QueryWithParams;
  indexName: string;
};

type PreAggregationDescription = {
  previewSql: QueryWithParams;
  timezone: string;
  indexesSql: IndexDescription[];
  invalidateKeyQueries: QueryWithParams[];
  sql: QueryWithParams;
  loadSql: QueryWithParams;
  tableName: string;
  matchedTimeDimensionDateRange: QueryDateRange;
  partitionGranularity: string;
  preAggregationStartEndQueries: [QueryWithParams, QueryWithParams];
};

const tablesToVersionEntries = (schema, tables: TableCacheEntry[]): VersionEntry[] => R.sortBy(
  table => -table.last_updated_at,
  tables.map(table => {
    const match = (table.table_name || table.TABLE_NAME).match(/(.+)_(.+)_(.+)_(.+)/);

    if (!match) {
      return null;
    }

    const entity: any = {
      table_name: `${schema}.${match[1]}`,
      content_version: match[2],
      structure_version: match[3]
    };

    if (match[4].length < 13) {
      entity.last_updated_at = decodeTimeStamp(match[4]);
      entity.naming_version = 2;
    } else {
      entity.last_updated_at = parseInt(match[4], 10);
    }

    return entity;
  }).filter(R.identity)
);

type PreAggregationLoadCacheOptions = {
  requestId?: string,
  dataSource: string
};

type VersionEntriesObj = {
  versionEntries: VersionEntry[],
  byStructure: { [key: string]: VersionEntry },
  byContent: { [key: string]: VersionEntry },
  byTableName: { [key: string]: VersionEntry },
};

class PreAggregationLoadCache {
  private redisPrefix: string;

  private driverFactory: DriverFactory;

  private queryCache: any;

  // eslint-disable-next-line no-use-before-define
  private preAggregations: PreAggregations;

  private queryResults: any;

  private cacheDriver: CacheDriverInterface;

  private externalDriverFactory: any;

  private requestId: any;

  private versionEntries: { [redisKey: string]: Promise<VersionEntriesObj> };

  private tables: { [redisKey: string]: TableCacheEntry[] };

  // TODO this is in memory cache structure as well however it depends on
  // data source only and load cache is per data source for now.
  // Make it per data source key in case load cache scope is broaden.
  private queryStageState: any;

  private dataSource: string;

  public constructor(
    redisPrefix,
    clientFactory: DriverFactory,
    queryCache,
    preAggregations,
    options: PreAggregationLoadCacheOptions = { dataSource: 'default' }
  ) {
    this.redisPrefix = `${redisPrefix}_${options.dataSource}`;
    this.dataSource = options.dataSource;
    this.driverFactory = clientFactory;
    this.queryCache = queryCache;
    this.preAggregations = preAggregations;
    this.queryResults = {};
    this.cacheDriver = preAggregations.cacheDriver;
    this.externalDriverFactory = preAggregations.externalDriverFactory;
    this.requestId = options.requestId;
    this.versionEntries = {};
    this.tables = {};
  }

  protected async tablesFromCache(preAggregation, forceRenew?) {
    let tables = forceRenew ? null : await this.cacheDriver.get(this.tablesRedisKey(preAggregation));
    if (!tables) {
      tables = await this.preAggregations.getLoadCacheQueue(this.dataSource).executeInQueue(
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

  public async fetchTables(preAggregation) {
    if (preAggregation.external && !this.externalDriverFactory) {
      throw new Error('externalDriverFactory is not provided. Please make sure @cubejs-backend/cubestore-driver is installed and use CUBEJS_DEV_MODE=true or provide Cube Store connection env variables for production usage.');
    }

    const newTables = await this.fetchTablesNoCache(preAggregation);
    await this.cacheDriver.set(
      this.tablesRedisKey(preAggregation),
      newTables,
      this.preAggregations.options.preAggregationsSchemaCacheExpire || 60 * 60
    );
    return newTables;
  }

  private async fetchTablesNoCache(preAggregation) {
    const client = preAggregation.external ?
      await this.externalDriverFactory() :
      await this.driverFactory();
    return client.getTablesQuery(preAggregation.preAggregationsSchema);
  }

  public tablesRedisKey(preAggregation) {
    return `SQL_PRE_AGGREGATIONS_TABLES_${this.redisPrefix}_${preAggregation.dataSource}${preAggregation.external ? '_EXT' : ''}`;
  }

  protected async getTablesQuery(preAggregation) {
    const redisKey = this.tablesRedisKey(preAggregation);
    if (!this.tables[redisKey]) {
      this.tables[redisKey] = this.preAggregations.options.skipExternalCacheAndQueue && preAggregation.external ?
        await this.fetchTablesNoCache(preAggregation) :
        await this.tablesFromCache(preAggregation);
    }
    return this.tables[redisKey];
  }

  private async calculateVersionEntries(preAggregation): Promise<VersionEntriesObj> {
    let versionEntries = tablesToVersionEntries(
      preAggregation.preAggregationsSchema,
      await this.getTablesQuery(preAggregation)
    );
    // It presumes strong consistency guarantees for external pre-aggregation tables ingestion
    if (!preAggregation.external) {
      // eslint-disable-next-line
      const [active, toProcess, queries] = await this.fetchQueryStageState();
      const targetTableNamesInQueue = (Object.keys(queries))
        // eslint-disable-next-line no-use-before-define
        .map(q => PreAggregations.targetTableName(queries[q].query.newVersionEntry));

      versionEntries = versionEntries.filter(
        // eslint-disable-next-line no-use-before-define
        e => targetTableNamesInQueue.indexOf(PreAggregations.targetTableName(e)) === -1
      );
    }

    const byContent: { [key: string]: VersionEntry } = {};
    const byStructure: { [key: string]: VersionEntry } = {};
    const byTableName: { [key: string]: VersionEntry } = {};

    versionEntries.forEach(e => {
      const contentKey = `${e.table_name}_${e.content_version}`;
      if (!byContent[contentKey]) {
        byContent[contentKey] = e;
      }
      const structureKey = `${e.table_name}_${e.structure_version}`;
      if (!byStructure[structureKey]) {
        byStructure[structureKey] = e;
      }
      if (!byTableName[e.table_name]) {
        byTableName[e.table_name] = e;
      }
    });

    return { versionEntries, byContent, byStructure, byTableName };
  }

  public async getVersionEntries(preAggregation): Promise<VersionEntriesObj> {
    const redisKey = this.tablesRedisKey(preAggregation);
    if (!this.versionEntries[redisKey]) {
      this.versionEntries[redisKey] = this.calculateVersionEntries(preAggregation).catch(e => {
        delete this.versionEntries[redisKey];
        throw e;
      });
    }
    return this.versionEntries[redisKey];
  }

  protected async keyQueryResult(sqlQuery: QueryWithParams, waitForRenew, priority) {
    const [query, values, queryOptions]: QueryTuple = Array.isArray(sqlQuery) ? sqlQuery : [sqlQuery, [], {}];

    if (!this.queryResults[this.queryCache.queryRedisKey([query, values])]) {
      this.queryResults[this.queryCache.queryRedisKey([query, values])] = await this.queryCache.cacheQueryResult(
        query,
        values,
        [query, values],
        60 * 60,
        {
          renewalThreshold: this.queryCache.options.refreshKeyRenewalThreshold
            || queryOptions?.renewalThreshold || 2 * 60,
          renewalKey: [query, values],
          waitForRenew,
          priority,
          requestId: this.requestId,
          dataSource: this.dataSource,
          useInMemory: true,
          external: queryOptions?.external
        }
      );
    }
    return this.queryResults[this.queryCache.queryRedisKey([query, values])];
  }

  protected hasKeyQueryResult(keyQuery) {
    return !!this.queryResults[this.queryCache.queryRedisKey(keyQuery)];
  }

  protected async getQueryStage(stageQueryKey) {
    const queue = this.preAggregations.getQueue(this.dataSource);
    await this.fetchQueryStageState(queue);
    return queue.getQueryStage(stageQueryKey, undefined, this.queryStageState);
  }

  protected async fetchQueryStageState(queue?) {
    queue = queue || this.preAggregations.getQueue(this.dataSource);
    if (!this.queryStageState) {
      this.queryStageState = await queue.fetchQueryStageState();
    }
    return this.queryStageState;
  }

  protected async reset(preAggregation) {
    await this.tablesFromCache(preAggregation, true);
    this.tables = {};
    this.queryStageState = undefined;
    this.versionEntries = {};
  }
}

type LoadPreAggregationResult = {
  targetTableName: string;
  refreshKeyValues: any[]
} | string;

export class PreAggregationLoader {
  // eslint-disable-next-line no-use-before-define
  private preAggregations: PreAggregations;

  private preAggregation: any;

  private preAggregationsTablesToTempTables: any;

  private loadCache: any;

  private waitForRenew: boolean;

  private forceBuild: boolean;

  private orphanedTimeout: number;

  private externalDriverFactory: DriverFactory;

  private requestId: string;

  private structureVersionPersistTime: any;

  private externalRefresh: boolean;

  public constructor(
    private readonly redisPrefix: string,
    private readonly driverFactory: DriverFactory,
    private readonly logger: any,
    private readonly queryCache: QueryCache,
    // eslint-disable-next-line no-use-before-define
    preAggregations: PreAggregations,
    preAggregation,
    preAggregationsTablesToTempTables,
    loadCache,
    options: any = {}
  ) {
    this.preAggregations = preAggregations;
    this.preAggregation = preAggregation;
    this.preAggregationsTablesToTempTables = preAggregationsTablesToTempTables;
    this.loadCache = loadCache;
    this.waitForRenew = options.waitForRenew;
    this.forceBuild = options.forceBuild;
    this.orphanedTimeout = options.orphanedTimeout;
    this.externalDriverFactory = preAggregations.externalDriverFactory;
    this.requestId = options.requestId;
    this.structureVersionPersistTime = preAggregations.structureVersionPersistTime;
    this.externalRefresh = options.externalRefresh;

    if (this.externalRefresh && this.waitForRenew) {
      const message = 'Invalid configuration - when externalRefresh is true, it will not perform a renew, therefore you cannot wait for it using waitForRenew.';
      if (['production', 'test'].includes(getEnv('nodeEnv'))) {
        throw new Error(message);
      } else {
        this.logger('Invalid Configuration', {
          requestId: this.requestId,
          warning: message,
        });
        this.waitForRenew = false;
      }
    }
  }

  public async loadPreAggregation(): Promise<LoadPreAggregationResult> {
    const notLoadedKey = (this.preAggregation.invalidateKeyQueries || [])
      .find(keyQuery => !this.loadCache.hasKeyQueryResult(keyQuery));
    if (notLoadedKey && !this.waitForRenew) {
      const structureVersion = getStructureVersion(this.preAggregation);

      const getVersionsStarted = new Date();
      const { byStructure } = await this.loadCache.getVersionEntries(this.preAggregation);
      this.logger('Load PreAggregations Tables', {
        preAggregation: this.preAggregation,
        requestId: this.requestId,
        duration: (new Date().getTime() - getVersionsStarted.getTime())
      });

      const versionEntryByStructureVersion = byStructure[`${this.preAggregation.tableName}_${structureVersion}`];
      if (this.externalRefresh) {
        if (!versionEntryByStructureVersion) {
          throw new Error('One or more pre-aggregation tables could not be found to satisfy that query');
        }

        // the rollups are being maintained independently of this instance of cube.js,
        // immediately return the latest data it already has
        return this.targetTableName(versionEntryByStructureVersion);
      }

      if (versionEntryByStructureVersion) {
        // this triggers an asyncronous/background load of the pre-aggregation but immediately
        // returns the latest data it already has
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
        // no rollup has been built yet - build it synchronously as part of responding to this request
        return this.loadPreAggregationWithKeys();
      }
    } else {
      // either we have no data cached for this rollup or waitForRenew is true, either way,
      // synchronously renew what data is needed so that the most current data will be returned for the current request
      return {
        targetTableName: await this.loadPreAggregationWithKeys(),
        refreshKeyValues: await this.getInvalidationKeyValues()
      };
    }
  }

  protected async loadPreAggregationWithKeys() {
    const invalidationKeys = await this.getInvalidationKeyValues();
    const contentVersion = this.contentVersion(invalidationKeys);
    const structureVersion = getStructureVersion(this.preAggregation);

    const versionEntries = await this.loadCache.getVersionEntries(this.preAggregation);

    const getVersionEntryByContentVersion = ({ byContent }: VersionEntriesObj) => byContent[`${this.preAggregation.tableName}_${contentVersion}`];

    const versionEntryByContentVersion = getVersionEntryByContentVersion(versionEntries);
    if (versionEntryByContentVersion && !this.forceBuild) {
      return this.targetTableName(versionEntryByContentVersion);
    }

    // TODO this check can be redundant due to structure version is already checked in loadPreAggregation()
    if (
      !this.waitForRenew &&
      // eslint-disable-next-line no-use-before-define
      await this.loadCache.getQueryStage(PreAggregations.preAggregationQueryCacheKey(this.preAggregation))
    ) {
      const versionEntryByStructureVersion = versionEntries.byStructure[`${this.preAggregation.tableName}_${structureVersion}`];
      if (versionEntryByStructureVersion) {
        return this.targetTableName(versionEntryByStructureVersion);
      }
    }

    if (!versionEntries.versionEntries.length) {
      const client = this.preAggregation.external ?
        await this.externalDriverFactory() :
        await this.driverFactory();
      await client.createSchemaIfNotExists(this.preAggregation.preAggregationsSchema);
    }
    // ensure we find appropriate structure version before invalidating anything
    const versionEntry =
      versionEntries.byStructure[`${this.preAggregation.tableName}_${structureVersion}`] ||
      versionEntries.byTableName[this.preAggregation.tableName];

    const newVersionEntry = {
      table_name: this.preAggregation.tableName,
      structure_version: structureVersion,
      content_version: contentVersion,
      last_updated_at: new Date().getTime(),
      naming_version: 2,
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

    if (this.forceBuild) {
      this.logger('Force build pre-aggregation', {
        preAggregation: this.preAggregation,
        requestId: this.requestId,
        queryKey: this.preAggregationQueryKey(invalidationKeys),
        newVersionEntry
      });
      await this.executeInQueue(invalidationKeys, this.priority(10), newVersionEntry);
      return mostRecentTargetTableName();
    }

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

  protected contentVersion(invalidationKeys) {
    return version(
      this.preAggregation.indexesSql && this.preAggregation.indexesSql.length ?
        [this.preAggregation.loadSql, this.preAggregation.indexesSql, invalidationKeys] :
        [this.preAggregation.loadSql, invalidationKeys]
    );
  }

  protected priority(defaultValue) {
    return this.preAggregation.priority != null ? this.preAggregation.priority : defaultValue;
  }

  protected getInvalidationKeyValues() {
    return Promise.all(
      (this.preAggregation.invalidateKeyQueries || []).map(
        (sqlQuery) => this.loadCache.keyQueryResult(sqlQuery, this.waitForRenew, this.priority(10))
      )
    );
  }

  protected scheduleRefresh(invalidationKeys, newVersionEntry) {
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

  protected async executeInQueue(invalidationKeys, priority, newVersionEntry) {
    return this.preAggregations.getQueue(this.preAggregation.dataSource).executeInQueue(
      'query',
      this.preAggregationQueryKey(invalidationKeys),
      {
        preAggregation: this.preAggregation,
        preAggregationsTablesToTempTables: this.preAggregationsTablesToTempTables,
        newVersionEntry,
        requestId: this.requestId,
        invalidationKeys,
        forceBuild: this.forceBuild,
        orphanedTimeout: this.orphanedTimeout
      },
      priority,
      // eslint-disable-next-line no-use-before-define
      { stageQueryKey: PreAggregations.preAggregationQueryCacheKey(this.preAggregation), requestId: this.requestId }
    );
  }

  protected preAggregationQueryKey(invalidationKeys) {
    return this.preAggregation.indexesSql && this.preAggregation.indexesSql.length ?
      [this.preAggregation.loadSql, this.preAggregation.indexesSql, invalidationKeys] :
      [this.preAggregation.loadSql, invalidationKeys];
  }

  protected targetTableName(versionEntry) {
    // eslint-disable-next-line no-use-before-define
    return PreAggregations.targetTableName(versionEntry);
  }

  public refresh(preAggregation: any, newVersionEntry, invalidationKeys) {
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
        saveCancelFn => refreshStrategy.bind(this)(
          client,
          newVersionEntry,
          saveCancelFn,
          preAggregation,
          invalidationKeys
        )
      );
    };
  }

  protected logExecutingSql(invalidationKeys, query, params, targetTableName, newVersionEntry) {
    this.logger(
      'Executing Load Pre Aggregation SQL',
      this.queryOptions(invalidationKeys, query, params, targetTableName, newVersionEntry)
    );
  }

  protected queryOptions(invalidationKeys, query, params, targetTableName, newVersionEntry) {
    return {
      queryKey: this.preAggregationQueryKey(invalidationKeys),
      query,
      values: params,
      targetTableName,
      requestId: this.requestId,
      newVersionEntry,
    };
  }

  protected async refreshImplStoreInSourceStrategy(
    client: DriverInterface,
    newVersionEntry,
    saveCancelFn: SaveCancelFn,
    preAggregation,
    invalidationKeys
  ) {
    const [loadSql, params] =
        Array.isArray(this.preAggregation.loadSql) ? this.preAggregation.loadSql : [this.preAggregation.loadSql, []];
    const targetTableName = this.targetTableName(newVersionEntry);
    const query = QueryCache.replacePreAggregationTableNames(loadSql, this.preAggregationsTablesToTempTables)
      .replace(
        this.preAggregation.tableName,
        targetTableName
      );
    this.logExecutingSql(invalidationKeys, query, params, targetTableName, newVersionEntry);
    // TODO move index creation to the driver
    await saveCancelFn(client.loadPreAggregationIntoTable(
      targetTableName,
      query,
      params,
      this.queryOptions(invalidationKeys, query, params, targetTableName, newVersionEntry)
    ));
    await this.createIndexes(client, newVersionEntry, saveCancelFn);
    await this.loadCache.fetchTables(this.preAggregation);
    await this.dropOrphanedTables(client, targetTableName, saveCancelFn, false);
    await this.loadCache.fetchTables(this.preAggregation);
  }

  /**
   * Strategy to copy pre-aggregation from source db (with write permissions) to external data
   */
  protected async refreshImplTempTableExternalStrategy(
    client: DriverInterface,
    newVersionEntry: VersionEntry,
    saveCancelFn: SaveCancelFn,
    preAggregation,
    invalidationKeys
  ) {
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
      params,
      this.queryOptions(invalidationKeys, query, params, targetTableName, newVersionEntry)
    ));

    const tableData = await this.downloadTempExternalPreAggregation(
      client,
      newVersionEntry,
      preAggregation,
      saveCancelFn
    );

    try {
      await this.uploadExternalPreAggregation(tableData, newVersionEntry, saveCancelFn);
    } finally {
      if (tableData.release) {
        await tableData.release();
      }
    }

    await this.loadCache.fetchTables(this.preAggregation);
    await this.dropOrphanedTables(client, targetTableName, saveCancelFn, false);
  }

  /**
   * Strategy to copy pre-aggregation from source db (for read-only permissions) to external data
   */
  protected async refreshImplStreamExternalStrategy(
    client: DriverInterface,
    newVersionEntry: VersionEntry,
    saveCancelFn,
    preAggregation,
    invalidationKeys
  ) {
    const [sql, params] =
        Array.isArray(this.preAggregation.sql) ? this.preAggregation.sql : [this.preAggregation.sql, []];

    // @todo Deprecated, BaseDriver already implements it, before remove we need to add check for factoryDriver
    if (!client.downloadQueryResults) {
      throw new Error('Can\'t load external pre-aggregation: source driver doesn\'t support downloadQueryResults()');
    }

    this.logExecutingSql(invalidationKeys, sql, params, this.targetTableName(newVersionEntry), newVersionEntry);
    this.logger('Downloading external pre-aggregation via query', {
      preAggregation: this.preAggregation,
      requestId: this.requestId
    });
    const externalDriver = await this.externalDriverFactory();
    const capabilities = externalDriver.capabilities && externalDriver.capabilities();

    const tableData = await saveCancelFn(client.downloadQueryResults(
      sql,
      params, {
        ...this.queryOptions(invalidationKeys, sql, params, this.targetTableName(newVersionEntry), newVersionEntry),
        ...capabilities,
        ...this.getStreamingOptions(),
      }
    ));

    try {
      await this.uploadExternalPreAggregation(tableData, newVersionEntry, saveCancelFn);
    } finally {
      if (tableData.release) {
        await tableData.release();
      }
    }

    await this.loadCache.fetchTables(this.preAggregation);
  }

  protected getUnloadOptions(): UnloadOptions {
    return {
      // Default: 16mb for Snowflake, Should be specified in MBs, because drivers convert it
      maxFileSize: 64
    };
  }

  protected getStreamingOptions(): StreamOptions {
    return {
      // Default: 16384 (16KB), or 16 for objectMode streams. PostgreSQL/MySQL use object streams
      highWaterMark: 10000
    };
  }

  /**
   * Create table (for db with write permissions) and extract data via memory/stream/unload
   */
  protected async downloadTempExternalPreAggregation(
    client: DriverInterface,
    newVersionEntry,
    preAggregation,
    saveCancelFn: SaveCancelFn
  ) {
    // @todo Deprecated, BaseDriver already implements it, before remove we need to add check for factoryDriver
    if (!client.downloadTable) {
      throw new Error('Can\'t load external pre-aggregation: source driver doesn\'t support downloadTable()');
    }

    const table = this.targetTableName(newVersionEntry);
    this.logger('Downloading external pre-aggregation', {
      preAggregation: this.preAggregation,
      requestId: this.requestId
    });

    const externalDriver = await this.externalDriverFactory();
    const capabilities = externalDriver.capabilities && externalDriver.capabilities();

    let tableData: DownloadTableData;

    if (capabilities.csvImport && client.unload && await client.isUnloadSupported(this.getUnloadOptions())) {
      tableData = await saveCancelFn(client.unload(table, this.getUnloadOptions()));
    } else if (capabilities.streamImport && client.stream) {
      tableData = await saveCancelFn(
        client.stream(`SELECT * FROM ${table}`, [], this.getStreamingOptions())
      );

      if (client.unload) {
        const stream = new LargeStreamWarning(preAggregation.preAggregationId);
        tableData.rowStream.pipe(stream);
        tableData.rowStream = stream;
      }
    } else {
      tableData = await saveCancelFn(client.downloadTable(table, capabilities));
    }

    if (!tableData.types) {
      tableData.types = await saveCancelFn(client.tableColumnTypes(table));
    }

    return tableData;
  }

  protected async uploadExternalPreAggregation(
    tableData: DownloadTableData,
    newVersionEntry: VersionEntry,
    saveCancelFn: SaveCancelFn,
  ) {
    const externalDriver: DriverInterface = await this.externalDriverFactory();
    const table = this.targetTableName(newVersionEntry);

    this.logger('Uploading external pre-aggregation', {
      preAggregation: this.preAggregation,
      requestId: this.requestId
    });

    await saveCancelFn(
      externalDriver.uploadTableWithIndexes(
        table, tableData.types, tableData, this.prepareIndexesSql(newVersionEntry)
      )
    );

    await this.loadCache.fetchTables(this.preAggregation);
    await this.dropOrphanedTables(externalDriver, table, saveCancelFn, true);
  }

  protected async createIndexes(driver, newVersionEntry, saveCancelFn) {
    const indexesSql = this.prepareIndexesSql(newVersionEntry);
    for (let i = 0; i < indexesSql.length; i++) {
      const [query, params] = indexesSql[i].sql;
      await saveCancelFn(driver.query(query, params));
    }
  }

  protected prepareIndexesSql(newVersionEntry) {
    if (!this.preAggregation.indexesSql || !this.preAggregation.indexesSql.length) {
      return [];
    }
    return this.preAggregation.indexesSql.map(({ sql, indexName }) => {
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
      const resultingSql = QueryCache.replacePreAggregationTableNames(
        query,
        this.preAggregationsTablesToTempTables.concat([
          [this.preAggregation.tableName, { targetTableName: this.targetTableName(newVersionEntry) }],
          [indexName, { targetTableName: this.targetTableName(indexVersionEntry) }]
        ])
      );
      return { sql: [resultingSql, params] };
    });
  }

  protected async dropOrphanedTables(
    client: DriverInterface,
    justCreatedTable: string,
    saveCancelFn: SaveCancelFn,
    external: boolean
  ) {
    await this.preAggregations.addTableUsed(justCreatedTable);

    const lockKey = external
      ? 'drop-orphaned-tables-external'
      : `drop-orphaned-tables:${this.preAggregation.dataSource}`;

    return this.queryCache.withLock(lockKey, 60 * 5, async () => {
      const actualTables = await client.getTablesQuery(this.preAggregation.preAggregationsSchema);
      const versionEntries = tablesToVersionEntries(this.preAggregation.preAggregationsSchema, actualTables);
      const versionEntriesToSave = R.pipe<
        VersionEntry[],
        { [index: string]: VersionEntry[] },
        Array<[string, VersionEntry[]]>,
        VersionEntry[]
        >(
          R.groupBy(v => v.table_name),
          R.toPairs,
          R.map(p => p[1][0])
        )(versionEntries);

      const structureVersionsToSave = R.pipe<
        VersionEntry[],
        VersionEntry[],
        { [index: string]: VersionEntry[] },
        Array<[string, VersionEntry[]]>,
        VersionEntry[]
        >(
          R.filter(
            (v: VersionEntry) => new Date().getTime() - v.last_updated_at < this.structureVersionPersistTime * 1000
          ),
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
    });
  }
}

export class PreAggregationPartitionRangeLoader {
  public constructor(
    private readonly redisPrefix: string,
    private readonly driverFactory: DriverFactory,
    private readonly logger: any,
    private readonly queryCache: QueryCache,
    // eslint-disable-next-line no-use-before-define
    private readonly preAggregations: PreAggregations,
    private readonly preAggregation: PreAggregationDescription,
    private readonly preAggregationsTablesToTempTables: any,
    private readonly loadCache: any,
    private readonly options: any = {}
  ) {
  }

  private async loadRangeQuery(rangeQuery) {
    return this.loadCache.keyQueryResult(rangeQuery, false, 10, 60 * 60); // TODO 60 * 60
  }

  private replacePartitionSqlAndParams(
    query: QueryWithParams,
    dateRange: QueryDateRange,
    partitionTableName: string
  ): QueryWithParams {
    const [sql, params, options] = query;
    const updateWindowToBoundary = options?.incremental && addSecondsToLocalTimestamp(
      dateRange[1], this.preAggregation.timezone, options?.updateWindowSeconds || 0
    );
    return [sql.replace(this.preAggregation.tableName, partitionTableName), params?.map(
      param => {
        if (dateRange && param === FROM_PARTITION_RANGE) {
          return PreAggregationPartitionRangeLoader.inDbTimeZone(this.preAggregation, dateRange[0]);
        } else if (dateRange && param === TO_PARTITION_RANGE) {
          return PreAggregationPartitionRangeLoader.inDbTimeZone(this.preAggregation, dateRange[1]);
        } else {
          return param;
        }
      },
    ), {
      ...options,
      renewalThreshold:
        options?.incremental && updateWindowToBoundary < new Date() ?
          // if updateWindowToBoundary passed just moments ago we want to renew it earlier in case of server
          // and db clock don't match
          Math.min(
            Math.round((new Date().getTime() - updateWindowToBoundary.getTime()) / 1000),
            options?.renewalThresholdOutsideUpdateWindow
          ) :
          options?.renewalThreshold
    }];
  }

  private partitionPreAggregationDescription(range: QueryDateRange): PreAggregationDescription {
    const partitionTableName = PreAggregationPartitionRangeLoader.partitionTableName(
      this.preAggregation.tableName, this.preAggregation.partitionGranularity, range
    );
    return {
      ...this.preAggregation,
      tableName: partitionTableName,
      loadSql: this.preAggregation.loadSql &&
        this.replacePartitionSqlAndParams(this.preAggregation.loadSql, range, partitionTableName),
      sql: this.preAggregation.sql &&
        this.replacePartitionSqlAndParams(this.preAggregation.sql, range, partitionTableName),
      invalidateKeyQueries: (this.preAggregation.invalidateKeyQueries || [])
        .map(q => this.replacePartitionSqlAndParams(q, range, partitionTableName)),
      indexesSql: (this.preAggregation.indexesSql || [])
        .map(q => ({ ...q, sql: this.replacePartitionSqlAndParams(q.sql, range, partitionTableName) })),
      previewSql: this.preAggregation.previewSql &&
        this.replacePartitionSqlAndParams(this.preAggregation.previewSql, range, partitionTableName)
    };
  }

  public async loadPreAggregations(): Promise<LoadPreAggregationResult> {
    if (this.preAggregation.preAggregationStartEndQueries) {
      const partitionRanges = await this.partitionRanges();
      const partitionLoaders = partitionRanges.map(range => new PreAggregationLoader(
        this.redisPrefix,
        this.driverFactory,
        this.logger,
        this.queryCache,
        this.preAggregations,
        this.partitionPreAggregationDescription(range),
        this.preAggregationsTablesToTempTables,
        this.loadCache,
        this.options
      ));
      const loadResults = await Promise.all(partitionLoaders.map(l => l.loadPreAggregation()));
      const allTableTargetNames = loadResults
        .map(
          targetTableName => (typeof targetTableName === 'string' ? targetTableName : targetTableName.targetTableName)
        );
      const unionTargetTableName = allTableTargetNames
        .map(targetTableName => `SELECT * FROM ${targetTableName}`)
        .join(' UNION ALL ');
      return {
        targetTableName: allTableTargetNames.length === 1 ? allTableTargetNames[0] : `(${unionTargetTableName})`,
        refreshKeyValues: loadResults.map(t => (typeof t === 'object' ? t.refreshKeyValues : {}))
      };
    } else {
      return new PreAggregationLoader(
        this.redisPrefix,
        this.driverFactory,
        this.logger,
        this.queryCache,
        this.preAggregations,
        this.preAggregation,
        this.preAggregationsTablesToTempTables,
        this.loadCache,
        this.options
      ).loadPreAggregation();
    }
  }

  public async partitionPreAggregations(): Promise<PreAggregationDescription[]> {
    if (this.preAggregation.preAggregationStartEndQueries) {
      const partitionRanges = await this.partitionRanges();
      return partitionRanges.map(range => this.partitionPreAggregationDescription(range));
    } else {
      return [this.preAggregation];
    }
  }

  private async partitionRanges() {
    const { preAggregationStartEndQueries } = this.preAggregation;
    const [startDate, endDate] = await Promise.all(
      preAggregationStartEndQueries.map(
        async rangeQuery => PreAggregationPartitionRangeLoader.extractDate(await this.loadRangeQuery(rangeQuery)),
      ),
    );
    let dateRange = PreAggregationPartitionRangeLoader.intersectDateRanges(
      [startDate, endDate],
      this.preAggregation.matchedTimeDimensionDateRange,
    );
    if (!dateRange) {
      // If there's no date range intersection between query data range and pre-aggregation build range
      // use last partition so outer query can receive expected table structure.
      dateRange = [endDate, endDate];
    }
    return PreAggregationPartitionRangeLoader.timeSeries(
      this.preAggregation.partitionGranularity,
      dateRange,
    );
  }

  private static checkDataRangeType(range: QueryDateRange) {
    if (!range) {
      return;
    }
    if (range.length !== 2) {
      throw new Error(`Date range expected to be an array with 2 elements but ${range} found`);
    }
    if (typeof range[0] !== 'string' || typeof range[1] !== 'string') {
      throw new Error(`Date range expected to be a string array but ${range} found`);
    }
    if (range[0].length !== 23 || range[1].length !== 23) {
      throw new Error(`Date range expected to be in YYYY-MM-DDTHH:mm:ss.SSS format but ${range} found`);
    }
  }

  public static intersectDateRanges(rangeA: QueryDateRange | null, rangeB: QueryDateRange | null): QueryDateRange {
    PreAggregationPartitionRangeLoader.checkDataRangeType(rangeA);
    PreAggregationPartitionRangeLoader.checkDataRangeType(rangeB);
    if (!rangeB) {
      return rangeA;
    }
    if (!rangeA) {
      return rangeB;
    }
    const from = rangeA[0] > rangeB[0] ? rangeA[0] : rangeB[0];
    const to = rangeA[1] < rangeB[1] ? rangeA[1] : rangeB[1];
    if (from > to) {
      return null;
    }
    return [
      from,
      to,
    ];
  }

  public static timeSeries(granularity: string, dateRange: QueryDateRange): QueryDateRange[] {
    return timeSeries(granularity, dateRange);
  }

  public static partitionTableName(tableName: string, partitionGranularity: string, dateRange: string[]) {
    const partitionSuffix = dateRange[0].substring(
      0,
      partitionGranularity === 'hour' ? 13 : 10
    ).replace(/[-T:]/g, '');
    return `${tableName}${partitionSuffix}`;
  }

  public static inDbTimeZone(preAggregationDescription: any, timestamp: string): string {
    return inDbTimeZone(preAggregationDescription.timezone, preAggregationDescription.timestampFormat, timestamp);
  }

  public static extractDate(data: any): string {
    return extractDate(data);
  }

  public static FROM_PARTITION_RANGE = FROM_PARTITION_RANGE;

  public static TO_PARTITION_RANGE = TO_PARTITION_RANGE;
}

type PreAggregationsOptions = {
  preAggregationsSchemaCacheExpire?: number;
  loadCacheQueueOptions?: any;
  queueOptions?: object | ((dataSource: String) => object);
  redisPool?: any;
  continueWaitTimeout?: number;
  cacheAndQueueDriver?: 'redis' | 'memory';
  skipExternalCacheAndQueue?: boolean;
};

export class PreAggregations {
  public options: PreAggregationsOptions;

  private cacheDriver: CacheDriverInterface;

  public externalDriverFactory: any;

  public structureVersionPersistTime: any;

  private readonly usedTablePersistTime: number;

  private readonly externalRefresh: boolean;

  private readonly loadCacheQueue: Record<string, QueryQueue> = {};

  private readonly queue: Record<string, QueryQueue> = {};

  public constructor(
    private readonly redisPrefix: string,
    private readonly driverFactory: DriverFactoryByDataSource,
    private readonly logger: any,
    private readonly queryCache: QueryCache,
    options
  ) {
    this.options = options || {};

    this.cacheDriver = options.cacheAndQueueDriver === 'redis' ?
      new RedisCacheDriver({ pool: options.redisPool }) :
      new LocalCacheDriver();
    this.externalDriverFactory = options.externalDriverFactory;
    this.structureVersionPersistTime = options.structureVersionPersistTime || 60 * 60 * 24 * 30;
    this.usedTablePersistTime = options.usedTablePersistTime || 600;
    this.externalRefresh = options.externalRefresh;
  }

  protected tablesUsedRedisKey(tableName) {
    // TODO add dataSource?
    return `SQL_PRE_AGGREGATIONS_${this.redisPrefix}_TABLES_USED_${tableName}`;
  }

  public async addTableUsed(tableName) {
    return this.cacheDriver.set(this.tablesUsedRedisKey(tableName), true, this.usedTablePersistTime);
  }

  public async tablesUsed() {
    return (await this.cacheDriver.keysStartingWith(this.tablesUsedRedisKey('')))
      .map(k => k.replace(this.tablesUsedRedisKey(''), ''));
  }

  public loadAllPreAggregationsIfNeeded(queryBody) {
    const preAggregations = queryBody.preAggregations || [];

    const loadCacheByDataSource = queryBody.preAggregationsLoadCacheByDataSource || {};

    const getLoadCacheByDataSource = (dataSource = 'default') => {
      if (!loadCacheByDataSource[dataSource]) {
        loadCacheByDataSource[dataSource] =
          new PreAggregationLoadCache(this.redisPrefix, () => this.driverFactory(dataSource), this.queryCache, this, {
            requestId: queryBody.requestId,
            dataSource
          });
      }

      return loadCacheByDataSource[dataSource];
    };

    return preAggregations.map(p => (preAggregationsTablesToTempTables) => {
      const loader = new PreAggregationPartitionRangeLoader(
        this.redisPrefix,
        () => this.driverFactory(p.dataSource || 'default'),
        this.logger,
        this.queryCache,
        this,
        p,
        preAggregationsTablesToTempTables,
        getLoadCacheByDataSource(p.dataSource),
        {
          waitForRenew: queryBody.renewQuery,
          forceBuild: queryBody.forceBuildPreAggregations,
          requestId: queryBody.requestId,
          orphanedTimeout: queryBody.orphanedTimeout,
          externalRefresh: this.externalRefresh
        }
      );

      const preAggregationPromise = () => loader.loadPreAggregations().then(async targetTableName => {
        const usedPreAggregation = {
          ...(typeof targetTableName === 'string' ? { targetTableName } : targetTableName),
          type: p.type,
        };
        await this.addTableUsed(usedPreAggregation.targetTableName);

        return [p.tableName, usedPreAggregation];
      });

      return preAggregationPromise().then(res => preAggregationsTablesToTempTables.concat([res]));
    }).reduce((promise, fn) => promise.then(fn), Promise.resolve([]));
  }

  public async expandPartitionsInPreAggregations(queryBody) {
    const preAggregations = queryBody.preAggregations || [];

    const loadCacheByDataSource = queryBody.preAggregationsLoadCacheByDataSource || {};

    const getLoadCacheByDataSource = (dataSource = 'default') => {
      if (!loadCacheByDataSource[dataSource]) {
        loadCacheByDataSource[dataSource] =
          new PreAggregationLoadCache(this.redisPrefix, () => this.driverFactory(dataSource), this.queryCache, this, {
            requestId: queryBody.requestId,
            dataSource
          });
      }

      return loadCacheByDataSource[dataSource];
    };

    const expandedPreAggregations: PreAggregationDescription[][] = await Promise.all(preAggregations.map(p => {
      const loader = new PreAggregationPartitionRangeLoader(
        this.redisPrefix,
        () => this.driverFactory(p.dataSource || 'default'),
        this.logger,
        this.queryCache,
        this,
        p,
        [],
        getLoadCacheByDataSource(p.dataSource),
        {
          waitForRenew: queryBody.renewQuery,
          requestId: queryBody.requestId,
          externalRefresh: this.externalRefresh
        }
      );

      return loader.partitionPreAggregations();
    }));

    return {
      ...queryBody,
      preAggregations: expandedPreAggregations.reduce((a, b) => a.concat(b), [])
    };
  }

  public getQueue(dataSource: string = 'default') {
    if (!this.queue[dataSource]) {
      this.queue[dataSource] = QueryCache.createQueue(`SQL_PRE_AGGREGATIONS_${this.redisPrefix}_${dataSource}`, () => this.driverFactory(dataSource), (client, q) => {
        const {
          preAggregation, preAggregationsTablesToTempTables, newVersionEntry, requestId, invalidationKeys
        } = q;
        const loader = new PreAggregationLoader(
          this.redisPrefix,
          () => this.driverFactory(dataSource),
          this.logger,
          this.queryCache,
          this,
          preAggregation,
          preAggregationsTablesToTempTables,
          new PreAggregationLoadCache(
            this.redisPrefix,
            () => this.driverFactory(dataSource),
            this.queryCache,
            this,
            { requestId, dataSource }
          ),
          { requestId, externalRefresh: this.externalRefresh }
        );
        return loader.refresh(preAggregation, newVersionEntry, invalidationKeys)(client);
      }, {
        concurrency: 1,
        logger: this.logger,
        cacheAndQueueDriver: this.options.cacheAndQueueDriver,
        redisPool: this.options.redisPool,
        // Centralized continueWaitTimeout that can be overridden in queueOptions
        continueWaitTimeout: this.options.continueWaitTimeout,
        ...(typeof this.options.queueOptions === 'function' ?
          this.options.queueOptions(dataSource) :
          this.options.queueOptions
        )
      });
    }
    return this.queue[dataSource];
  }

  public getLoadCacheQueue(dataSource: string = 'default') {
    if (!this.loadCacheQueue[dataSource]) {
      this.loadCacheQueue[dataSource] = QueryCache.createQueue(
        `SQL_PRE_AGGREGATIONS_CACHE_${this.redisPrefix}_${dataSource}`,
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        () => <BaseDriver> {},
        (_, q) => {
          const {
            preAggregation,
            requestId
          } = q;
          const loadCache = new PreAggregationLoadCache(
            this.redisPrefix, () => this.driverFactory(dataSource), this.queryCache, this,
            { requestId, dataSource }
          );
          return loadCache.fetchTables(preAggregation);
        }, {
          concurrency: 4,
          logger: this.logger,
          cacheAndQueueDriver: this.options.cacheAndQueueDriver,
          redisPool: this.options.redisPool,
          ...this.options.loadCacheQueueOptions
        }
      );
    }
    return this.loadCacheQueue[dataSource];
  }

  public static preAggregationQueryCacheKey(preAggregation) {
    return preAggregation.tableName;
  }

  public static targetTableName(versionEntry) {
    if (versionEntry.naming_version === 2) {
      return `${versionEntry.table_name}_${versionEntry.content_version}_${versionEntry.structure_version}_${encodeTimeStamp(versionEntry.last_updated_at)}`;
    }

    return `${versionEntry.table_name}_${versionEntry.content_version}_${versionEntry.structure_version}_${versionEntry.last_updated_at}`;
  }

  public static structureVersion(preAggregation) {
    return getStructureVersion(preAggregation);
  }

  public async getVersionEntries(preAggregations, requestId): Promise<VersionEntry[][]> {
    const loadCacheByDataSource = [...new Set(preAggregations.map(p => p.dataSource))]
      .reduce((obj, dataSource: string) => {
        obj[dataSource] = new PreAggregationLoadCache(
          this.redisPrefix,
          () => this.driverFactory(dataSource),
          this.queryCache,
          this,
          {
            requestId,
            dataSource
          }
        );

        return obj;
      }, {});

    const firstByCacheKey = {};
    const data: VersionEntry[][] = await Promise.all(
      preAggregations.map(
        async preAggregation => {
          const { dataSource } = preAggregation;
          const cacheKey = loadCacheByDataSource[dataSource].tablesRedisKey(preAggregation);
          if (!firstByCacheKey[cacheKey]) {
            firstByCacheKey[cacheKey] = loadCacheByDataSource[dataSource].getVersionEntries(preAggregation);
            const res = await firstByCacheKey[cacheKey];
            return res.versionEntries;
          }

          return null;
        }
      )
    );
    return data.filter(res => res);
  }

  public async getQueueState(dataSource = undefined) {
    const queries = await this.getQueue(dataSource).getQueries();
    return queries;
  }
}
