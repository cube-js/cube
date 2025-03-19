import R from 'ramda';
import { getEnv, MaybeCancelablePromise } from '@cubejs-backend/shared';
import {
  cancelCombinator,
  DownloadQueryResultsResult,
  DownloadTableData,
  DriverCapabilities,
  DriverInterface,
  isDownloadTableCSVData,
  SaveCancelFn,
  StreamOptions,
  UnloadOptions
} from '@cubejs-backend/base-driver';
import { DriverFactory } from './DriverFactory';
import { PreAggTableToTempTableNames, QueryCache, QueryWithParams } from './QueryCache';
import { ContinueWaitError } from './ContinueWaitError';
import { LargeStreamWarning } from './StreamObjectsCounter';
import {
  getStructureVersion,
  InvalidationKeys,
  LoadPreAggregationResult,
  PreAggregations,
  PreAggregationTableToTempTable,
  tablesToVersionEntries,
  version,
  VersionEntriesObj,
  VersionEntry
} from './PreAggregations';
import { PreAggregationLoadCache } from './PreAggregationLoadCache';

type IndexesSql = { sql: [string, unknown[]], indexName: string }[];

type QueryKey = [QueryWithParams, IndexesSql, InvalidationKeys] | [QueryWithParams, InvalidationKeys];

type QueryOptions = {
  queryKey: QueryKey;
  newVersionEntry: VersionEntry;
  query: string;
  values: unknown[];
  requestId: string;
  buildRangeEnd?: string;
};

// There are community developed and custom drivers which not always up-to-date with latest BaseDriver.
// Extra defence for drivers that don't expose now() yet.
function nowTimestamp(client: DriverInterface) {
  return client.nowTimestamp?.() ?? new Date().getTime();
}

export class PreAggregationLoader {
  private preAggregations: PreAggregations;

  public preAggregation: any;

  private readonly preAggregationsTablesToTempTables: PreAggregationTableToTempTable[];

  /**
   * Determines whether current instance instantiated for a jobbed build query
   * (initialized by the /cubejs-system/v1/pre-aggregations/jobs endpoint) or
   * not.
   */
  private readonly isJob: boolean;

  private readonly waitForRenew: boolean;

  private readonly forceBuild: boolean;

  private readonly orphanedTimeout: number;

  private readonly externalDriverFactory: DriverFactory;

  private readonly requestId: string;

  private readonly metadata: any;

  private readonly structureVersionPersistTime: any;

  private readonly externalRefresh: boolean;

  public constructor(
    private readonly driverFactory: DriverFactory,
    private readonly logger: any,
    private readonly queryCache: QueryCache,
    preAggregations: PreAggregations,
    preAggregation,
    preAggregationsTablesToTempTables: PreAggregationTableToTempTable[],
    private readonly loadCache: PreAggregationLoadCache,
    options: any = {}
  ) {
    this.preAggregations = preAggregations;
    this.preAggregation = preAggregation;
    this.preAggregationsTablesToTempTables = preAggregationsTablesToTempTables;
    this.isJob = !!options.isJob;
    this.waitForRenew = options.waitForRenew;
    this.forceBuild = options.forceBuild;
    this.orphanedTimeout = options.orphanedTimeout;
    this.externalDriverFactory = preAggregations.externalDriverFactory;
    this.requestId = options.requestId;
    this.metadata = options.metadata;
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

  public async loadPreAggregation(
    throwOnMissingPartition: boolean,
  ): Promise<null | LoadPreAggregationResult> {
    const notLoadedKey = (this.preAggregation.invalidateKeyQueries || [])
      .find(keyQuery => !this.loadCache.hasKeyQueryResult(keyQuery));

    if (this.isJob || !(notLoadedKey && !this.waitForRenew)) {
      // Case 1: pre-agg build job processing.
      // Case 2: either we have no data cached for this rollup or waitForRenew
      // is true, either way, synchronously renew what data is needed so that
      // the most current data will be returned fo the current request.
      const result = await this.loadPreAggregationWithKeys();
      const refreshKeyValues = await this.getInvalidationKeyValues();
      return {
        ...result,
        refreshKeyValues,
        queryKey: this.isJob
          // We need to return a queryKey value for the jobed build query
          // (initialized by the /cubejs-system/v1/pre-aggregations/jobs
          // endpoint) as a part of the response to make it possible to get a
          // query result from the cache by the other API call.
          ? this.preAggregationQueryKey(refreshKeyValues)
          : undefined,
      };
    } else {
      // Case 3: pre-agg exists
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
        if (!versionEntryByStructureVersion && throwOnMissingPartition) {
          // eslint-disable-next-line no-use-before-define
          throw new Error(PreAggregations.noPreAggregationPartitionsBuiltMessage([this.preAggregation]));
        }
        if (!versionEntryByStructureVersion) {
          return null;
        } else {
          // the rollups are being maintained independently of this instance of cube.js
          // immediately return the latest rollup data that instance already has
          return {
            targetTableName: this.targetTableName(versionEntryByStructureVersion),
            refreshKeyValues: [],
            lastUpdatedAt: versionEntryByStructureVersion.last_updated_at,
            buildRangeEnd: versionEntryByStructureVersion.build_range_end,
          };
        }
      }

      if (versionEntryByStructureVersion) {
        // this triggers an asynchronous/background load of the pre-aggregation but immediately
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
        return {
          targetTableName: this.targetTableName(versionEntryByStructureVersion),
          refreshKeyValues: [],
          lastUpdatedAt: versionEntryByStructureVersion.last_updated_at,
          buildRangeEnd: versionEntryByStructureVersion.build_range_end,
        };
      } else {
        // no rollup has been built yet - build it synchronously as part of responding to this request
        return this.loadPreAggregationWithKeys();
      }
    }
  }

  protected async loadPreAggregationWithKeys(): Promise<LoadPreAggregationResult> {
    const invalidationKeys = await this.getPartitionInvalidationKeyValues();

    const contentVersion = this.contentVersion(invalidationKeys);
    const structureVersion = getStructureVersion(this.preAggregation);

    const versionEntries = await this.loadCache.getVersionEntries(this.preAggregation);

    const getVersionEntryByContentVersion = ({ byContent }: VersionEntriesObj) => byContent[`${this.preAggregation.tableName}_${contentVersion}`];

    const versionEntryByContentVersion = getVersionEntryByContentVersion(versionEntries);
    if (versionEntryByContentVersion && !this.forceBuild) {
      const targetTableName = this.targetTableName(versionEntryByContentVersion);
      // No need to block here
      this.updateLastTouch(targetTableName);
      return {
        targetTableName,
        refreshKeyValues: [],
        lastUpdatedAt: versionEntryByContentVersion.last_updated_at,
        buildRangeEnd: versionEntryByContentVersion.build_range_end,
      };
    }

    if (!this.waitForRenew && !this.forceBuild) {
      const versionEntryByStructureVersion = versionEntries.byStructure[`${this.preAggregation.tableName}_${structureVersion}`];
      if (versionEntryByStructureVersion) {
        const targetTableName = this.targetTableName(versionEntryByStructureVersion);
        // No need to block here
        this.updateLastTouch(targetTableName);
        return {
          targetTableName,
          refreshKeyValues: [],
          lastUpdatedAt: versionEntryByStructureVersion.last_updated_at,
          buildRangeEnd: versionEntryByStructureVersion.build_range_end,
        };
      }
    }

    const client = this.preAggregation.external ?
      await this.externalDriverFactory() :
      await this.driverFactory();

    if (!versionEntries.versionEntries.length) {
      await client.createSchemaIfNotExists(this.preAggregation.preAggregationsSchema);
    }

    // ensure we find appropriate structure version before invalidating anything
    const versionEntry =
      versionEntries.byStructure[`${this.preAggregation.tableName}_${structureVersion}`] ||
      versionEntries.byTableName[this.preAggregation.tableName];

    const newVersionEntry: VersionEntry = {
      table_name: this.preAggregation.tableName,
      structure_version: structureVersion,
      content_version: contentVersion,
      last_updated_at: nowTimestamp(client),
      naming_version: 2,
    };

    const mostRecentResult: () => Promise<LoadPreAggregationResult> = async () => {
      await this.loadCache.reset(this.preAggregation);
      const lastVersion = getVersionEntryByContentVersion(
        await this.loadCache.getVersionEntries(this.preAggregation)
      );
      if (!lastVersion) {
        throw new Error(`Pre-aggregation table is not found for ${this.preAggregation.tableName} after it was successfully created`);
      }
      const targetTableName = this.targetTableName(lastVersion);
      this.updateLastTouch(targetTableName);
      return {
        targetTableName,
        refreshKeyValues: [],
        lastUpdatedAt: lastVersion.last_updated_at,
        buildRangeEnd: lastVersion.build_range_end,
      };
    };

    if (this.forceBuild) {
      this.logger('Force build pre-aggregation', {
        preAggregation: this.preAggregation,
        requestId: this.requestId,
        metadata: this.metadata,
        queryKey: this.preAggregationQueryKey(invalidationKeys),
        newVersionEntry
      });
      if (this.isJob) {
        // We don't want to wait for the jobed build query result. So we run the
        // executeInQueue method and immediately return the LoadPreAggregationResult object.
        this
          .executeInQueue(invalidationKeys, this.priority(10), newVersionEntry)
          .catch((e: any) => {
            this.logger('Pre-aggregations build job error', {
              preAggregation: this.preAggregation,
              requestId: this.requestId,
              newVersionEntry,
              error: (e.stack || e),
            });
          });
        const targetTableName = this.targetTableName(newVersionEntry);
        this.updateLastTouch(targetTableName);
        return {
          targetTableName,
          refreshKeyValues: [],
          lastUpdatedAt: newVersionEntry.last_updated_at,
          buildRangeEnd: this.preAggregation.buildRangeEnd,
        };
      } else {
        await this.executeInQueue(invalidationKeys, this.priority(10), newVersionEntry);
        return mostRecentResult();
      }
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
        return mostRecentResult();
      } else if (versionEntry.content_version !== newVersionEntry.content_version) {
        if (this.waitForRenew) {
          this.logger('Waiting for pre-aggregation renew', {
            preAggregation: this.preAggregation,
            requestId: this.requestId,
            queryKey: this.preAggregationQueryKey(invalidationKeys),
            newVersionEntry
          });
          await this.executeInQueue(invalidationKeys, this.priority(0), newVersionEntry);
          return mostRecentResult();
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
      return mostRecentResult();
    }
    const targetTableName = this.targetTableName(versionEntry);
    this.updateLastTouch(targetTableName);
    return {
      targetTableName,
      refreshKeyValues: [],
      lastUpdatedAt: versionEntry.last_updated_at,
      buildRangeEnd: versionEntry.build_range_end,
    };
  }

  private updateLastTouch(tableName: string) {
    this.preAggregations.updateLastTouch(tableName).catch(e => {
      this.logger('Error on pre-aggregation touch', {
        error: (e.stack || e), preAggregation: this.preAggregation, requestId: this.requestId,
      });
    });
  }

  protected contentVersion(invalidationKeys: InvalidationKeys) {
    const versionArray = [this.preAggregation.structureVersionLoadSql || this.preAggregation.loadSql];
    if (this.preAggregation.indexesSql && this.preAggregation.indexesSql.length) {
      versionArray.push(this.preAggregation.indexesSql);
    }
    if (this.preAggregation.streamOffset) {
      versionArray.push(this.preAggregation.streamOffset);
    }
    if (this.preAggregation.outputColumnTypes) {
      versionArray.push(this.preAggregation.outputColumnTypes);
    }
    versionArray.push(invalidationKeys);
    return version(versionArray);
  }

  protected priority(defaultValue: number): number {
    return this.preAggregation.priority != null ? this.preAggregation.priority : defaultValue;
  }

  protected getInvalidationKeyValues() {
    return Promise.all(
      (this.preAggregation.invalidateKeyQueries || []).map(
        (sqlQuery) => this.loadCache.keyQueryResult(sqlQuery, this.waitForRenew, this.priority(10))
      )
    );
  }

  protected getPartitionInvalidationKeyValues() {
    if (this.preAggregation.partitionInvalidateKeyQueries) {
      return Promise.all(
        (this.preAggregation.partitionInvalidateKeyQueries || []).map(
          (sqlQuery) => this.loadCache.keyQueryResult(sqlQuery, this.waitForRenew, this.priority(10))
        )
      );
    } else {
      return this.getInvalidationKeyValues();
    }
  }

  protected scheduleRefresh(invalidationKeys: InvalidationKeys, newVersionEntry: VersionEntry) {
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

  protected async executeInQueue(invalidationKeys: InvalidationKeys, priority: number, newVersionEntry: VersionEntry) {
    const queue = await this.preAggregations.getQueue(this.preAggregation.dataSource);
    return queue.executeInQueue(
      'query',
      this.preAggregationQueryKey(invalidationKeys),
      {
        preAggregation: this.preAggregation,
        preAggregationsTablesToTempTables: this.preAggregationsTablesToTempTables,
        newVersionEntry,
        requestId: this.requestId,
        invalidationKeys,
        forceBuild: this.forceBuild,
        isJob: this.isJob,
        metadata: this.metadata,
        orphanedTimeout: this.orphanedTimeout,
      },
      priority,
      // eslint-disable-next-line no-use-before-define
      { stageQueryKey: PreAggregations.preAggregationQueryCacheKey(this.preAggregation), requestId: this.requestId }
    );
  }

  protected preAggregationQueryKey(invalidationKeys: InvalidationKeys): QueryKey {
    return this.preAggregation.indexesSql && this.preAggregation.indexesSql.length ?
      [this.preAggregation.loadSql, this.preAggregation.indexesSql, invalidationKeys] :
      [this.preAggregation.loadSql, invalidationKeys];
  }

  protected targetTableName(versionEntry: VersionEntry): string {
    // eslint-disable-next-line no-use-before-define
    return PreAggregations.targetTableName(versionEntry);
  }

  public refresh(newVersionEntry: VersionEntry, invalidationKeys: InvalidationKeys, client) {
    this.updateLastTouch(this.targetTableName(newVersionEntry));
    let refreshStrategy = this.refreshStoreInSourceStrategy;
    if (this.preAggregation.external) {
      const readOnly =
        this.preAggregation.readOnly ||
        client.config && client.config.readOnly ||
        client.readOnly && (typeof client.readOnly === 'boolean' ? client.readOnly : client.readOnly());

      if (readOnly) {
        refreshStrategy = this.refreshReadOnlyExternalStrategy;
      } else {
        refreshStrategy = this.refreshWriteStrategy;
      }
    }
    return cancelCombinator(
      saveCancelFn => refreshStrategy.bind(this)(
        client,
        newVersionEntry,
        saveCancelFn,
        invalidationKeys
      )
    );
  }

  protected logExecutingSql(payload) {
    this.logger(
      'Executing Load Pre Aggregation SQL',
      payload
    );
  }

  protected queryOptions(invalidationKeys: InvalidationKeys, query: string, params: unknown[], targetTableName: string, newVersionEntry: VersionEntry) {
    return {
      queryKey: this.preAggregationQueryKey(invalidationKeys),
      query,
      values: params,
      targetTableName,
      requestId: this.requestId,
      newVersionEntry,
      buildRangeEnd: this.preAggregation.buildRangeEnd,
    };
  }

  protected async refreshStoreInSourceStrategy(
    client: DriverInterface,
    newVersionEntry: VersionEntry,
    saveCancelFn: SaveCancelFn,
    invalidationKeys: InvalidationKeys
  ) {
    const [loadSql, params] =
      Array.isArray(this.preAggregation.loadSql) ? this.preAggregation.loadSql : [this.preAggregation.loadSql, []];
    const targetTableName = this.targetTableName(newVersionEntry);
    const query = (
      <string>QueryCache.replacePreAggregationTableNames(
        loadSql,
        this.preAggregationsTablesToTempTables,
      )
    ).replace(
      this.preAggregation.tableName,
      targetTableName
    );
    const queryOptions = this.queryOptions(invalidationKeys, query, params, targetTableName, newVersionEntry);
    this.logExecutingSql(queryOptions);

    try {
      // TODO move index creation to the driver
      await saveCancelFn(client.loadPreAggregationIntoTable(
        targetTableName,
        query,
        params,
        {
          streamOffset: this.preAggregation.streamOffset,
          outputColumnTypes: this.preAggregation.outputColumnTypes,
          ...queryOptions
        }
      ));

      await this.createIndexes(client, newVersionEntry, saveCancelFn, queryOptions);
      await this.loadCache.fetchTables(this.preAggregation);
    } finally {
      // We must clean orphaned in any cases: success or exception
      await this.dropOrphanedTables(client, targetTableName, saveCancelFn, false, queryOptions);
      await this.loadCache.fetchTables(this.preAggregation);
    }
  }

  protected async refreshWriteStrategy(
    client: DriverInterface,
    newVersionEntry: VersionEntry,
    saveCancelFn: SaveCancelFn,
    invalidationKeys: InvalidationKeys,
  ) {
    const capabilities = client?.capabilities();

    const withTempTable = !(capabilities?.unloadWithoutTempTable);
    const dropSourceTempTable = !capabilities?.streamingSource;

    return this.runWriteStrategy(
      client,
      newVersionEntry,
      saveCancelFn,
      invalidationKeys,
      withTempTable,
      dropSourceTempTable
    );
  }

  /**
   * Runs export strategy with write access in data source
   */
  protected async runWriteStrategy(
    client: DriverInterface,
    newVersionEntry: VersionEntry,
    saveCancelFn: SaveCancelFn,
    invalidationKeys: InvalidationKeys,
    withTempTable: boolean,
    dropSourceTempTable: boolean,
  ) {
    if (withTempTable) {
      await client.createSchemaIfNotExists(this.preAggregation.preAggregationsSchema);
    }
    const targetTableName = this.targetTableName(newVersionEntry);
    const queryOptions = await this.prepareWriteStrategy(
      client,
      targetTableName,
      newVersionEntry,
      saveCancelFn,
      invalidationKeys,
      withTempTable,
    );

    try {
      const tableData = await this.downloadExternalPreAggregation(
        client,
        newVersionEntry,
        saveCancelFn,
        queryOptions,
        withTempTable,
      );

      try {
        await this.uploadExternalPreAggregation(tableData, newVersionEntry, saveCancelFn, queryOptions);
      } finally {
        if (tableData && tableData.release) {
          await tableData.release();
        }
      }
    } finally {
      await this.cleanupWriteStrategy(
        client,
        targetTableName,
        queryOptions,
        saveCancelFn,
        withTempTable,
        dropSourceTempTable,
      );
    }
  }

  /**
   * Cleanup tables after write strategy
   */
  protected async cleanupWriteStrategy(
    client: DriverInterface,
    targetTableName: string,
    queryOptions: QueryOptions,
    saveCancelFn: SaveCancelFn,
    withTempTable: boolean,
    dropSourceTempTable: boolean,
  ) {
    if (withTempTable && dropSourceTempTable) {
      await this.withDropLock(`drop-temp-table:${this.preAggregation.dataSource}:${targetTableName}`, async () => {
        this.logger('Dropping source temp table', queryOptions);

        const actualTables = await client.getTablesQuery(this.preAggregation.preAggregationsSchema);
        const mappedActualTables = actualTables.map(t => `${this.preAggregation.preAggregationsSchema}.${t.table_name || t.TABLE_NAME}`);
        if (mappedActualTables.includes(targetTableName)) {
          await client.dropTable(targetTableName);
        }
      });
    }

    // We must clean orphaned in any cases: success or exception
    await this.loadCache.fetchTables(this.preAggregation);
    await this.dropOrphanedTables(client, targetTableName, saveCancelFn, false, queryOptions);
  }

  /**
   * Create table (if required) and prepares query options object
   */
  protected async prepareWriteStrategy(
    client: DriverInterface,
    targetTableName: string,
    newVersionEntry: VersionEntry,
    saveCancelFn: SaveCancelFn,
    invalidationKeys: InvalidationKeys,
    withTempTable: boolean
  ): Promise<QueryOptions> {
    if (withTempTable) {
      const [loadSql, params] =
        Array.isArray(this.preAggregation.loadSql) ? this.preAggregation.loadSql : [this.preAggregation.loadSql, []];

      const query = (
        <string>QueryCache.replacePreAggregationTableNames(
          loadSql,
          this.preAggregationsTablesToTempTables,
        )
      ).replace(
        this.preAggregation.tableName,
        targetTableName
      );
      const queryOptions = this.queryOptions(invalidationKeys, query, params, targetTableName, newVersionEntry);
      this.logExecutingSql(queryOptions);
      await saveCancelFn(client.loadPreAggregationIntoTable(
        targetTableName,
        query,
        params,
        {
          streamOffset: this.preAggregation.streamOffset,
          outputColumnTypes: this.preAggregation.outputColumnTypes,
          ...queryOptions
        }
      ));

      return queryOptions;
    } else {
      const [sql, params] =
        Array.isArray(this.preAggregation.sql) ? this.preAggregation.sql : [this.preAggregation.sql, []];
      const queryOptions = this.queryOptions(invalidationKeys, sql, params, targetTableName, newVersionEntry);
      this.logExecutingSql(queryOptions);
      return queryOptions;
    }
  }

  /**
   * Strategy to copy pre-aggregation from source db (for read-only permissions) to external data
   */
  protected async refreshReadOnlyExternalStrategy(
    client: DriverInterface,
    newVersionEntry: VersionEntry,
    saveCancelFn: SaveCancelFn,
    invalidationKeys: InvalidationKeys
  ) {
    const [sql, params] =
      Array.isArray(this.preAggregation.sql) ? this.preAggregation.sql : [this.preAggregation.sql, []];

    const queryOptions = this.queryOptions(invalidationKeys, sql, params, this.targetTableName(newVersionEntry), newVersionEntry);
    this.logExecutingSql(queryOptions);
    this.logger('Downloading external pre-aggregation via query', queryOptions);
    const externalDriver = await this.externalDriverFactory();
    const capabilities = externalDriver.capabilities && externalDriver.capabilities();

    let tableData: DownloadQueryResultsResult;

    if (capabilities.csvImport && client.unloadFromQuery && await client.isUnloadSupported(this.getUnloadOptions())) {
      tableData = await saveCancelFn(
        client.unloadFromQuery(
          sql,
          params,
          this.getUnloadOptions(),
        )
      ).catch((error: any) => {
        this.logger('Downloading external pre-aggregation via query error', {
          ...queryOptions,
          error: error.stack || error.message
        });
        throw error;
      });
    } else {
      tableData = await saveCancelFn(client.downloadQueryResults(
        sql,
        params, {
          streamOffset: this.preAggregation.streamOffset,
          outputColumnTypes: this.preAggregation.outputColumnTypes,
          ...queryOptions,
          ...capabilities,
          ...this.getStreamingOptions(),
        }
      )).catch((error: any) => {
        this.logger('Downloading external pre-aggregation via query error', {
          ...queryOptions,
          error: error.stack || error.message
        });
        throw error;
      });
    }

    this.logger('Downloading external pre-aggregation via query completed', {
      ...queryOptions,
      isUnloadSupported: isDownloadTableCSVData(tableData)
    });

    try {
      await this.uploadExternalPreAggregation(tableData, newVersionEntry, saveCancelFn, queryOptions);
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
   * prepares download data for future cube store usage
   */
  protected async downloadExternalPreAggregation(
    client: DriverInterface,
    newVersionEntry: VersionEntry,
    saveCancelFn: SaveCancelFn,
    queryOptions: QueryOptions,
    withTempTable: boolean
  ) {
    const table = this.targetTableName(newVersionEntry);
    this.logger('Downloading external pre-aggregation', queryOptions);

    try {
      const externalDriver = await this.externalDriverFactory();
      const capabilities = externalDriver.capabilities && externalDriver.capabilities();

      let tableData: DownloadTableData;
      if (withTempTable) {
        tableData = await this.getTableDataWithTempTable(client, table, saveCancelFn, queryOptions, capabilities);
      } else {
        tableData = await this.getTableDataWithoutTempTable(client, table, saveCancelFn, queryOptions, capabilities);
      }

      this.logger('Downloading external pre-aggregation completed', {
        ...queryOptions,
        isUnloadSupported: isDownloadTableCSVData(tableData)
      });

      return tableData;
    } catch (error: any) {
      this.logger('Downloading external pre-aggregation error', {
        ...queryOptions,
        error: error?.stack || error?.message
      });
      throw error;
    }
  }

  /**
   * prepares download data when temp table = true
   */
  protected async getTableDataWithTempTable(client: DriverInterface, table: string, saveCancelFn: SaveCancelFn, queryOptions: QueryOptions, externalDriverCapabilities: DriverCapabilities) {
    let tableData: DownloadTableData;

    if (externalDriverCapabilities.csvImport && client.unload && await client.isUnloadSupported(this.getUnloadOptions())) {
      tableData = await saveCancelFn(
        client.unload(table, this.getUnloadOptions()),
      );
    } else if (externalDriverCapabilities.streamImport && client.stream) {
      tableData = await saveCancelFn(
        client.stream(`SELECT * FROM ${table}`, [], this.getStreamingOptions())
      );

      if (client.unload) {
        const stream = new LargeStreamWarning(this.preAggregation.preAggregationId, (msg) => {
          this.logger('Downloading external pre-aggregation warning', {
            ...queryOptions,
            error: msg
          });
        });
        tableData.rowStream.pipe(stream);
        tableData.rowStream = stream;
      }
    } else {
      tableData = await saveCancelFn(client.downloadTable(table, {
        streamOffset: this.preAggregation.streamOffset,
        outputColumnTypes: this.preAggregation.outputColumnTypes,
        ...externalDriverCapabilities
      }));
    }

    if (!tableData.types) {
      tableData.types = await saveCancelFn(client.tableColumnTypes(table));
    }

    return tableData;
  }

  /**
   * prepares download data when temp table = false
   */
  protected async getTableDataWithoutTempTable(client: DriverInterface, table: string, saveCancelFn: SaveCancelFn, queryOptions: QueryOptions, externalDriverCapabilities: DriverCapabilities) {
    const [sql, params] =
      Array.isArray(this.preAggregation.sql) ? this.preAggregation.sql : [this.preAggregation.sql, []];

    let tableData: DownloadTableData;

    if (externalDriverCapabilities.csvImport && client.unload && await client.isUnloadSupported(this.getUnloadOptions())) {
      return saveCancelFn(
        client.unload(
          table,
          { ...this.getUnloadOptions(), query: { sql, params } },
        )
      );
    } else if (externalDriverCapabilities.streamImport && client.stream) {
      tableData = await saveCancelFn(
        client.stream(sql, params, this.getStreamingOptions())
      );

      if (client.unload) {
        const stream = new LargeStreamWarning(this.preAggregation.preAggregationId, (msg) => {
          this.logger('Downloading external pre-aggregation warning', {
            ...queryOptions,
            error: msg
          });
        });
        tableData.rowStream.pipe(stream);
        tableData.rowStream = stream;
      }
    } else {
      tableData = { rows: await saveCancelFn(client.query(sql, params)) };
    }

    if (!tableData.types && client.queryColumnTypes) {
      tableData.types = await saveCancelFn(client.queryColumnTypes(sql, params));
    }

    return tableData;
  }

  protected async uploadExternalPreAggregation(
    tableData: DownloadTableData,
    newVersionEntry: VersionEntry,
    saveCancelFn: SaveCancelFn,
    queryOptions: QueryOptions
  ) {
    const externalDriver: DriverInterface = await this.externalDriverFactory();
    const table = this.targetTableName(newVersionEntry);

    this.logger('Uploading external pre-aggregation', queryOptions);
    await saveCancelFn(
      externalDriver.uploadTableWithIndexes(
        table,
        tableData.types,
        tableData,
        this.prepareIndexesSql(newVersionEntry, queryOptions),
        this.preAggregation.uniqueKeyColumns,
        queryOptions,
        {
          aggregationsColumns: this.preAggregation.aggregationsColumns,
          createTableIndexes: this.prepareCreateTableIndexes(newVersionEntry),
          sealAt: this.preAggregation.sealAt
        }
      )
    ).catch((error: any) => {
      this.logger('Uploading external pre-aggregation error', {
        ...queryOptions,
        error: error?.stack || error?.message
      });
      throw error;
    });
    this.logger('Uploading external pre-aggregation completed', queryOptions);

    await this.loadCache.fetchTables(this.preAggregation);
    await this.dropOrphanedTables(externalDriver, table, saveCancelFn, true, queryOptions);
  }

  protected async createIndexes(driver: DriverInterface, newVersionEntry: VersionEntry, saveCancelFn: SaveCancelFn, queryOptions: QueryOptions) {
    const indexesSql = this.prepareIndexesSql(newVersionEntry, queryOptions);
    for (let i = 0; i < indexesSql.length; i++) {
      const [query, params] = indexesSql[i].sql;
      await saveCancelFn(driver.query(query, params));
    }
  }

  protected prepareIndexesSql(newVersionEntry: VersionEntry, queryOptions: QueryOptions) {
    if (!this.preAggregation.indexesSql || !this.preAggregation.indexesSql.length) {
      return [];
    }
    return this.preAggregation.indexesSql.map(({ sql, indexName }) => {
      const [query, params] = sql;
      const indexVersionEntry = {
        ...newVersionEntry,
        table_name: indexName
      };
      this.logger('Creating pre-aggregation index', queryOptions);
      const preAggTableToTempTableNames = this.preAggregationsTablesToTempTables as PreAggTableToTempTableNames[];
      const resultingSql = QueryCache.replacePreAggregationTableNames(
        query,
        preAggTableToTempTableNames.concat([
          [this.preAggregation.tableName, { targetTableName: this.targetTableName(newVersionEntry) }],
          [indexName, { targetTableName: this.targetTableName(indexVersionEntry) }]
        ])
      );
      return { sql: [resultingSql, params] };
    });
  }

  protected prepareCreateTableIndexes(newVersionEntry: VersionEntry) {
    if (!this.preAggregation.createTableIndexes || !this.preAggregation.createTableIndexes.length) {
      return [];
    }
    return this.preAggregation.createTableIndexes.map(({ indexName, type, columns }) => {
      const indexVersionEntry = {
        ...newVersionEntry,
        table_name: indexName
      };
      return { indexName: this.targetTableName(indexVersionEntry), type, columns };
    });
  }

  private async withDropLock<T>(lockKey: string, lockFn: () => MaybeCancelablePromise<T>): Promise<boolean> {
    return this.queryCache.withLock(lockKey, 60 * 5, lockFn);
  }

  protected async dropOrphanedTables(
    client: DriverInterface,
    justCreatedTable: string,
    saveCancelFn: SaveCancelFn,
    external: boolean,
    queryOptions: QueryOptions
  ) {
    await this.preAggregations.addTableUsed(justCreatedTable);

    return this.withDropLock(this.dropOrphanedLockKey(external), async () => {
      this.logger('Dropping orphaned tables', { ...queryOptions, external });
      const actualTables = await client.getTablesQuery(
        this.preAggregation.preAggregationsSchema,
      );
      const versionEntries = tablesToVersionEntries(
        this.preAggregation.preAggregationsSchema,
        actualTables,
      );
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
          (v: VersionEntry) => (
            new Date().getTime() - v.last_updated_at <
            this.structureVersionPersistTime * 1000
          )
        ),
        R.groupBy(v => `${v.table_name}_${v.structure_version}`),
        R.toPairs,
        R.map(p => p[1][0])
      )(versionEntries);

      const refreshEndReached = await this.preAggregations.getRefreshEndReached();
      const toSave =
        this.preAggregations.dropPreAggregationsWithoutTouch && refreshEndReached
          ? (await this.preAggregations.tablesUsed())
            .concat(await this.preAggregations.tablesTouched())
            .concat([justCreatedTable])
          : (await this.preAggregations.tablesUsed())
            .concat(structureVersionsToSave.map(v => this.targetTableName(v)))
            .concat(versionEntriesToSave.map(v => this.targetTableName(v)))
            .concat([justCreatedTable]);
      const toDrop = actualTables
        .map(t => `${this.preAggregation.preAggregationsSchema}.${t.table_name || t.TABLE_NAME}`)
        .filter(t => toSave.indexOf(t) === -1);

      await Promise.all(toDrop.map(table => saveCancelFn(client.dropTable(table))));
      this.logger('Dropping orphaned tables completed', {
        ...queryOptions,
        external,
        tablesToDrop: JSON.stringify(toDrop),
      });
    });
  }

  private dropOrphanedLockKey(external: boolean) {
    return external
      ? 'drop-orphaned-tables-external'
      : `drop-orphaned-tables:${this.preAggregation.dataSource}`;
  }
}
