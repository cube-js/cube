import * as stream from 'stream';
import R from 'ramda';
import { getEnv } from '@cubejs-backend/shared';
import { CubeStoreDriver } from '@cubejs-backend/cubestore-driver';
import {
  QuerySchemasResult,
  QueryTablesResult,
  QueryColumnsResult,
  QueryKey
} from '@cubejs-backend/base-driver';

import { QueryCache, QueryBody, TempTable, PreAggTableToTempTable, QueryWithParams, CacheKey } from './QueryCache';
import { PreAggregations, PreAggregationDescription, getLastUpdatedAtTimestamp } from './PreAggregations';
import { DriverFactory, DriverFactoryByDataSource } from './DriverFactory';
import { QueryStream } from './QueryStream';

export type CacheAndQueryDriverType = 'memory' | 'cubestore' | /** removed, used for exception */ 'redis';

export enum DriverType {
  External = 'external',
  Internal = 'internal',
  Cache = 'cache',
}

export enum MetadataOperationType {
  GET_SCHEMAS = 'GET_SCHEMAS',
  GET_TABLES_FOR_SCHEMAS = 'GET_TABLES_FOR_SCHEMAS',
  GET_COLUMNS_FOR_TABLES = 'GET_COLUMNS_FOR_TABLES'
}

export interface QueryOrchestratorOptions {
  externalDriverFactory?: DriverFactory;
  cacheAndQueueDriver?: CacheAndQueryDriverType;
  queryCacheOptions?: any;
  preAggregationsOptions?: any;
  rollupOnlyMode?: boolean;
  continueWaitTimeout?: number;
  skipExternalCacheAndQueue?: boolean;
}

function detectQueueAndCacheDriver(options: QueryOrchestratorOptions): CacheAndQueryDriverType {
  if (options.cacheAndQueueDriver) {
    return options.cacheAndQueueDriver;
  }

  const cacheAndQueueDriver = getEnv('cacheAndQueueDriver');
  if (cacheAndQueueDriver) {
    return cacheAndQueueDriver;
  }

  if (getEnv('redisUrl') || getEnv('redisUseIORedis')) {
    return 'redis';
  }

  if (getEnv('nodeEnv') === 'production') {
    return 'cubestore';
  }

  return 'memory';
}

export class QueryOrchestrator {
  protected queryCache: QueryCache;

  protected readonly preAggregations: PreAggregations;

  protected readonly rollupOnlyMode: boolean;

  protected readonly cacheAndQueueDriver: string;

  public constructor(
    protected readonly redisPrefix: string,
    protected readonly driverFactory: DriverFactoryByDataSource,
    protected readonly logger: any,
    options: QueryOrchestratorOptions = {}
  ) {
    this.rollupOnlyMode = options.rollupOnlyMode;
    const cacheAndQueueDriver = detectQueueAndCacheDriver(options);

    if (!['memory', 'cubestore'].includes(cacheAndQueueDriver)) {
      throw new Error(
        `Only 'cubestore' or 'memory' are supported for cacheAndQueueDriver option, passed: ${cacheAndQueueDriver}`
      );
    }

    const { externalDriverFactory, continueWaitTimeout, skipExternalCacheAndQueue } = options;

    this.cacheAndQueueDriver = cacheAndQueueDriver;

    const cubeStoreDriverFactory = cacheAndQueueDriver === 'cubestore' ? async () => {
      if (externalDriverFactory) {
        const externalDriver = await externalDriverFactory();
        if (externalDriver instanceof CubeStoreDriver) {
          return externalDriver;
        }

        throw new Error('It`s not possible to use Cube Store as queue/cache driver without using it as external');
      }

      throw new Error('Cube Store was specified as queue/cache driver. Please set CUBEJS_CUBESTORE_HOST and CUBEJS_CUBESTORE_PORT variables. Please see https://cube.dev/docs/deployment/production-checklist#set-up-cube-store to learn more.');
    } : undefined;

    this.queryCache = new QueryCache(
      this.redisPrefix,
      driverFactory,
      this.logger,
      {
        externalDriverFactory,
        cacheAndQueueDriver,
        cubeStoreDriverFactory,
        continueWaitTimeout,
        skipExternalCacheAndQueue,
        ...options.queryCacheOptions,
      }
    );
    this.preAggregations = new PreAggregations(
      this.redisPrefix,
      this.driverFactory,
      this.logger,
      this.queryCache,
      {
        externalDriverFactory,
        cacheAndQueueDriver,
        cubeStoreDriverFactory,
        continueWaitTimeout,
        skipExternalCacheAndQueue,
        ...options.preAggregationsOptions,
      }
    );
  }

  /**
   * Returns QueryCache instance.
   */
  public getQueryCache(): QueryCache {
    return this.queryCache;
  }

  /**
   * Returns PreAggregations instance.
   */
  public getPreAggregations(): PreAggregations {
    return this.preAggregations;
  }

  /**
   * Force reconcile queue logic to be executed.
   */
  public async forceReconcile(datasource = 'default') {
    // pre-aggregations queue reconcile
    const preaggsQueue = await this.preAggregations.getQueue(datasource);
    if (preaggsQueue) {
      await preaggsQueue.reconcileQueue();
    }

    // queries queue reconcile
    const queryQueue = await this.queryCache.getQueue(datasource);
    if (queryQueue) {
      await queryQueue.reconcileQueue();
    }
  }

  /**
   * Determines whether the partition table is already exists or not.
   */
  public async isPartitionExist(
    request: string,
    external: boolean,
    dataSource = 'default',
    schema: string,
    table: string,
    key: any,
    token: string,
  ): Promise<[boolean, string]> {
    return this.preAggregations.isPartitionExist(
      request,
      external,
      dataSource,
      schema,
      table,
      key,
      token,
    );
  }

  /**
   * Returns stream object which will be used to stream results from
   * the data source if applicable. Throw otherwise.
   *
   * @throw Error
   */
  public async streamQuery(query: QueryBody): Promise<stream.Transform> {
    const {
      preAggregationsTablesToTempTables,
      values,
    } = await this.preAggregations.loadAllPreAggregationsIfNeeded(query);
    query.values = values || query.values;
    const _stream = await this.queryCache.cachedQueryResult(
      query,
      preAggregationsTablesToTempTables,
    );
    return <stream.Transform>_stream;
  }

  /**
   * Push query to the queue, fetch and return result if query takes
   * less than `continueWaitTimeout` seconds, throw `ContinueWaitError`
   * error otherwise.
   *
   * @throw ContinueWaitError
   */
  public async fetchQuery(queryBody: QueryBody): Promise<any> {
    const {
      preAggregationsTablesToTempTables,
      values,
    } = await this.preAggregations.loadAllPreAggregationsIfNeeded(queryBody);

    if (values) {
      queryBody = {
        ...queryBody,
        values
      };
    }

    const usedPreAggregations = R.pipe<
      PreAggTableToTempTable[],
      Record<string, TempTable>,
      Record<string, unknown>
    >(
      R.fromPairs,
      R.mapObjIndexed((pa: TempTable) => ({
        targetTableName: pa.targetTableName,
        refreshKeyValues: pa.refreshKeyValues,
        lastUpdatedAt: pa.lastUpdatedAt,
      })),
    )(preAggregationsTablesToTempTables);

    if (this.rollupOnlyMode && Object.keys(usedPreAggregations).length === 0) {
      throw new Error(
        'No pre-aggregation table has been built for this query yet. ' +
        'Please check your refresh worker configuration if it persists.'
      );
    }

    let lastRefreshTimestamp = getLastUpdatedAtTimestamp(
      preAggregationsTablesToTempTables.map(pa => pa[1].lastUpdatedAt)
    );

    if (!queryBody.query) {
      // We want to return a more convenient and filled object for the following
      // processing for a jobed build query (initialized by the
      // /cubejs-system/v1/pre-aggregations/jobs endpoint).
      if (queryBody.isJob) {
        return preAggregationsTablesToTempTables.map((pa) => ({
          preAggregation: queryBody.preAggregations[0].preAggregationId,
          tableName: pa[0],
          ...pa[1],
        }));
      } else {
        return {
          usedPreAggregations,
          lastRefreshTime: lastRefreshTimestamp && new Date(lastRefreshTimestamp),
        };
      }
    }

    const result = await this.queryCache.cachedQueryResult(
      queryBody,
      preAggregationsTablesToTempTables
    );

    lastRefreshTimestamp = getLastUpdatedAtTimestamp([
      lastRefreshTimestamp,
      result.lastRefreshTime?.getTime()
    ]);

    if (result instanceof QueryStream) {
      // TODO do some wrapper object to provide metadata?
      return result;
    }

    return {
      ...result,
      dataSource: queryBody.dataSource,
      external: queryBody.external,
      usedPreAggregations,
      lastRefreshTime: lastRefreshTimestamp && new Date(lastRefreshTimestamp),
    };
  }

  public async loadRefreshKeys(query) {
    return this.queryCache.loadRefreshKeysFromQuery(query);
  }

  public async queryStage(queryBody: any) {
    const preAggregationsQueryStageStateByDataSource = {};

    const preAggregationsQueryStageState = async (dataSource) => {
      if (!preAggregationsQueryStageStateByDataSource[dataSource]) {
        const queue = await this.preAggregations.getQueue(dataSource);
        preAggregationsQueryStageStateByDataSource[dataSource] = queue.fetchQueryStageState();
      }
      return preAggregationsQueryStageStateByDataSource[dataSource];
    };

    const pendingPreAggregationIndex =
      (await Promise.all(
        (queryBody.preAggregations || [])
          .map(async p => {
            const queue = await this.preAggregations.getQueue(p.dataSource);
            return queue.getQueryStage(
              PreAggregations.preAggregationQueryCacheKey(p),
              10,
              await preAggregationsQueryStageState(p.dataSource),
            );
          })
      )).findIndex(p => !!p);

    if (pendingPreAggregationIndex === -1) {
      const qcQueue = await this.queryCache.getQueue(queryBody.dataSource);
      return qcQueue.getQueryStage(QueryCache.queryCacheKey(queryBody) as QueryKey);
    }

    const preAggregation = queryBody.preAggregations[pendingPreAggregationIndex];
    const paQueue = await this.preAggregations.getQueue(preAggregation.dataSource);
    const preAggregationStage = await paQueue.getQueryStage(
      PreAggregations.preAggregationQueryCacheKey(preAggregation),
      undefined,
      await preAggregationsQueryStageState(preAggregation.dataSource)
    );
    if (!preAggregationStage) {
      return undefined;
    }
    const stageMessage =
      `Building pre-aggregation ${pendingPreAggregationIndex + 1}/${queryBody.preAggregations.length}`;
    if (preAggregationStage.stage.indexOf('queue') !== -1) {
      return { ...preAggregationStage, stage: `${stageMessage}: ${preAggregationStage.stage}` };
    } else {
      return { ...preAggregationStage, stage: stageMessage };
    }
  }

  public resultFromCacheIfExists(queryBody: any) {
    return this.queryCache.resultFromCacheIfExists(queryBody);
  }

  public async testConnections(): Promise<void> {
    // @todo Possible, We will allow to use different drivers for cache and queue, dont forget to add both
    try {
      await this.queryCache.testConnection();
    } catch (e: any) {
      e.driverType = DriverType.Cache;
      throw e;
    }
  }

  public async cleanup() {
    return this.queryCache.cleanup();
  }

  public async getPreAggregationVersionEntries(
    preAggregations: { preAggregation: any, partitions: any[]}[],
    preAggregationsSchema: string,
    requestId: string,
  ) {
    const versionEntries = await this.preAggregations.getVersionEntries(
      preAggregations.map(p => {
        const { preAggregation } = p.preAggregation;
        const partition = p.partitions[0];
        preAggregation.dataSource = partition?.dataSource || 'default';
        preAggregation.preAggregationsSchema = preAggregationsSchema;
        return preAggregation;
      }),
      requestId
    );

    const flatFn = (arrResult: any[], arrItem: any[]) => ([...arrResult, ...arrItem]);
    const structureVersionsByTableName = preAggregations
      .map(p => p.partitions)
      .reduce(flatFn, [])
      .reduce((obj, partition) => {
        if (partition) {
          obj[partition.tableName] = PreAggregations.structureVersion(partition);
        }
        return obj;
      }, {});

    return {
      structureVersionsByTableName,
      versionEntriesByTableName: versionEntries
        .reduce(flatFn, [])
        .filter((versionEntry) => {
          const structureVersion = structureVersionsByTableName[versionEntry.table_name];
          return structureVersion && versionEntry.structure_version === structureVersion;
        })
        .reduce((obj, versionEntry) => {
          if (!obj[versionEntry.table_name]) obj[versionEntry.table_name] = [];
          obj[versionEntry.table_name].push(versionEntry);
          return obj;
        }, {})
    };
  }

  public async getPreAggregationPreview(requestId: string, preAggregation: PreAggregationDescription) {
    if (!preAggregation) return [];
    const [query] = preAggregation.previewSql;
    const { external } = preAggregation;

    const data = await this.fetchQuery({
      dataSource: preAggregation.dataSource,
      continueWait: true,
      query,
      external,
      preAggregations: [
        preAggregation
      ],
      requestId,
    });

    return data || [];
  }

  public async expandPartitionsInPreAggregations(queryBody) {
    return this.preAggregations.expandPartitionsInPreAggregations(queryBody);
  }

  public async checkPartitionsBuildRangeCache(queryBody) {
    return this.preAggregations.checkPartitionsBuildRangeCache(queryBody);
  }

  public async getPreAggregationQueueStates(dataSource = 'default') {
    return this.preAggregations.getQueueState(dataSource);
  }

  public async cancelPreAggregationQueriesFromQueue(queryKeys: string[], dataSource = 'default') {
    return this.preAggregations.cancelQueriesFromQueue(queryKeys, dataSource);
  }

  public async updateRefreshEndReached() {
    return this.preAggregations.updateRefreshEndReached();
  }

  private createMetadataQuery(operation: string, params: Record<string, any>): QueryWithParams {
    return [
      `METADATA:${operation}`,
      // TODO (@MikeNitsenko): Metadata queries need object params like [{ schema, table }]
      // but QueryWithParams expects string[]. This forces JSON.stringify workaround.
      [JSON.stringify(params)],
      { external: false }
    ];
  }

  private async queryDataSourceMetadata<T>(
    operation: MetadataOperationType,
    params: Record<string, any>,
    dataSource: string = 'default',
    options: {
      requestId?: string;
      syncJobId?: string;
      expiration?: number;
    } = {}
  ): Promise<T> {
    const {
      requestId,
      syncJobId,
      expiration = 30 * 24 * 60 * 60,
    } = options;

    const metadataQuery = this.createMetadataQuery(operation, params);
    const cacheKey: CacheKey = syncJobId
      ? [metadataQuery, dataSource, syncJobId]
      : [metadataQuery, dataSource];

    return this.queryCache.cacheQueryResult(
      metadataQuery,
      [],
      cacheKey,
      expiration,
      {
        requestId,
        dataSource,
        forceNoCache: !syncJobId,
        useInMemory: true,
      }
    );
  }

  /**
   * Query the data source for available schemas.
   */
  public async queryDataSourceSchemas(
    dataSource: string = 'default',
    options: {
      requestId?: string;
      syncJobId?: string;
      expiration?: number;
    } = {}
  ): Promise<QuerySchemasResult[]> {
    return this.queryDataSourceMetadata<QuerySchemasResult[]>(
      MetadataOperationType.GET_SCHEMAS,
      {},
      dataSource,
      options
    );
  }

  /**
   * Query the data source for tables within the specified schemas.
   */
  public async queryTablesForSchemas(
    schemas: QuerySchemasResult[],
    dataSource: string = 'default',
    options: {
      requestId?: string;
      syncJobId?: string;
      expiration?: number;
    } = {}
  ): Promise<QueryTablesResult[]> {
    return this.queryDataSourceMetadata<QueryTablesResult[]>(
      MetadataOperationType.GET_TABLES_FOR_SCHEMAS,
      { schemas },
      dataSource,
      options
    );
  }

  /**
   * Query the data source for columns within the specified tables.
   */
  public async queryColumnsForTables(
    tables: QueryTablesResult[],
    dataSource: string = 'default',
    options: {
      requestId?: string;
      syncJobId?: string;
      expiration?: number;
    } = {}
  ): Promise<QueryColumnsResult[]> {
    return this.queryDataSourceMetadata<QueryColumnsResult[]>(
      MetadataOperationType.GET_COLUMNS_FOR_TABLES,
      { tables },
      dataSource,
      options
    );
  }
}
