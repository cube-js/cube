import * as stream from 'stream';
import R from 'ramda';
import { getEnv } from '@cubejs-backend/shared';
import { CubeStoreDriver } from '@cubejs-backend/cubestore-driver';

import { QueryCache, QueryBody, TempTable } from './QueryCache';
import { PreAggregations, PreAggregationDescription, getLastUpdatedAtTimestamp } from './PreAggregations';
import { RedisPool, RedisPoolOptions } from './RedisPool';
import { DriverFactory, DriverFactoryByDataSource } from './DriverFactory';
import { RedisQueueEventsBus } from './RedisQueueEventsBus';
import { LocalQueueEventsBus } from './LocalQueueEventsBus';

export type CacheAndQueryDriverType = 'redis' | 'memory' | 'cubestore';

export enum DriverType {
  External = 'external',
  Internal = 'internal',
  Cache = 'cache',
}

export interface QueryOrchestratorOptions {
  externalDriverFactory?: DriverFactory;
  cacheAndQueueDriver?: CacheAndQueryDriverType;
  redisPoolOptions?: RedisPoolOptions;
  queryCacheOptions?: any;
  preAggregationsOptions?: any;
  rollupOnlyMode?: boolean;
  continueWaitTimeout?: number;
  skipExternalCacheAndQueue?: boolean;
}

export class QueryOrchestrator {
  protected readonly queryCache: QueryCache;

  protected readonly preAggregations: PreAggregations;

  protected readonly redisPool: RedisPool | undefined;

  protected readonly rollupOnlyMode: boolean;

  private queueEventsBus: RedisQueueEventsBus | LocalQueueEventsBus;

  private readonly cacheAndQueueDriver: string;

  public constructor(
    protected readonly redisPrefix: string,
    protected readonly driverFactory: DriverFactoryByDataSource,
    protected readonly logger: any,
    options: QueryOrchestratorOptions = {}
  ) {
    this.rollupOnlyMode = options.rollupOnlyMode;

    const cacheAndQueueDriver = options.cacheAndQueueDriver || getEnv('cacheAndQueueDriver') || (
      (getEnv('nodeEnv') === 'production' || getEnv('redisUrl') || getEnv('redisUseIORedis'))
        ? 'redis'
        : 'memory'
    );
    this.cacheAndQueueDriver = cacheAndQueueDriver;

    if (!['redis', 'memory', 'cubestore'].includes(cacheAndQueueDriver)) {
      throw new Error('Only \'redis\', \'memory\' or \'cubestore\' are supported for cacheAndQueueDriver option');
    }

    const { externalDriverFactory, continueWaitTimeout, skipExternalCacheAndQueue } = options;

    const redisPool = cacheAndQueueDriver === 'redis' ? new RedisPool(options.redisPoolOptions) : undefined;
    this.redisPool = redisPool;

    const cubeStoreDriverFactory = cacheAndQueueDriver === 'cubestore' ? async () => {
      const externalDriver = await externalDriverFactory();
      if (externalDriver instanceof CubeStoreDriver) {
        return externalDriver;
      }

      throw new Error('It`s not possible to use CubeStore as queue & cache driver without using it as external');
    } : undefined;

    this.queryCache = new QueryCache(
      this.redisPrefix,
      driverFactory,
      this.logger,
      {
        externalDriverFactory,
        cacheAndQueueDriver,
        redisPool,
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
        redisPool,
        cubeStoreDriverFactory,
        continueWaitTimeout,
        skipExternalCacheAndQueue,
        ...options.preAggregationsOptions,
        getQueueEventsBus:
          getEnv('preAggregationsQueueEventsBus') &&
          this.getQueueEventsBus.bind(this)
      }
    );
  }

  private getQueueEventsBus() {
    if (!this.queueEventsBus) {
      const isRedis = this.cacheAndQueueDriver === 'redis';
      this.queueEventsBus = isRedis ?
        new RedisQueueEventsBus({ redisPool: this.redisPool }) :
        new LocalQueueEventsBus();
    }
    return this.queueEventsBus;
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
    await this.queryCache.forceReconcile(datasource);
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

    const usedPreAggregations = R.pipe(
      R.fromPairs,
      R.map((pa: TempTable) => ({
        targetTableName: pa.targetTableName,
        refreshKeyValues: pa.refreshKeyValues,
        lastUpdatedAt: pa.lastUpdatedAt,
      })),
    )(
      preAggregationsTablesToTempTables as unknown as [
        number, // TODO: we actually have a string here
        {
          buildRangeEnd: string,
          lastUpdatedAt: number,
          queryKey: unknown,
          refreshKeyValues: [{
            'refresh_key': string,
          }][],
          targetTableName: string,
          type: string,
        },
      ][]
    );

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
      return qcQueue.getQueryStage(QueryCache.queryCacheKey(queryBody));
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
        preAggregation.dataSource = (partition && partition.dataSource) || 'default';
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

  public async subscribeQueueEvents(id, callback) {
    return this.getQueueEventsBus().subscribe(id, callback);
  }

  public async unSubscribeQueueEvents(id) {
    return this.getQueueEventsBus().unsubscribe(id);
  }

  public async updateRefreshEndReached() {
    return this.preAggregations.updateRefreshEndReached();
  }
}
