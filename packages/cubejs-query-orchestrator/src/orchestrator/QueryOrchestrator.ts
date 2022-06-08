import R from 'ramda';
import { getEnv } from '@cubejs-backend/shared';

import { QueryCache } from './QueryCache';
import { PreAggregations, PreAggregationDescription, getLastUpdatedAtTimestamp } from './PreAggregations';
import { RedisPool, RedisPoolOptions } from './RedisPool';
import { DriverFactory, DriverFactoryByDataSource } from './DriverFactory';
import { RedisQueueEventsBus } from './RedisQueueEventsBus';
import { LocalQueueEventsBus } from './LocalQueueEventsBus';

export type CacheAndQueryDriverType = 'redis' | 'memory';

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

    if (!['redis', 'memory'].includes(cacheAndQueueDriver)) {
      throw new Error('Only \'redis\' or \'memory\' are supported for cacheAndQueueDriver option');
    }

    const redisPool = cacheAndQueueDriver === 'redis' ? new RedisPool(options.redisPoolOptions) : undefined;
    this.redisPool = redisPool;
    const { externalDriverFactory, continueWaitTimeout, skipExternalCacheAndQueue } = options;

    this.queryCache = new QueryCache(
      this.redisPrefix,
      driverFactory,
      this.logger,
      {
        externalDriverFactory,
        cacheAndQueueDriver,
        redisPool,
        continueWaitTimeout,
        skipExternalCacheAndQueue,
        ...options.queryCacheOptions,
        preAggregationsQueueOptions: options.preAggregationsOptions.queueOptions,
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
   * Force reconcile queue logic to be executed.
   */
  public async forceReconcile(datasource = 'default') {
    await this.queryCache.forceReconcile(datasource);
  }

  public async fetchQuery(queryBody: any): Promise<any> {
    const { preAggregationsTablesToTempTables, values } = await this.preAggregations.loadAllPreAggregationsIfNeeded(queryBody);

    if (values) {
      queryBody = {
        ...queryBody,
        values
      };
    }

    const usedPreAggregations = R.fromPairs(preAggregationsTablesToTempTables);
    if (this.rollupOnlyMode && Object.keys(usedPreAggregations).length === 0) {
      throw new Error('No pre-aggregation table has been built for this query yet. Please check your refresh worker configuration if it persists.');
    }

    let lastRefreshTimestamp = getLastUpdatedAtTimestamp(preAggregationsTablesToTempTables.map(pa => new Date(pa[1].lastUpdatedAt)));

    if (!queryBody.query) {
      return {
        usedPreAggregations,
        lastRefreshTime: lastRefreshTimestamp && new Date(lastRefreshTimestamp),
      };
    }

    const result = await this.queryCache.cachedQueryResult(
      queryBody,
      preAggregationsTablesToTempTables
    );

    lastRefreshTimestamp = getLastUpdatedAtTimestamp([lastRefreshTimestamp, result.lastRefreshTime?.getTime()]);

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
        const queue = this.preAggregations.getQueue(dataSource);
        preAggregationsQueryStageStateByDataSource[dataSource] = queue.fetchQueryStageState();
      }
      return preAggregationsQueryStageStateByDataSource[dataSource];
    };

    const pendingPreAggregationIndex =
      (await Promise.all(
        (queryBody.preAggregations || [])
          .map(async p => this.preAggregations.getQueue(p.dataSource).getQueryStage(
            PreAggregations.preAggregationQueryCacheKey(p), 10, await preAggregationsQueryStageState(p.dataSource)
          ))
      )).findIndex(p => !!p);

    if (pendingPreAggregationIndex === -1) {
      return this.queryCache.getQueue(queryBody.dataSource).getQueryStage(QueryCache.queryCacheKey(queryBody));
    }

    const preAggregation = queryBody.preAggregations[pendingPreAggregationIndex];
    const preAggregationStage = await this.preAggregations.getQueue(preAggregation.dataSource).getQueryStage(
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

  public async testConnections() {
    // @todo Possible, We will allow to use different drivers for cache and queue, dont forget to add both
    return this.queryCache.testConnection();
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
}
