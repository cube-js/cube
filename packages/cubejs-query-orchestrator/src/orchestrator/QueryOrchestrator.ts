import R from 'ramda';
import { getEnv } from '@cubejs-backend/shared';

import { QueryCache } from './QueryCache';
import { PreAggregations } from './PreAggregations';
import { RedisPool, RedisPoolOptions } from './RedisPool';
import { DriverFactory, DriverFactoryByDataSource } from './DriverFactory';

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

  protected readonly redisPool: RedisPool|undefined;

  protected readonly rollupOnlyMode: boolean;

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

    if (!['redis', 'memory'].includes(cacheAndQueueDriver)) {
      throw new Error('Only \'redis\' or \'memory\' are supported for cacheAndQueueDriver option');
    }

    const redisPool = cacheAndQueueDriver === 'redis' ? new RedisPool(options.redisPoolOptions) : undefined;
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
      }
    );

    this.preAggregations = new PreAggregations(
      this.redisPrefix, this.driverFactory, this.logger, this.queryCache, {
        externalDriverFactory,
        cacheAndQueueDriver,
        redisPool,
        continueWaitTimeout,
        skipExternalCacheAndQueue,
        ...options.preAggregationsOptions
      }
    );
  }

  public async fetchQuery(queryBody: any): Promise<any> {
    const preAggregationsTablesToTempTables = await this.preAggregations.loadAllPreAggregationsIfNeeded(queryBody);

    const usedPreAggregations = R.fromPairs(preAggregationsTablesToTempTables);
    if (this.rollupOnlyMode && Object.keys(usedPreAggregations).length === 0) {
      throw new Error('No pre-aggregation exists for that query');
    }

    if (!queryBody.query) {
      return {
        usedPreAggregations
      };
    }

    const result = await this.queryCache.cachedQueryResult(
      queryBody,
      preAggregationsTablesToTempTables
    );

    return {
      ...result,
      dataSource: queryBody.dataSource,
      external: queryBody.external,
      usedPreAggregations
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
    const partitionsByTableName = preAggregations
      .map(p => p.partitions)
      .reduce(flatFn, [])
      .reduce((obj, partition) => {
        if (partition && partition.sql) obj[partition.sql.tableName] = partition;
        return obj;
      }, {});

    return versionEntries
      .reduce(flatFn, [])
      .filter((versionEntry) => {
        const partition = partitionsByTableName[versionEntry.table_name];
        return partition && versionEntry.structure_version === PreAggregations.structureVersion(partition.sql);
      });
  }

  public async getPreAggregationPreview(requestId, preAggregation, versionEntry) {
    if (!preAggregation.sql) return [];

    const { previewSql, tableName, external, dataSource } = preAggregation.sql;
    const targetTableName = PreAggregations.targetTableName(versionEntry);
    const querySql = QueryCache.replacePreAggregationTableNames(previewSql, [[tableName, { targetTableName }]]);
    const query = querySql && querySql[0];

    const data = query && await this.fetchQuery({
      continueWait: true,
      external,
      dataSource,
      query,
      requestId
    });

    return data || [];
  }

  public async expandPartitionsInPreAggregations(queryBody) {
    return this.preAggregations.expandPartitionsInPreAggregations(queryBody);
  }

  public async getPreAggregationQueueStates() {
    return this.preAggregations.getQueueState();
  }
}
