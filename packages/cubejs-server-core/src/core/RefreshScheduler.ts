import R from 'ramda';
import { v4 as uuidv4 } from 'uuid';
import { Required } from '@cubejs-backend/shared';
import { PreAggregationDescription } from '@cubejs-backend/query-orchestrator';

import { CubejsServerCore } from './server';
import { CompilerApi } from './CompilerApi';
import { RequestContext } from './types';

export interface ScheduledRefreshOptions {
  timezone?: string,
  timezones?: string[],
  throwErrors?: boolean;
  preAggregationsWarmup?: boolean;
  concurrency?: number,
  queryIteratorState?: any;
  workerIndices?: number[];
}

type ScheduledRefreshQueryingOptions = Required<ScheduledRefreshOptions, 'concurrency' | 'workerIndices'> & {
  contextSymbols: {
    securityContext: object,
  };
  cacheOnly?: boolean,
  timezones: string[]
};

type PreAggregationsQueryingOptions = {
  metadata?: any,
  timezones: string[],
  preAggregations: {
    id: string,
    cacheOnly?: boolean,
    partitions?: string[]
  }[],
  forceBuildPreAggregations?: boolean,
  throwErrors?: boolean
};

type RefreshQueries = {
  error?: string,
  partitions: PreAggregationDescription[],
  groupedPartitions: PreAggregationDescription[][],
};

export class RefreshScheduler {
  public constructor(
    protected readonly serverCore: CubejsServerCore,
  ) {
  }

  protected async refreshQueriesForPreAggregation(
    context,
    compilerApi: CompilerApi,
    preAggregation,
    queryingOptions: ScheduledRefreshQueryingOptions
  ): Promise<RefreshQueries> {
    const baseQuery = await this.baseQueryForPreAggregation(compilerApi, preAggregation, queryingOptions);
    const baseQuerySql = await compilerApi.getSql(baseQuery);
    const preAggregationDescriptionList = baseQuerySql.preAggregations;
    const preAggregationDescription = preAggregationDescriptionList.find(p => p.preAggregationId === preAggregation.id);
    const orchestratorApi = this.serverCore.getOrchestratorApi(context);
    const preAggregationsLoadCacheByDataSource = {};

    // Return a empty array for cases with 2 same pre-aggregations but with different partitionGranularity
    // Only the most detailed pre-aggregations will be use
    if (!preAggregationDescription) {
      return {
        error: 'Unused pre-aggregation',
        partitions: [],
        groupedPartitions: []
      };
    }

    const queryBody = {
      preAggregations: preAggregationDescriptionList,
      preAggregationsLoadCacheByDataSource,
      requestId: context.requestId
    };

    if (queryingOptions.cacheOnly) {
      const [{ isCached }]: any = await orchestratorApi.checkPartitionsBuildRangeCache(queryBody);
      if (!isCached) {
        return {
          error: 'Waiting for cache',
          partitions: [],
          groupedPartitions: []
        };
      }
    }

    const partitions = await orchestratorApi.expandPartitionsInPreAggregations(queryBody);

    return {
      error: null,
      partitions: partitions.preAggregations,
      groupedPartitions: partitions.groupedPartitionPreAggregations,
    };
  }

  protected async baseQueryForPreAggregation(
    compilerApi: CompilerApi,
    preAggregation,
    queryingOptions: ScheduledRefreshQueryingOptions
  ) {
    const compilers = await compilerApi.getCompilers();
    const query = compilerApi.createQueryByDataSource(compilers, queryingOptions);
    if (preAggregation.preAggregation.partitionGranularity || preAggregation.preAggregation.type === 'rollup') {
      return { ...queryingOptions, ...preAggregation.references };
    } else if (preAggregation.preAggregation.type === 'originalSql') {
      const cubeFromPath = query.cubeEvaluator.cubeFromPath(preAggregation.cube);
      const measuresCount = Object.keys(cubeFromPath.measures || {}).length;
      const dimensionsCount = Object.keys(cubeFromPath.dimensions || {}).length;
      if (measuresCount === 0 && dimensionsCount === 0) {
        throw new Error(
          `Can't refresh pre-aggregation without measures and dimensions: ${preAggregation.preAggregationName}`
        );
      }
      return {
        ...queryingOptions,
        ...(
          measuresCount &&
            { measures: [`${preAggregation.cube}.${Object.keys(cubeFromPath.measures)[0]}`] }
        ),
        ...(
          dimensionsCount &&
            { dimensions: [`${preAggregation.cube}.${Object.keys(cubeFromPath.dimensions)[0]}`] }
        )
      };
    } else {
      throw new Error(
        `Scheduled refresh is unsupported for ${preAggregation.preAggregation.type} of ${preAggregation.preAggregationName}`
      );
    }
  }

  public async runScheduledRefresh(ctx: RequestContext | null, options: Readonly<ScheduledRefreshOptions>) {
    const context: RequestContext = {
      authInfo: null,
      ...ctx,
      securityContext: ctx?.securityContext ? ctx.securityContext : {},
      requestId: `scheduler-${ctx && ctx.requestId || uuidv4()}`,
    };

    const queryingOptions: ScheduledRefreshQueryingOptions = {
      timezones: [options.timezone || 'UTC'],
      ...options,
      concurrency: options.concurrency || 1,
      workerIndices: options.workerIndices || R.range(0, options.concurrency || 1),
      contextSymbols: {
        securityContext: context.securityContext,
      },
    };

    this.serverCore.logger('Refresh Scheduler Run', {
      securityContext: context.securityContext,
      requestId: context.requestId
    });

    try {
      const compilerApi = this.serverCore.getCompilerApi(context);
      if (queryingOptions.preAggregationsWarmup) {
        await this.refreshPreAggregations(context, compilerApi, queryingOptions);
      } else {
        await Promise.all([
          this.refreshCubesRefreshKey(context, compilerApi, queryingOptions),
          this.refreshPreAggregations(context, compilerApi, queryingOptions)
        ]);
      }
      return {
        finished: true
      };
    } catch (e: any) {
      if (e.error !== 'Continue wait') {
        this.serverCore.logger('Refresh Scheduler Error', {
          error: e.error || e.stack || e.toString(),
          securityContext: context.securityContext,
          requestId: context.requestId
        });
      }

      if (options.throwErrors) {
        throw e;
      }
    }
    return { finished: false };
  }

  protected async refreshCubesRefreshKey(
    context: RequestContext,
    compilerApi: CompilerApi,
    queryingOptions: ScheduledRefreshQueryingOptions
  ) {
    const compilers = await compilerApi.getCompilers();
    const queryForEvaluation = compilerApi.createQueryByDataSource(compilers, {});

    await Promise.all(queryForEvaluation.cubeEvaluator.cubeNames().map(async cube => {
      const cubeFromPath = queryForEvaluation.cubeEvaluator.cubeFromPath(cube);
      const measuresCount = Object.keys(cubeFromPath.measures || {}).length;
      const dimensionsCount = Object.keys(cubeFromPath.dimensions || {}).length;
      if (measuresCount === 0 && dimensionsCount === 0) {
        return;
      }
      await Promise.all(queryingOptions.timezones.map(async timezone => {
        const query = {
          ...queryingOptions,
          ...(
            measuresCount &&
            { measures: [`${cube}.${Object.keys(cubeFromPath.measures)[0]}`] }
          ),
          ...(
            dimensionsCount &&
            { dimensions: [`${cube}.${Object.keys(cubeFromPath.dimensions)[0]}`] }
          ),
          timezone
        };
        const sqlQuery = await compilerApi.getSql(query);
        const orchestratorApi = this.serverCore.getOrchestratorApi(context);
        await orchestratorApi.executeQuery({
          ...sqlQuery,
          sql: null,
          preAggregations: [],
          continueWait: true,
          renewQuery: true,
          requestId: context.requestId,
          scheduledRefresh: true,
          loadRefreshKeysOnly: true
        });
      }));
    }));
  }

  public async preAggregationPartitions(
    context,
    queryingOptions: PreAggregationsQueryingOptions
  ) {
    const compilerApi = this.serverCore.getCompilerApi(context);
    const preAggregationsQueryingOptions = queryingOptions.preAggregations.reduce((obj, p) => {
      obj[p.id] = p;
      return obj;
    }, {});

    const preAggregations = await compilerApi.preAggregations({
      preAggregationIds: Object.keys(preAggregationsQueryingOptions)
    });

    return Promise.all(preAggregations.map(async preAggregation => {
      const { timezones } = queryingOptions;
      const { partitions: partitionsFilter, cacheOnly } = preAggregationsQueryingOptions[preAggregation.id] || {};

      const isRollupJoin = preAggregation?.preAggregation?.type === 'rollupJoin';

      const queriesForPreAggregation: RefreshQueries[] = !isRollupJoin && (await Promise.all(
        timezones.map(async timezone => this.refreshQueriesForPreAggregation(
          context,
          compilerApi,
          preAggregation,
          // TODO: timezones, concurrency, workerIndices???
          {
            timezones: undefined,
            concurrency: undefined,
            workerIndices: undefined,
            timezone,
            cacheOnly,
            contextSymbols: {
              securityContext: context.securityContext || {},
            }
          }
        ))
      )) || [];

      const partitionsWithDependencies = queriesForPreAggregation
        .map(query => {
          let dependencies: PreAggregationDescription[] = [];
          for (let i = 0; i < query.groupedPartitions.length - 1; i++) {
            dependencies = dependencies.concat(query.groupedPartitions[i]);
          }
          return {
            dependencies,
            partitions: query.groupedPartitions.length && query.groupedPartitions[query.groupedPartitions.length - 1]
              .filter(p => !partitionsFilter || !partitionsFilter.length || partitionsFilter.includes(p?.tableName)) || []
          };
        });

      const partitions = partitionsWithDependencies.map(p => p.partitions).reduce((a, b) => a.concat(b), []);

      const { invalidateKeyQueries, preAggregationStartEndQueries } = partitionsWithDependencies[0]?.partitions[0] || {};
      const [[refreshRangeStart], [refreshRangeEnd]] = preAggregationStartEndQueries || [[], []];
      const [[refreshKey]] = invalidateKeyQueries || [[]];

      const refreshesSqlMap = {
        refreshKey,
        refreshRangeStart,
        refreshRangeEnd
      };
      const preAggRefreshesWithSql = {};
      Object.keys(refreshesSqlMap).forEach((field) => {
        if (preAggregation?.preAggregation[field]?.sql) {
          preAggRefreshesWithSql[field] = {
            ...preAggregation.preAggregation[field],
            sql: refreshesSqlMap[field]
          };
        }
      });

      const errors = [...new Set(queriesForPreAggregation.map(q => q?.error).filter(e => e))];

      return {
        timezones,
        invalidateKeyQueries,
        preAggregationStartEndQueries,
        preAggregation: {
          ...preAggregation,
          preAggregation: {
            ...preAggregation?.preAggregation,
            ...preAggRefreshesWithSql
          }
        },
        partitions,
        errors,
        partitionsWithDependencies
      };
    }));
  }

  protected async roundRobinRefreshPreAggregationsQueryIterator(context, compilerApi: CompilerApi, queryingOptions) {
    const { timezones, preAggregationsWarmup } = queryingOptions;
    const scheduledPreAggregations = await compilerApi.scheduledPreAggregations();

    let preAggregationCursor = 0;
    let timezoneCursor = 0;
    let partitionCursor = 0;
    let partitionCounter = 0;

    const queriesCache: { [key: string]: Promise<PreAggregationDescription[][]> } = {};
    const finishedPartitions = {};
    scheduledPreAggregations.forEach((p, pi) => {
      timezones.forEach((t, ti) => {
        finishedPartitions[`${pi}_${ti}`] = false;
      });
    });
    const queriesForPreAggregation = async (preAggregationIndex, timezone) => {
      const key = `${preAggregationIndex}_${timezone}`;
      if (!queriesCache[key]) {
        const preAggregation = scheduledPreAggregations[preAggregationIndex];
        queriesCache[key] = this.refreshQueriesForPreAggregation(
          context, compilerApi, preAggregation, { ...queryingOptions, timezone }
        ).then(
          ({ groupedPartitions }) => (groupedPartitions[groupedPartitions.length - 1] || []).map(partition => {
            let cascadedPartitions: PreAggregationDescription[] = [];
            for (let j = 0; j < groupedPartitions.length - 1; j++) {
              cascadedPartitions = cascadedPartitions.concat(groupedPartitions[j]);
            }
            cascadedPartitions.push(partition);
            return cascadedPartitions;
          })
        ).catch(e => {
          delete queriesCache[key];
          throw e;
        });
      }
      return queriesCache[key];
    };

    const advance = async () => {
      const initialPreAggregationCursor = preAggregationCursor;
      const initialTimezoneCursor = timezoneCursor;
      const initialPartitionCursor = partitionCursor;
      const initialPartitionCounter = partitionCounter;
      try {
        preAggregationCursor += 1;
        if (preAggregationCursor >= scheduledPreAggregations.length) {
          preAggregationCursor = 0;
          timezoneCursor += 1;
        }

        if (timezoneCursor >= timezones.length) {
          timezoneCursor = 0;
          partitionCursor += 1;
        }

        const queries = await queriesForPreAggregation(preAggregationCursor, timezones[timezoneCursor]);
        if (partitionCursor < queries.length) {
          partitionCounter += 1;
          return true;
        } else {
          finishedPartitions[`${preAggregationCursor}_${timezoneCursor}`] = true;
          return false;
        }
      } catch (e) {
        preAggregationCursor = initialPreAggregationCursor;
        timezoneCursor = initialTimezoneCursor;
        partitionCursor = initialPartitionCursor;
        partitionCounter = initialPartitionCounter;
        throw e;
      }
    };

    return {
      partitionCounter: () => partitionCounter,
      advance: async () => {
        while (Object.keys(finishedPartitions).find(k => !finishedPartitions[k])) {
          if (await advance()) {
            return true;
          }
        }
        return false;
      },
      current: async () => {
        if (!scheduledPreAggregations[preAggregationCursor]) {
          return null;
        }
        const queries = await queriesForPreAggregation(preAggregationCursor, timezones[timezoneCursor]);
        if (partitionCursor < queries.length) {
          const queryCursor = queries.length - 1 - partitionCursor;
          const partitions = queries[queryCursor];
          return {
            preAggregations: partitions.map(partition => ({
              ...partition,
              priority: preAggregationsWarmup ? 1 : queryCursor - queries.length
            })),
            continueWait: true,
            renewQuery: true,
            requestId: context.requestId,
            timezone: timezones[timezoneCursor],
            scheduledRefresh: true,
          };
        } else {
          return null;
        }
      }
    };
  }

  protected async refreshPreAggregations(
    context: RequestContext,
    compilerApi: CompilerApi,
    queryingOptions: ScheduledRefreshQueryingOptions
  ) {
    const { securityContext } = context;
    const { queryIteratorState, concurrency, workerIndices } = queryingOptions;

    const preAggregationsLoadCacheByDataSource = {};
    return Promise.all(R.range(0, concurrency)
      .filter(workerIndex => workerIndices.indexOf(workerIndex) !== -1)
      .map(async workerIndex => {
        const queryIteratorStateKey = JSON.stringify({ ...securityContext, workerIndex });
        const queryIterator = queryIteratorState && queryIteratorState[queryIteratorStateKey] ||
          (await this.roundRobinRefreshPreAggregationsQueryIterator(
            context, compilerApi, queryingOptions
          ));
        if (queryIteratorState) {
          queryIteratorState[queryIteratorStateKey] = queryIterator;
        }
        for (;;) {
          const currentQuery = await queryIterator.current();
          if (currentQuery && queryIterator.partitionCounter() % concurrency === workerIndex) {
            const orchestratorApi = this.serverCore.getOrchestratorApi(context);
            await orchestratorApi.executeQuery({ ...currentQuery, preAggregationsLoadCacheByDataSource });
          }
          const hasNext = await queryIterator.advance();
          if (!hasNext) {
            return;
          }
        }
      }));
  }

  public async buildPreAggregations(
    context: RequestContext,
    queryingOptions: PreAggregationsQueryingOptions
  ) {
    const orchestratorApi = this.serverCore.getOrchestratorApi(context);
    const preAggregations = await this.preAggregationPartitions(context, queryingOptions);
    const preAggregationsLoadCacheByDataSource = {};

    const promise = Promise.all(preAggregations.map(async (p: any) => {
      const { partitionsWithDependencies } = p;
      return Promise.all(partitionsWithDependencies.map(({ partitions, dependencies }) => (
        Promise.all(partitions.map(async (partition) => {
          await orchestratorApi.executeQuery({
            preAggregations: dependencies.concat([partition]),
            continueWait: true,
            renewQuery: true,
            forceBuildPreAggregations: queryingOptions.forceBuildPreAggregations != null ? queryingOptions.forceBuildPreAggregations : true,
            orphanedTimeout: 60 * 60,
            requestId: context.requestId,
            timezone: partition.timezone,
            scheduledRefresh: false,
            preAggregationsLoadCacheByDataSource,
            metadata: queryingOptions.metadata
          });
        }))
      )));
    })).catch(e => {
      if (e.error !== 'Continue wait') {
        this.serverCore.logger('Manual Build Pre-aggregations Error', {
          error: e.error || e.stack || e.toString(),
          securityContext: context.securityContext,
          requestId: context.requestId
        });
      }
      if (queryingOptions.throwErrors) {
        throw e;
      }
    });

    if (queryingOptions.throwErrors) {
      return promise;
    }

    return true;
  }
}
