import R from 'ramda';
import pLimit from 'p-limit';
import { v4 as uuidv4 } from 'uuid';
import crypto from 'crypto';
import { Required } from '@cubejs-backend/shared';
import type { PreAggregationInfo } from '@cubejs-backend/schema-compiler';
import type { NormalizedQuery } from '@cubejs-backend/api-gateway';
import {
  PreAggregationDescription,
  PreAggregationPartitionRangeLoader,
  QueuePriority
} from '@cubejs-backend/query-orchestrator';

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
  dateRange?: [string, string],
  preAggregations: {
    id: string,
    cacheOnly?: boolean,
    metaOnly?: boolean,
    partitions?: string[]
  }[],
  forceBuildPreAggregations?: boolean,
  throwErrors?: boolean,
  preAggregationLoadConcurrency?: number,
};

type RefreshQueries = {
  error?: string,
  partitions: PreAggregationDescription[],
  groupedPartitions: PreAggregationDescription[][],
};

const REFRESH_SQL_FIELDS = ['refreshKey', 'refreshRangeStart', 'refreshRangeEnd'] as const;

type RefreshQueryBody = {
  preAggregations: PreAggregationDescription[],
  preAggregationsLoadCacheByDataSource: {},
  requestId: string,
  compilerCacheFn: (subKey: string[], cacheFn: () => any) => any,
};

type ScheduledQueriesCache = { [key: string]: Promise<PreAggregationDescription[][]> };

type ScheduledRefreshCaches = {
  preAggregationsLoadCacheByDataSource: Record<string, any>,
  queriesCache: ScheduledQueriesCache,
};

const newScheduledRefreshCaches = (): ScheduledRefreshCaches => ({
  preAggregationsLoadCacheByDataSource: {},
  // Keyed by pre-aggregation ids: without a prototype, no key can reach `Object.prototype`
  queriesCache: Object.create(null),
});

type PartitionsWithDependencies = {
  dependencies: PreAggregationDescription[],
  partitions: PreAggregationDescription[],
};

type JobedPreAggregation = {
  preAggregation: string,
  tableName: string,
  targetTableName: string,
  // eslint-disable-next-line camelcase
  refreshKeyValues: { refresh_key: string }[][],
  queryKey: any[],
  lastUpdatedAt: string,
  type: string,
  timezone: string,
  dataSource: string,
};

type PreAggJob = {
  request: string,
  context: { securityContext: any },
  preagg: string,
  table: string,
  target: string,
  structure: string,
  content: string,
  updated: string,
  key: any[],
  status: string;
  timezone: string,
  dataSource: string,
};

/**
 * Fetch `structure` and `content` segments from the table (partition) name and
 * returns an object contained these segments.
 */
function getPreAggJobHashes(table: string) {
  const items = table.split('_');
  return {
    structure: items[items.length - 2],
    content: items[items.length - 3],
  };
}

/**
 * Convert posted build pre-aggs jobs results structure to the jobs list format.
 */
function getPreAggsJobsList(
  context: RequestContext,
  jobedPA: JobedPreAggregation[][][][]
): PreAggJob[] {
  const jobs: PreAggJob[] = [];
  jobedPA.forEach((l1) => {
    l1.forEach((l2) => {
      l2.forEach((l3) => {
        l3.forEach((pa) => {
          jobs.push({
            request: context.requestId,
            context: { securityContext: context.securityContext },
            preagg: pa.preAggregation,
            table: pa.tableName,
            target: pa.targetTableName,
            ...getPreAggJobHashes(pa.targetTableName),
            updated: pa.lastUpdatedAt,
            key: pa.queryKey,
            status: 'posted',
            timezone: pa.timezone,
            dataSource: pa.dataSource,
          });
        });
      });
    });
  });
  return jobs;
}

/**
 * Returns MD5 hash token of the job object.
 */
function getPreAggJobToken(job: PreAggJob) {
  return crypto
    .createHash('md5')
    .update(JSON.stringify(job))
    .digest('hex');
}

export class RefreshScheduler {
  public constructor(
    protected readonly serverCore: CubejsServerCore,
  ) {
  }

  protected async refreshQueriesForPreAggregation(
    context: RequestContext,
    compilerApi: CompilerApi,
    preAggregation: PreAggregationInfo,
    queryingOptions: ScheduledRefreshQueryingOptions
  ): Promise<RefreshQueries> {
    const { error, queryBody } = await this.refreshQueryBodyForPreAggregation(context, compilerApi, preAggregation, queryingOptions);
    if (!queryBody) {
      return {
        error,
        partitions: [],
        groupedPartitions: []
      };
    }

    const orchestratorApi = await this.serverCore.getOrchestratorApi(context);
    const partitions = await orchestratorApi.expandPartitionsInPreAggregations(queryBody);

    return {
      error: null,
      partitions: partitions.preAggregations,
      groupedPartitions: partitions.groupedPartitionPreAggregations,
    };
  }

  /**
   * Compiles the descriptions `refreshQueriesForPreAggregation` expands into partitions.
   */
  protected async refreshQueryBodyForPreAggregation(
    context: RequestContext,
    compilerApi: CompilerApi,
    preAggregation: PreAggregationInfo,
    queryingOptions: ScheduledRefreshQueryingOptions
  ): Promise<{ error: string, queryBody?: undefined } | { error: null, queryBody: RefreshQueryBody }> {
    const baseQuery = await this.baseQueryForPreAggregation(compilerApi, preAggregation, queryingOptions);
    const baseQuerySql = await compilerApi.getSql(baseQuery, { preAggregationsOnly: true });
    const preAggregationDescriptionList = baseQuerySql.preAggregations;
    const preAggregationDescription = preAggregationDescriptionList.find((p: PreAggregationDescription) => p.preAggregationId === preAggregation.id);

    // Return an empty array for cases with 2 same pre-aggregations but with different partitionGranularity
    // Only the most detailed pre-aggregations will be use
    if (!preAggregationDescription) {
      return { error: 'Unused pre-aggregation' };
    }

    const queryBody = {
      preAggregations: preAggregationDescriptionList,
      preAggregationsLoadCacheByDataSource: {},
      requestId: context.requestId,
      compilerCacheFn: await compilerApi.compilerCacheFn(context.requestId, baseQuery, ['expandPartitions']),
    };

    if (queryingOptions.cacheOnly) {
      const orchestratorApi = await this.serverCore.getOrchestratorApi(context);
      const [{ isCached }]: any = await orchestratorApi.checkPartitionsBuildRangeCache(queryBody);
      if (!isCached) {
        return { error: 'Waiting for cache' };
      }
    }

    return { error: null, queryBody };
  }

  protected async baseQueryForPreAggregation(
    compilerApi: CompilerApi,
    preAggregation: PreAggregationInfo,
    queryingOptions: ScheduledRefreshQueryingOptions
  ): Promise<NormalizedQuery> {
    const { cubeEvaluator } = await compilerApi.getCompilers();
    if (preAggregation.preAggregation.partitionGranularity || preAggregation.preAggregation.type === 'rollup') {
      // References carry custom granularity names, which QueryTimeDimensionGranularity doesn't list
      return <NormalizedQuery>{ ...queryingOptions, ...preAggregation.references, preAggregationId: preAggregation.id };
    } else if (preAggregation.preAggregation.type === 'originalSql') {
      const cubeFromPath = cubeEvaluator.cubeFromPath(preAggregation.cube);
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

  /**
   * Evaluate and returns minimal QueryQueue concurrency value.
   */
  protected async getSchedulerConcurrency(
    core: CubejsServerCore,
    context: RequestContext,
  ): Promise<null | number> {
    const orchestratorApi = await core
      .getOrchestratorApi(context);
    const preaggsQueues = orchestratorApi.getQueryOrchestrator()
      .getPreAggregations()
      .getQueues();

    let concurrency: null | number;

    if (Object.keys(preaggsQueues).length === 0) {
      // first execution - no queues
      concurrency = null;
    } else {
      // further executions - queues ready
      const concurrencies: number[] = [];
      Object.keys(preaggsQueues).forEach((name) => {
        concurrencies.push(preaggsQueues[name].getConcurrency());
      });
      concurrency = Math.min(...concurrencies);
    }
    return concurrency;
  }

  public async runScheduledRefresh(ctx: RequestContext | null, options: Readonly<ScheduledRefreshOptions>) {
    const context: RequestContext = {
      authInfo: null,
      ...ctx,
      securityContext: ctx?.securityContext ? ctx.securityContext : {},
      requestId: `scheduler-${ctx?.requestId || uuidv4()}`,
    };

    const concurrency =
      options.concurrency ||
      (await this.getSchedulerConcurrency(this.serverCore, context)) ||
      1;

    const queryingOptions: ScheduledRefreshQueryingOptions = {
      timezones: [options.timezone || 'UTC'],
      ...options,
      concurrency,
      workerIndices: options.workerIndices || R.range(0, concurrency),
      contextSymbols: {
        securityContext: context.securityContext,
      },
    };

    this.serverCore.logger('Refresh Scheduler Run', {
      securityContext: context.securityContext,
      requestId: context.requestId
    });

    try {
      const compilerApi = await this.serverCore.getCompilerApi(context);
      const caches = newScheduledRefreshCaches();
      if (queryingOptions.preAggregationsWarmup) {
        await this.refreshPreAggregations(context, compilerApi, queryingOptions, caches);
      } else {
        // Never throws, so awaiting it can't hide a refresh error. A run that stops at `Continue wait`
        // still ends only once its touch does, so the next run doesn't overlap it.
        const touch = this.touchPreAggregationPartitionsIfDue(context, compilerApi, queryingOptions, caches);

        try {
          await Promise.all([
            this.refreshCubesRefreshKey(context, compilerApi, queryingOptions),
            this.refreshPreAggregations(context, compilerApi, queryingOptions, caches),
          ]);
        } finally {
          await touch;
        }
      }
      await this.forceReconcile(context, compilerApi);
      return {
        finished: true
      };
    } catch (e: any) {
      if (e.error !== 'Continue wait') {
        this.serverCore.logger('Refresh Scheduler Error', {
          error: e.stack || e.error || e.toString(),
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

  /**
   * Runs `touchPreAggregationPartitions` at most once per touch pass interval of this security context and
   * worker split: touches only have to land before they expire, not on every run. Never throws.
   */
  protected async touchPreAggregationPartitionsIfDue(
    context: RequestContext,
    compilerApi: CompilerApi,
    queryingOptions: ScheduledRefreshQueryingOptions,
    caches: ScheduledRefreshCaches
  ): Promise<void> {
    try {
      const orchestratorApi = await this.serverCore.getOrchestratorApi(context);
      const preAggsInstance = orchestratorApi.getQueryOrchestrator().getPreAggregations();
      // Workers on other data model versions sharing this cache store keep their own passes
      const scope = crypto.createHash('md5').update(JSON.stringify({
        dataModelHash: await compilerApi.dataModelHash({ requestId: context.requestId }),
        securityContext: context.securityContext,
        concurrency: queryingOptions.concurrency,
        workerIndices: queryingOptions.workerIndices,
      })).digest('hex');
      if (!(await preAggsInstance.isTouchPassDue(scope))) {
        return;
      }

      // A pass still waiting for partitions is retried on the next run instead of the next interval
      if (await this.touchPreAggregationPartitions(context, compilerApi, queryingOptions, caches)) {
        await preAggsInstance.touchPassCompleted(scope);
      }
    } catch (e: any) {
      this.serverCore.logger('Refresh Scheduler Touch Error', {
        error: e?.stack || e?.error || String(e),
        securityContext: context.securityContext,
        requestId: context.requestId
      });
    }
  }

  /**
   * Touches the built partitions of every rollup/originalSql pre-aggregation, `scheduledRefresh: false` included,
   * so ones the refresh doesn't reach this run aren't dropped. Never throws: errors are logged per pre-aggregation,
   * and the result is false only when a pre-aggregation is still waiting for its partitions.
   */
  protected async touchPreAggregationPartitions(
    context: RequestContext,
    compilerApi: CompilerApi,
    queryingOptions: ScheduledRefreshQueryingOptions,
    { preAggregationsLoadCacheByDataSource, queriesCache }: ScheduledRefreshCaches = newScheduledRefreshCaches()
  ): Promise<boolean> {
    let complete = true;
    const logError = (e: any) => {
      // Only a pending build range is retried on the next run: other errors wait for the next interval
      if (e?.error === 'Continue wait') {
        complete = false;
      } else {
        this.serverCore.logger('Refresh Scheduler Touch Error', {
          error: e?.stack || e?.error || String(e),
          securityContext: context.securityContext,
          requestId: context.requestId
        });
      }
    };

    try {
      const orchestratorApi = await this.serverCore.getOrchestratorApi(context);
      const preAggsInstance = orchestratorApi.getQueryOrchestrator().getPreAggregations();
      // Touches only keep tables from being dropped when this is on
      if (!preAggsInstance.dropPreAggregationsWithoutTouch) {
        return true;
      }

      const { concurrency, workerIndices } = queryingOptions;
      // Only these have tables of their own: see `baseQueryForPreAggregation`. Workers that split
      // the refresh by `workerIndices` split the touch by pre-aggregation too.
      const preAggregations = (await compilerApi.preAggregations())
        .filter(p => p.preAggregation?.type === 'rollup' || p.preAggregation?.type === 'originalSql')
        .filter(p => workerIndices.includes(
          crypto.createHash('md5').update(p.id).digest().readUInt32BE(0) % concurrency
        ));
      // Expanding partitions may run build range queries: don't enqueue them all at once
      const limit = pLimit(queryingOptions.concurrency);

      await Promise.all(preAggregations.flatMap(
        preAggregation => queryingOptions.timezones.map(timezone => limit(async () => {
          try {
            const queries = await this.partitionQueries(
              context, compilerApi, preAggregation, { ...queryingOptions, timezone }, queriesCache
            );
            const partitions = R.uniqBy(p => p.tableName, queries.flat());
            await preAggsInstance.touchPartitions(partitions, context.requestId, preAggregationsLoadCacheByDataSource);
          } catch (e: any) {
            logError(e);
          }
        }))
      ));
    } catch (e: any) {
      logError(e);
    }
    return complete;
  }

  /**
   * Force reconcile queue logic to be executed.
   */
  protected async forceReconcile(
    context: RequestContext,
    compilerApi: CompilerApi,
  ) {
    const orchestratorApi = await this.serverCore.getOrchestratorApi(context);
    const compilers = await compilerApi.getCompilers();

    const { cubeEvaluator } = compilers;
    const processed: string[] = [];

    await Promise.all(cubeEvaluator.cubeNames().map(async (name) => {
      const ds = cubeEvaluator.cubeDataSource(name);
      if (processed.indexOf(ds) === -1) {
        processed.push(ds);
        await orchestratorApi.forceReconcile(ds);
      }
    }));
  }

  protected async refreshCubesRefreshKey(
    context: RequestContext,
    compilerApi: CompilerApi,
    queryingOptions: ScheduledRefreshQueryingOptions
  ) {
    const { cubeEvaluator } = await compilerApi.getCompilers();

    const orchestratorApi = await this.serverCore.getOrchestratorApi(context);
    const queryCache = orchestratorApi.getQueryOrchestrator().getQueryCache();
    const uncachedLocalRefreshKey = queryCache.usesUncachedLocalRefreshKey();

    await Promise.all(cubeEvaluator.cubeNames().map(async cube => {
      const cubeFromPath = cubeEvaluator.cubeFromPath(cube);
      // A query on a view runs the refresh keys of the cubes it reads, which those cubes warm
      // here themselves; a view's own refresh_key isn't one of them.
      if (cubeFromPath.isView) {
        return;
      }

      const measuresCount = Object.keys(cubeFromPath.measures || {}).length;
      const dimensionsCount = Object.keys(cubeFromPath.dimensions || {}).length;
      if (measuresCount === 0 && dimensionsCount === 0) {
        return;
      }

      // Without a threshold, local keys have no entry to warm. With a threshold, warm the
      // shared entry as for SQL keys; its value is computed locally without the queue.
      const sqlRefreshKey = !!cubeFromPath.refreshKey && 'sql' in cubeFromPath.refreshKey;
      if (uncachedLocalRefreshKey && !sqlRefreshKey) {
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
        await orchestratorApi.executeQuery({
          ...sqlQuery,
          preAggregations: [],
          cacheMode: 'must-revalidate',
          requestId: context.requestId,
          scheduledRefresh: true,
          loadRefreshKeysOnly: true,
        });
      }));
    }));
  }

  public async preAggregationPartitions(
    context: RequestContext,
    queryingOptions: PreAggregationsQueryingOptions
  ) {
    const compilerApi = await this.serverCore.getCompilerApi(context);
    const preAggregationsQueryingOptions = queryingOptions.preAggregations.reduce((obj, p) => {
      obj[p.id] = p;
      return obj;
    }, {} as Record<string, PreAggregationsQueryingOptions['preAggregations'][number]>);

    const preAggregations = await compilerApi.preAggregations({
      preAggregationIds: Object.keys(preAggregationsQueryingOptions)
    });

    const loadConcurrency = pLimit(queryingOptions.preAggregationLoadConcurrency || 1);

    return Promise.all(preAggregations.map(preAggregation => async () => {
      const { timezones } = queryingOptions;
      const { partitions: partitionsFilter, cacheOnly, metaOnly } = preAggregationsQueryingOptions[preAggregation.id] || {};

      if (metaOnly) {
        return {
          timezones,
          preAggregation,
          partitions: <PreAggregationDescription[]>[],
          errors: <string[]>[],
          partitionsWithDependencies: <PartitionsWithDependencies[]>[]
        };
      }

      const type = preAggregation?.preAggregation?.type;
      const isEphemeralPreAggregation = type === 'rollupJoin' || type === 'rollupLambda';

      const queriesForPreAggregation: RefreshQueries[] = !isEphemeralPreAggregation && (await Promise.all(
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
        .map((query): PartitionsWithDependencies => {
          let dependencies: PreAggregationDescription[] = [];

          for (let i = 0; i < query.groupedPartitions.length - 1; i++) {
            dependencies = dependencies.concat(query.groupedPartitions[i]);
          }
          return {
            dependencies,
            partitions: query.groupedPartitions.length && query.groupedPartitions[query.groupedPartitions.length - 1]
              .filter(p => !partitionsFilter?.length || partitionsFilter.includes(p?.tableName)) || []
          };
        });

      const partitions = partitionsWithDependencies.map(p => p.partitions).reduce((a, b) => a.concat(b), []);

      const firstPartition = partitionsWithDependencies[0]?.partitions[0];
      const { invalidateKeyQueries, preAggregationStartEndQueries } = firstPartition || {};

      const errors = [...new Set(queriesForPreAggregation.map(q => q?.error).filter(e => e))];

      return {
        timezones,
        invalidateKeyQueries,
        preAggregationStartEndQueries,
        preAggregation: this.withRefreshSql(preAggregation, firstPartition),
        partitions,
        errors,
        partitionsWithDependencies
      };
    }).map(loadConcurrency));
  }

  /**
   * The pre-aggregations of `preAggregationPartitions`, without their partitions. The refresh SQL is
   * read from the compiled description instead of its first partition: partitions differ only in
   * the table name and params, so the SQL is the same unless it names the pre-aggregation's own table.
   * Expanding every partition of every pre-aggregation is what made listing large data models time out.
   */
  public async preAggregationsWithRefreshSql(
    context: RequestContext,
    queryingOptions: Pick<PreAggregationsQueryingOptions, 'timezones' | 'preAggregationLoadConcurrency'> & {
      preAggregations: Omit<PreAggregationsQueryingOptions['preAggregations'][number], 'partitions'>[],
    }
  ): Promise<PreAggregationInfo[]> {
    const compilerApi = await this.serverCore.getCompilerApi(context);
    const queryingOptionsById = Object.fromEntries(queryingOptions.preAggregations.map(p => [p.id, p]));
    const preAggregations = await compilerApi.preAggregations({
      preAggregationIds: Object.keys(queryingOptionsById)
    });
    const loadConcurrency = pLimit(queryingOptions.preAggregationLoadConcurrency || 1);

    return Promise.all(preAggregations.map(preAggregation => async () => {
      const { cacheOnly, metaOnly } = queryingOptionsById[preAggregation.id] || {};
      if (metaOnly) {
        return preAggregation;
      }

      const type = preAggregation.preAggregation?.type;
      if (!REFRESH_SQL_FIELDS.some(field => preAggregation.preAggregation?.[field]?.sql) ||
        type === 'rollupJoin' || type === 'rollupLambda') {
        return this.withRefreshSql(preAggregation, undefined);
      }

      // Like `preAggregationPartitions`, which reads the SQL from the first time zone's queries
      const { queryBody } = await this.refreshQueryBodyForPreAggregation(context, compilerApi, preAggregation, {
        timezones: undefined,
        concurrency: undefined,
        workerIndices: undefined,
        timezone: queryingOptions.timezones[0],
        cacheOnly,
        contextSymbols: {
          securityContext: context.securityContext || {},
        }
      });
      if (!queryBody) {
        return this.withRefreshSql(preAggregation, undefined);
      }

      // `preAggregationPartitions` reads the partitions of the last description
      const description = queryBody.preAggregations[queryBody.preAggregations.length - 1];
      const [[refreshKeySql] = []] = description.invalidateKeyQueries || [];
      const partitioned = description.partitionGranularity && !description.expandedPartition;
      if (!partitioned || !refreshKeySql?.includes(description.tableName)) {
        return this.withRefreshSql(preAggregation, description);
      }

      // Each partition replaces the table name with its own, so the SQL comes from the first one
      const orchestratorApi = await this.serverCore.getOrchestratorApi(context);
      const { groupedPartitionPreAggregations } = await orchestratorApi.expandPartitionsInPreAggregations(queryBody);
      return this.withRefreshSql(preAggregation, groupedPartitionPreAggregations[groupedPartitionPreAggregations.length - 1]?.[0]);
    }).map(loadConcurrency));
  }

  /**
   * Replaces the refresh key and build range `sql` functions of the pre-aggregation with the SQL of
   * the description, or with `undefined` when there is none.
   */
  protected withRefreshSql(
    preAggregation: PreAggregationInfo,
    description: Pick<PreAggregationDescription, 'invalidateKeyQueries' | 'preAggregationStartEndQueries'> | undefined
  ): PreAggregationInfo {
    const { invalidateKeyQueries, preAggregationStartEndQueries } = description || {};
    const [[refreshRangeStart], [refreshRangeEnd]] = preAggregationStartEndQueries || [[], []];
    const [[refreshKey]] = invalidateKeyQueries?.length ? invalidateKeyQueries : [[]];

    const refreshesSqlMap = {
      refreshKey,
      refreshRangeStart,
      refreshRangeEnd
    };
    const preAggRefreshesWithSql: Record<string, any> = {};
    REFRESH_SQL_FIELDS.forEach((field) => {
      if (preAggregation?.preAggregation[field]?.sql) {
        preAggRefreshesWithSql[field] = {
          ...preAggregation.preAggregation[field],
          sql: refreshesSqlMap[field]
        };
      }
    });

    return {
      ...preAggregation,
      preAggregation: {
        ...preAggregation?.preAggregation,
        ...preAggRefreshesWithSql
      }
    };
  }

  /**
   * Partitions of the last pre-aggregation in the pre-aggregation's queries, each preceded
   * by the partitions of the pre-aggregations it depends on. Shared by the refresh and touch flows
   * through `queriesCache`, so a run expands the partitions of a pre-aggregation only once.
   */
  protected async partitionQueries(
    context: RequestContext,
    compilerApi: CompilerApi,
    preAggregation: PreAggregationInfo,
    queryingOptions: ScheduledRefreshQueryingOptions & { timezone: string },
    queriesCache: ScheduledQueriesCache
  ): Promise<PreAggregationDescription[][]> {
    const key = `${preAggregation.id}_${queryingOptions.timezone}`;
    if (!(await queriesCache[key])) {
      queriesCache[key] = this.refreshQueriesForPreAggregation(
        context, compilerApi, preAggregation, queryingOptions
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
  }

  protected async roundRobinRefreshPreAggregationsQueryIterator(context: RequestContext, compilerApi: CompilerApi, queryingOptions: ScheduledRefreshQueryingOptions, queriesCache: ScheduledQueriesCache) {
    const { timezones, preAggregationsWarmup } = queryingOptions;
    const scheduledPreAggregations = await compilerApi.scheduledPreAggregations();

    let preAggregationCursor = 0;
    let timezoneCursor = 0;
    let partitionCursor = 0;
    let partitionCounter = 0;

    const finishedPartitions: Record<string, boolean> = {};
    scheduledPreAggregations.forEach((p, pi) => {
      timezones.forEach((t, ti) => {
        finishedPartitions[`${pi}_${ti}`] = false;
      });
    });
    const queriesForPreAggregation = async (preAggregationIndex: number, timezone: string) => this.partitionQueries(
      context, compilerApi, scheduledPreAggregations[preAggregationIndex], { ...queryingOptions, timezone }, queriesCache
    );

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
              priority: preAggregationsWarmup
                ? QueuePriority.Warmup
                : QueuePriority.Scheduled - (queries.length - 1 - queryCursor)
            })),
            cacheMode: 'must-revalidate',
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
    queryingOptions: ScheduledRefreshQueryingOptions,
    { preAggregationsLoadCacheByDataSource, queriesCache }: ScheduledRefreshCaches = newScheduledRefreshCaches()
  ) {
    const { securityContext } = context;
    const { queryIteratorState, concurrency, workerIndices } = queryingOptions;

    await Promise.all(R.range(0, concurrency)
      .filter(workerIndex => workerIndices.indexOf(workerIndex) !== -1)
      .map(async workerIndex => {
        const queryIteratorStateKey = JSON.stringify({ ...securityContext, workerIndex });
        const queryIterator = queryIteratorState?.[queryIteratorStateKey] ||
          (await this.roundRobinRefreshPreAggregationsQueryIterator(
            context, compilerApi, queryingOptions, queriesCache
          ));
        if (queryIteratorState) {
          queryIteratorState[queryIteratorStateKey] = queryIterator;
        }

        for (;;) {
          const currentQuery = await queryIterator.current();
          if (currentQuery && queryIterator.partitionCounter() % concurrency === workerIndex) {
            const orchestratorApi = await this.serverCore.getOrchestratorApi(context);
            const preAggsInstance = orchestratorApi.getQueryOrchestrator().getPreAggregations();
            const now = new Date();

            const backoffChecks = await Promise.all(
              currentQuery.preAggregations.map((p: PreAggregationDescription) => preAggsInstance.getPreAggBackoff(p.tableName))
            );

            // Skip execution if any pre-aggregation is still in backoff window
            const shouldSkip = backoffChecks.some(backoffData => backoffData && now < backoffData.nextTimestamp);

            if (!shouldSkip) {
              try {
                await orchestratorApi.executeQuery({ ...currentQuery, preAggregationsLoadCacheByDataSource });
              } catch (e: any) {
                // Check if this is a "Continue wait" error - these are normal queue signals
                // For Continue wait errors, re-throw to handle them in the normal flow
                if (e.error === 'Continue wait') {
                  throw e;
                }

                // Real datasource error - apply exponential backoff
                for (const p of currentQuery.preAggregations) {
                  let backoffData = await preAggsInstance.getPreAggBackoff(p.tableName);

                  if (backoffData && backoffData.backoffMultiplier > 0) {
                    const newMultiplier = backoffData.backoffMultiplier * 2;
                    const delaySeconds = Math.min(newMultiplier, preAggsInstance.getPreAggBackoffMaxTime());

                    backoffData = {
                      backoffMultiplier: newMultiplier,
                      nextTimestamp: new Date(now.valueOf() + delaySeconds * 1000),
                    };
                  } else {
                    backoffData = {
                      backoffMultiplier: 1,
                      nextTimestamp: new Date(now.valueOf() + 1000),
                    };
                  }

                  await preAggsInstance.updatePreAggBackoff(p.tableName, backoffData);
                }
              }
            }
          }
          const hasNext = await queryIterator.advance();
          if (!hasNext) {
            return;
          }
        }
      }));

    const orchestratorApi = await this.serverCore.getOrchestratorApi(context);
    await orchestratorApi.updateRefreshEndReached();
  }

  public async buildPreAggregations(
    context: RequestContext,
    queryingOptions: PreAggregationsQueryingOptions
  ) {
    const orchestratorApi = await this.serverCore.getOrchestratorApi(context);
    const preAggregations = await this.preAggregationPartitions(context, queryingOptions);
    const preAggregationsLoadCacheByDataSource = {};

    const promise = Promise.all(preAggregations.map(async (p) => {
      const { partitionsWithDependencies } = p;
      return Promise.all(partitionsWithDependencies.map(({ partitions, dependencies }) => (
        Promise.all(partitions.map(async (partition) => {
          await orchestratorApi.executeQuery({
            preAggregations: dependencies.concat([partition]),
            cacheMode: 'must-revalidate',
            forceBuildPreAggregations: queryingOptions.forceBuildPreAggregations ?? true,
            orphanedTimeout: 60 * 60,
            requestId: context.requestId,
            timezone: partition.timezone,
            scheduledRefresh: false,
            preAggregationsLoadCacheByDataSource,
            metadata: queryingOptions.metadata,
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

  /**
   * Post pre-aggregations build jobs and returns jobs identifier objects.
   */
  public async postBuildJobs(
    context: RequestContext,
    queryingOptions: PreAggregationsQueryingOptions
  ): Promise<string[]> {
    const orchestratorApi = await this.serverCore.getOrchestratorApi(context);
    const preAggregations = await this.preAggregationPartitions(context, queryingOptions);
    if (queryingOptions.dateRange) {
      preAggregations.forEach(preAggregation => {
        preAggregation.partitions = preAggregation.partitions
          .filter(p => {
            if (!p.buildRangeStart && !p.buildRangeEnd) {
              return true; // If there is no range specified - we should include it like rebuild in anyway
            }

            return PreAggregationPartitionRangeLoader.intersectDateRanges(
              [p.buildRangeStart, p.buildRangeEnd],
              queryingOptions.dateRange,
            );
          });
        preAggregation.partitionsWithDependencies.forEach(pd => {
          pd.partitions = pd.partitions.filter(p => {
            if (!p.buildRangeStart && !p.buildRangeEnd) {
              return true; // If there is no range specified - we should include it like rebuild in any way
            }

            return PreAggregationPartitionRangeLoader.intersectDateRanges(
              [p.buildRangeStart, p.buildRangeEnd],
              queryingOptions.dateRange,
            );
          });

          pd.dependencies = pd.dependencies.filter(p => {
            if (!p.buildRangeStart && !p.buildRangeEnd) {
              return true; // If there is no range specified - we should include it like rebuild in any way
            }

            return PreAggregationPartitionRangeLoader.intersectDateRanges(
              [p.buildRangeStart, p.buildRangeEnd],
              queryingOptions.dateRange,
            );
          });
        });
      });
    }

    const preAggregationsLoadCacheByDataSource = {};
    const jobsPromise = Promise.all(
      preAggregations
        // Filter out pre-aggs without partitions
        .filter(p => p.partitions.length)
        .map(async (p) => {
          const { partitionsWithDependencies } = p;
          return Promise.all(
            partitionsWithDependencies.map(({ partitions, dependencies }) => (
              Promise.all(
                partitions.map(
                  async (partition): Promise<JobedPreAggregation[]> => {
                    const job = await orchestratorApi.executeQuery({
                      preAggregations: dependencies.concat([partition]),
                      cacheMode: 'stale-if-slow',
                      forceBuildPreAggregations: true,
                      orphanedTimeout: 60 * 60,
                      requestId: context.requestId,
                      timezone: partition.timezone,
                      scheduledRefresh: false,
                      preAggregationsLoadCacheByDataSource,
                      metadata: queryingOptions.metadata,
                      isJob: true,
                    });
                    job.forEach((j: JobedPreAggregation) => {
                      j.dataSource = j.dataSource || partition.dataSource;
                      j.timezone = j.timezone || partition.timezone;
                    });

                    return job;
                  }
                )
              )
            ))
          );
        })
    );

    const jobedPAs = await jobsPromise;

    return getPreAggsJobsList(
      context,
      <JobedPreAggregation[][][][]>jobedPAs,
    ).map((job: PreAggJob) => {
      const key = getPreAggJobToken(job);
      orchestratorApi
        .getQueryOrchestrator()
        .getQueryCache()
        .getCacheDriver()
        .set(`PRE_AGG_JOB_${key}`, job, 86400);
      return key;
    });
  }

  /**
   * Returns pre-aggregations build jobs from the cache.
   */
  public async getCachedBuildJobs(
    context: RequestContext,
    tokens: string[],
  ): Promise<{ job: PreAggJob | null, token: string }[]> {
    const orchestratorApi = await this.serverCore.getOrchestratorApi(context);
    const jobsPromise = Promise.all(
      tokens.map(async (token) => {
        const job = await orchestratorApi
          .getQueryOrchestrator()
          .getQueryCache()
          .getCacheDriver()
          .get<PreAggJob>(`PRE_AGG_JOB_${token}`);

        return {
          job,
          token
        };
      })
    );

    const jobs = await jobsPromise;
    return jobs;
  }
}
