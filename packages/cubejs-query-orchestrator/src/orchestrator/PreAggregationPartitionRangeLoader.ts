import {
  addSecondsToLocalTimestamp,
  BUILD_RANGE_END_LOCAL,
  BUILD_RANGE_START_LOCAL,
  FROM_PARTITION_RANGE,
  TO_PARTITION_RANGE,
  MAX_SOURCE_ROW_LIMIT,
  reformatInIsoLocal,
  utcToLocalTimeZone,
  timeSeries,
  localTimestampToUtc,
  parseUtcIntoLocalDate,
  LoggerFn,
} from '@cubejs-backend/shared';
import { InlineTable, QueuePriority, TableStructure } from '@cubejs-backend/base-driver';
import { DriverFactory } from './DriverFactory';
import { QueryCache, QueryWithParams } from './QueryCache';
import {
  getLastUpdatedAtTimestamp,
  LAMBDA_TABLE_PREFIX,
  LambdaQuery,
  LoadPreAggregationResult,
  PartitionRanges,
  PreAggregationDescription,
  PreAggregations,
  PreAggregationTableToTempTable,
  QueryDateRange
} from './PreAggregations';
import { PreAggregationLoader } from './PreAggregationLoader';
import { PreAggregationLoadCache } from './PreAggregationLoadCache';

const DEFAULT_TS_FORMAT = 'YYYY-MM-DDTHH:mm:ss.SSS';

interface ResolvedQueryDateRange {
  local: QueryDateRange;
  utc: QueryDateRange;
}

interface PreAggsPartitionRangeLoaderOpts {
  maxPartitions: number;
  maxSourceRowLimit: number;
  waitForRenew?: boolean;
  requestId?: string;
  externalRefresh?: boolean;
  forceBuild?: boolean;
  metadata?: any;
  orphanedTimeout?: number;
  lambdaQuery?: LambdaQuery;
  isJob?: boolean;
  compilerCacheFn?: <T>(subKey: string[], cacheFn: () => T) => T;
}

export class PreAggregationPartitionRangeLoader {
  /**
   * Determines whether current instance instantiated for a jobbed build query
   * (initialized by the /cubejs-system/v1/pre-aggregations/jobs endpoint) or
   * not.
   */
  protected isJob: boolean;

  protected waitForRenew: boolean;

  protected requestId: string;

  protected lambdaQuery: LambdaQuery;

  protected dataSource: string;

  protected compilerCacheFn: <T>(subKey: string[], cacheFn: () => T) => T;

  public constructor(
    private readonly driverFactory: DriverFactory,
    private readonly logger: LoggerFn,
    private readonly queryCache: QueryCache,
    private readonly preAggregations: PreAggregations,
    private readonly preAggregation: PreAggregationDescription,
    private readonly preAggregationsTablesToTempTables: PreAggregationTableToTempTable[],
    private readonly loadCache: PreAggregationLoadCache,
    private readonly options: PreAggsPartitionRangeLoaderOpts = {
      maxPartitions: 10000,
      maxSourceRowLimit: 10000,
    },
  ) {
    this.isJob = !!options.isJob;
    this.waitForRenew = options.waitForRenew;
    this.requestId = options.requestId;
    this.lambdaQuery = options.lambdaQuery;
    this.dataSource = preAggregation.dataSource;
    this.compilerCacheFn = options.compilerCacheFn || ((subKey, cacheFn) => cacheFn());
  }

  private async loadRangeQuery(rangeQuery: QueryWithParams, partitionRange?: QueryDateRange) {
    const [query, values, queryOptions]: QueryWithParams = rangeQuery;
    const invalidate = QueryCache.buildRangeInvalidateKey(this.preAggregation);

    return this.queryCache.cacheQueryResult(
      query,
      <string[]>values,
      QueryCache.queryCacheKey({
        query,
        values: (<string[]>values),
        invalidate,
      }),
      24 * 60 * 60,
      {
        renewalThreshold: this.queryCache.options.refreshKeyRenewalThreshold
          || queryOptions?.renewalThreshold || 24 * 60 * 60,
        waitForRenew: this.waitForRenew,
        priority: this.priority(QueuePriority.Interactive),
        requestId: this.requestId,
        dataSource: this.dataSource,
        useInMemory: true,
        external: queryOptions?.external,
        renewalKey: partitionRange ? await this.getInvalidationKeyValues(partitionRange) : null,
      }
    );
  }

  protected getInvalidationKeyValues(range) {
    const { invalidateKeyQueries } = this.preAggregation;
    if (!invalidateKeyQueries?.length) {
      return Promise.resolve([]);
    }
    const partitionTableName = PreAggregationPartitionRangeLoader.partitionTableName(
      this.preAggregation.tableName, this.preAggregation.partitionGranularity, range
    );
    const partitionRange = this.resolvePartitionRange(range);
    return Promise.all(
      invalidateKeyQueries.map(
        (sqlQuery) => (
          this.loadCache.keyQueryResult(
            this.replacePartitionSqlAndParams(sqlQuery, partitionRange, partitionTableName), this.waitForRenew, this.priority(QueuePriority.Interactive)
          )
        )
      )
    );
  }

  protected priority(defaultValue) {
    return this.preAggregation.priority ?? defaultValue;
  }

  public async replaceQueryBuildRangeParams(queryValues: string[]): Promise<string[] | null> {
    if (queryValues.find(p => p === BUILD_RANGE_START_LOCAL || p === BUILD_RANGE_END_LOCAL)) {
      const [buildRangeStart, buildRangeEnd] = await this.loadBuildRange(this.preAggregation.timestampFormat);
      return queryValues?.map(
        param => {
          if (param === BUILD_RANGE_START_LOCAL) {
            return buildRangeStart;
          } else if (param === BUILD_RANGE_END_LOCAL) {
            return buildRangeEnd;
          } else {
            return param;
          }
        },
      );
    }
    return null;
  }

  // Converted once per partition so every query shares the UTC boundaries instead of re-deriving them per param.
  private resolvePartitionRange(local: QueryDateRange): ResolvedQueryDateRange {
    return {
      local,
      utc: [
        PreAggregationPartitionRangeLoader.inDbTimeZone(this.preAggregation, local[0]),
        PreAggregationPartitionRangeLoader.inDbTimeZone(this.preAggregation, local[1]),
      ],
    };
  }

  private replacePartitionSqlAndParams(
    query: QueryWithParams,
    { local: dateRange, utc: utcDateRange }: ResolvedQueryDateRange,
    partitionTableName: string
  ): QueryWithParams {
    const [sql, params, options] = query;
    const updateWindowToBoundary = options?.incremental && addSecondsToLocalTimestamp(
      dateRange[1], this.preAggregation.timezone, options?.updateWindowSeconds || 0
    );
    return [sql.replace(this.preAggregation.tableName, partitionTableName), params?.map(
      param => {
        if (param === FROM_PARTITION_RANGE) {
          return utcDateRange[0];
        } else if (param === TO_PARTITION_RANGE) {
          return utcDateRange[1];
        } else {
          return param;
        }
      },
    ), {
      ...options,
      renewalThreshold:
        options?.incremental && updateWindowToBoundary < new Date() ?
          // if updateWindowToBoundary passed just moments ago we want to renew it earlier in case
          // of server and db clock don't match
          Math.min(
            Math.round((new Date().getTime() - updateWindowToBoundary.getTime()) / 1000),
            options?.renewalThresholdOutsideUpdateWindow
          ) :
          options?.renewalThreshold
    }];
  }

  private partitionPreAggregationDescription(range: QueryDateRange, buildRange: QueryDateRange): PreAggregationDescription {
    const partitionTableName = PreAggregationPartitionRangeLoader.partitionTableName(
      this.preAggregation.tableName, this.preAggregation.partitionGranularity, range
    );
    const [_, buildRangeEnd] = buildRange;
    const loadRange: [string, string] = [...range];
    const partitionInvalidateKeyQueries = this.preAggregation.partitionInvalidateKeyQueries || this.preAggregation.invalidateKeyQueries;
    // `partitionInvalidateKeyQueries = []` in case of real time
    if ((!partitionInvalidateKeyQueries || partitionInvalidateKeyQueries.length > 0) && buildRangeEnd < range[1]) {
      loadRange[1] = buildRangeEnd;
    }
    const clipped = loadRange[1] !== range[1];
    const partitionRange = this.resolvePartitionRange(range);
    const partitionLoadRange = clipped ? this.resolvePartitionRange(loadRange) : partitionRange;
    const sealAt = addSecondsToLocalTimestamp(
      loadRange[1], this.preAggregation.timezone, this.preAggregation.updateWindowSeconds || 0
    ).toISOString();
    const structureVersionLoadSql = this.preAggregation.loadSql &&
      this.replacePartitionSqlAndParams(this.preAggregation.loadSql, partitionRange, partitionTableName);
    // Reuse the SQL tuple for unclipped partitions to reduce computation and memory allocations.
    const loadSql = clipped
      ? this.preAggregation.loadSql && this.replacePartitionSqlAndParams(this.preAggregation.loadSql, partitionLoadRange, partitionTableName)
      : structureVersionLoadSql;
    return {
      ...this.preAggregation,
      tableName: partitionTableName,
      structureVersionLoadSql,
      loadSql,
      sql: this.preAggregation.sql &&
        this.replacePartitionSqlAndParams(this.preAggregation.sql, partitionLoadRange, partitionTableName),
      invalidateKeyQueries: (this.preAggregation.invalidateKeyQueries || [])
        .map(q => this.replacePartitionSqlAndParams(q, partitionRange, partitionTableName)),
      partitionInvalidateKeyQueries: this.preAggregation.partitionInvalidateKeyQueries?.map(q => this.replacePartitionSqlAndParams(q, partitionRange, partitionTableName)),
      indexesSql: (this.preAggregation.indexesSql || [])
        .map(q => ({ ...q, sql: this.replacePartitionSqlAndParams(q.sql, partitionRange, partitionTableName) })),
      previewSql: this.preAggregation.previewSql &&
        this.replacePartitionSqlAndParams(this.preAggregation.previewSql, partitionRange, partitionTableName),
      buildRangeStart: loadRange[0],
      buildRangeEnd: loadRange[1],
      sealAt, // Used only for kSql pre aggregations
    };
  }

  public async loadPreAggregations(): Promise<LoadPreAggregationResult> {
    if (this.preAggregation.partitionGranularity && !this.preAggregation.expandedPartition) {
      const loadPreAggregationsByPartitionRanges = async ({ buildRange, partitionRanges }: PartitionRanges) => {
        const partitionLoaders = partitionRanges.map(range => new PreAggregationLoader(
          this.driverFactory,
          this.logger,
          this.queryCache,
          this.preAggregations,
          this.partitionPreAggregationDescription(range, buildRange),
          this.preAggregationsTablesToTempTables,
          this.loadCache,
          this.options,
        ));
        const resolveResults = await Promise.all(partitionLoaders.map(async (l, i) => {
          const result = await l.loadPreAggregation(false);
          return result && {
            ...result,
            partitionRange: partitionRanges[i]
          };
        }));
        return { loadResults: resolveResults.filter(res => res !== null), partitionLoaders };
      };

      // eslint-disable-next-line prefer-const
      let loadResultAndLoaders = await loadPreAggregationsByPartitionRanges(await this.partitionRanges());
      if (this.options.externalRefresh && loadResultAndLoaders.loadResults.length === 0) {
        loadResultAndLoaders = await loadPreAggregationsByPartitionRanges(await this.partitionRanges(true));
        // In case there are no partitions ready at matched time dimension intersection then no data can be retrieved.
        // We need to provide any table so query can just execute successfully.
        if (loadResultAndLoaders.loadResults.length > 0) {
          loadResultAndLoaders.loadResults = [loadResultAndLoaders.loadResults[loadResultAndLoaders.loadResults.length - 1]];
        }
      }
      if (this.options.externalRefresh && loadResultAndLoaders.loadResults.length === 0) {
        throw new Error(
          // eslint-disable-next-line no-use-before-define
          PreAggregations.noPreAggregationPartitionsBuiltMessage(loadResultAndLoaders.partitionLoaders.map(p => p.preAggregation))
        );
      }

      let { loadResults } = loadResultAndLoaders;

      let lambdaTable: InlineTable;
      let emptyResult = false;

      if (this.preAggregation.rollupLambdaId) {
        if (this.lambdaQuery && loadResults.length > 0) {
          const { buildRangeEnd, targetTableName } = loadResults[loadResults.length - 1];
          const lambdaTypes = await this.loadCache.getTableColumnTypes(this.preAggregation, targetTableName);
          lambdaTable = await this.downloadLambdaTable(buildRangeEnd, lambdaTypes);
        }
        const rollupLambdaResults = this.preAggregationsTablesToTempTables.filter(tempTableResult => tempTableResult[1].rollupLambdaId === this.preAggregation.rollupLambdaId);
        const filteredResults = loadResults.filter(
          r => (this.preAggregation.lastRollupLambda || reformatInIsoLocal(r.buildRangeEnd) === reformatInIsoLocal(r.partitionRange[1])) &&
            rollupLambdaResults.every(result => !result[1].buildRangeEnd || reformatInIsoLocal(result[1].buildRangeEnd) < reformatInIsoLocal(r.partitionRange[0]))
        );
        if (filteredResults.length === 0) {
          emptyResult = true;
          loadResults = [loadResults[loadResults.length - 1]];
        } else {
          loadResults = filteredResults;
        }
      }

      const allTableTargetNames = loadResults.map(targetTableName => targetTableName.targetTableName);
      let lastUpdatedAt = getLastUpdatedAtTimestamp(loadResults.map(r => r.lastUpdatedAt));

      if (lambdaTable) {
        allTableTargetNames.push(lambdaTable.name);
        lastUpdatedAt = Date.now();
      }

      const unionTargetTableName = allTableTargetNames
        .map(targetTableName => `SELECT * FROM ${targetTableName}${emptyResult ? ' WHERE 1 = 0' : ''}`)
        .join(' UNION ALL ');

      const baseTargetTableName = allTableTargetNames.length === 1 && !emptyResult ? allTableTargetNames[0] : `(${unionTargetTableName})`;

      // Build per-usage target table names if usageMapping is present
      let usageTargetTableNames: Record<string, string> | undefined;
      if (this.preAggregation.usageMapping) {
        usageTargetTableNames = {};

        for (const [suffix, usageInfo] of Object.entries(this.preAggregation.usageMapping)) {
          if (usageInfo.dateRange && this.preAggregation.partitionGranularity) {
            // Load partition ranges specific to this usage's dateRange.
            // Use partitionRange (generated locally via timeSeries, always in DEFAULT_TS_FORMAT)
            // instead of buildRangeEnd (from DB, may include Z suffix depending on driver timestampFormat).
            const usageDateRange = PreAggregationPartitionRangeLoader.intersectDateRanges(
              [loadResults[0]?.partitionRange?.[0] || null, loadResults[loadResults.length - 1]?.partitionRange?.[1] || null] as QueryDateRange,
              usageInfo.dateRange as QueryDateRange,
            );
            if (usageDateRange) {
              const usagePartitions = loadResults.filter(r => {
                if (!r.partitionRange) return true;
                const [pStart, pEnd] = r.partitionRange;
                const [uStart, uEnd] = usageDateRange;
                return pEnd >= uStart && pStart <= uEnd;
              });
              const usageTableNames = usagePartitions.map(r => r.targetTableName);
              if (usageTableNames.length === 1) {
                [usageTargetTableNames[suffix]] = usageTableNames;
              } else if (usageTableNames.length > 0) {
                const usageUnion = usageTableNames
                  .map(t => `SELECT * FROM ${t}`)
                  .join(' UNION ALL ');
                usageTargetTableNames[suffix] = `(${usageUnion})`;
              } else {
                usageTargetTableNames[suffix] = baseTargetTableName;
              }
            } else {
              usageTargetTableNames[suffix] = baseTargetTableName;
            }
          } else {
            usageTargetTableNames[suffix] = baseTargetTableName;
          }
        }
      }

      return {
        targetTableName: baseTargetTableName,
        refreshKeyValues: loadResults.map(t => t.refreshKeyValues),
        lastUpdatedAt,
        buildRangeEnd: !emptyResult && loadResults.length && loadResults[loadResults.length - 1].buildRangeEnd,
        lambdaTable,
        rollupLambdaId: this.preAggregation.rollupLambdaId,
        isMultiTableUnion: allTableTargetNames.length > 1,
        usageTargetTableNames,
      };
    } else {
      const result = await new PreAggregationLoader(
        this.driverFactory,
        this.logger,
        this.queryCache,
        this.preAggregations,
        this.preAggregation,
        this.preAggregationsTablesToTempTables,
        this.loadCache,
        this.options
      ).loadPreAggregation(true);
      if (result && this.preAggregation.usageMapping) {
        const usageTargetTableNames: Record<string, string> = {};

        for (const suffix of Object.keys(this.preAggregation.usageMapping)) {
          usageTargetTableNames[suffix] = result.targetTableName;
        }
        return { ...result, usageTargetTableNames };
      }
      return result;
    }
  }

  /**
   * Downloads the lambda table from the source DB.
   */
  private async downloadLambdaTable(fromDate: string, lambdaTypes: TableStructure): Promise<InlineTable> {
    const { sqlAndParams, cacheKeyQueries } = this.lambdaQuery;
    const [query, params] = sqlAndParams;
    const values = params.map((p) => {
      if (p === FROM_PARTITION_RANGE) {
        return fromDate;
      }
      if (p === MAX_SOURCE_ROW_LIMIT) {
        return this.options.maxSourceRowLimit;
      }
      return p;
    });
    const { data } = await this.queryCache.renewQuery(
      query,
      <string[]>values,
      cacheKeyQueries,
      60 * 60,
      [query, <string[]>values],
      undefined,
      {
        requestId: this.requestId,
        skipRefreshKeyWaitForRenew: false,
        priority: this.priority(QueuePriority.Interactive),
        dataSource: this.dataSource,
        external: false,
        useCsvQuery: true,
        lambdaTypes,
      }
    );
    if (data.rowCount === this.options.maxSourceRowLimit) {
      throw new Error(`The maximum number of source rows ${this.options.maxSourceRowLimit} was reached for ${this.preAggregation.preAggregationId}`);
    }
    return {
      name: `${LAMBDA_TABLE_PREFIX}_${this.preAggregation.tableName.replace('.', '_')}`,
      columns: data.types,
      csvRows: data.csvRows,
    };
  }

  public async partitionPreAggregations(): Promise<PreAggregationDescription[]> {
    if (this.preAggregation.partitionGranularity && !this.preAggregation.expandedPartition) {
      const { buildRange, partitionRanges } = await this.partitionRanges();
      return this.compilerCacheFn(['partitions', JSON.stringify(buildRange)], () => partitionRanges.map(range => this.partitionPreAggregationDescription(range, buildRange)));
    } else {
      return [this.preAggregation];
    }
  }

  private async partitionRanges(ignoreMatchedDateRange?: boolean): Promise<PartitionRanges> {
    const buildRange = await this.loadBuildRange();

    // buildRange was localized in loadBuildRange()
    // preAggregation.matchedTimeDimensionDateRange is also localized
    // in BaseFilter->formatToDate()/formatFromDate()
    let dateRange = PreAggregationPartitionRangeLoader.intersectDateRanges(
      buildRange,
      ignoreMatchedDateRange ? undefined : this.preAggregation.matchedTimeDimensionDateRange,
    );

    if (!dateRange) {
      // If there's no date range intersection between query data range and pre-aggregation build range
      // use last partition so outer query can receive expected table structure.
      dateRange = [buildRange[1], buildRange[1]];
    }

    const partitionRanges = this.compilerCacheFn(
      ['timeSeries', this.preAggregation.partitionGranularity, JSON.stringify(dateRange), `${this.preAggregation.timestampPrecision}`],
      () => PreAggregationPartitionRangeLoader.timeSeries(
        this.preAggregation.partitionGranularity,
        dateRange,
        this.preAggregation.timestampPrecision
      )
    );

    if (partitionRanges.length > this.options.maxPartitions) {
      throw new Error(
        `Pre-aggregation '${this.preAggregation.tableName}' requested to build ${partitionRanges.length} partitions which exceeds the maximum number of partitions per pre-aggregation of ${this.options.maxPartitions}`
      );
    }

    return { buildRange: dateRange, partitionRanges };
  }

  public async loadBuildRange(timestampFormat: string = DEFAULT_TS_FORMAT): Promise<QueryDateRange> {
    const { preAggregationStartEndQueries } = this.preAggregation;
    const [startDate, endDate] = await Promise.all(
      preAggregationStartEndQueries.map(
        async rangeQuery => PreAggregationPartitionRangeLoader.extractDate(await this.loadRangeQuery(rangeQuery), this.preAggregation.timezone, timestampFormat),
      ),
    );

    if (!this.preAggregation.partitionGranularity) {
      return this.orNowIfEmpty([startDate, endDate]);
    }

    // startDate & endDate are `localized` here
    const wholeSeriesRanges = PreAggregationPartitionRangeLoader.timeSeries(
      this.preAggregation.partitionGranularity,
      this.orNowIfEmpty([startDate, endDate]),
      this.preAggregation.timestampPrecision,
    );
    const [rangeStart, rangeEnd] = await Promise.all(
      preAggregationStartEndQueries.map(
        async (rangeQuery, i) => PreAggregationPartitionRangeLoader.extractDate(
          await this.loadRangeQuery(
            rangeQuery, i === 0 ? wholeSeriesRanges[0] : wholeSeriesRanges[wholeSeriesRanges.length - 1],
          ),
          this.preAggregation.timezone,
          timestampFormat,
        ),
      ),
    );
    return this.orNowIfEmpty([rangeStart, rangeEnd]);
  }

  private now() {
    return utcToLocalTimeZone(this.preAggregation.timezone, DEFAULT_TS_FORMAT, new Date().toJSON().substring(0, 23));
  }

  private orNowIfEmpty(dateRange: QueryDateRange): QueryDateRange {
    if (!dateRange[0] && !dateRange[1]) {
      const now = this.now();
      return [now, now];
    }
    if (!dateRange[0]) {
      return [dateRange[1], dateRange[1]];
    }
    if (!dateRange[1]) {
      return [dateRange[0], dateRange[0]];
    }
    return dateRange;
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

    if ((range[0].length !== 23 && range[0].length !== 26) || (range[1].length !== 23 && range[0].length !== 26)) {
      throw new Error(`Date range expected to be in ${DEFAULT_TS_FORMAT} format but ${range} found`);
    }
  }

  public static intersectDateRanges(rangeA: QueryDateRange | null, rangeB: QueryDateRange | null): QueryDateRange | null {
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

  public static timeSeries(granularity: string, dateRange: QueryDateRange | null, timestampPrecision: number): QueryDateRange[] {
    if (!dateRange) {
      return [];
    }
    return timeSeries(granularity, dateRange, {
      timestampPrecision
    });
  }

  public static partitionTableName(tableName: string, partitionGranularity: string, dateRange: QueryDateRange) {
    let dateLenCut: number;
    switch (partitionGranularity) {
      case 'hour':
        dateLenCut = 13;
        break;
      case 'minute':
        dateLenCut = 16;
        break;
      default:
        dateLenCut = 10;
        break;
    }

    const partitionSuffix = dateRange[0].substring(
      0,
      dateLenCut
    ).replace(/[-T:]/g, '');

    return `${tableName}${partitionSuffix}`;
  }

  public static inDbTimeZone(preAggregationDescription: any, timestamp: string): string {
    return localTimestampToUtc(preAggregationDescription.timezone, preAggregationDescription.timestampFormat, timestamp);
  }

  public static extractDate(data: any, timezone: string, timestampFormat: string = DEFAULT_TS_FORMAT): string {
    return parseUtcIntoLocalDate(data, timezone, timestampFormat);
  }

  public static readonly FROM_PARTITION_RANGE = FROM_PARTITION_RANGE;

  public static readonly TO_PARTITION_RANGE = TO_PARTITION_RANGE;
}
