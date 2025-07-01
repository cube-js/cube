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
} from '@cubejs-backend/shared';
import { InlineTable, TableStructure } from '@cubejs-backend/base-driver';
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
    private readonly logger: any,
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
    const invalidate =
      this.preAggregation.invalidateKeyQueries?.[0]
        ? this.preAggregation.invalidateKeyQueries[0].slice(0, 2)
        : false;

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
        priority: this.priority(10),
        requestId: this.requestId,
        dataSource: this.dataSource,
        useInMemory: true,
        external: queryOptions?.external,
        renewalKey: partitionRange ? await this.getInvalidationKeyValues(partitionRange) : null,
      }
    );
  }

  protected getInvalidationKeyValues(range) {
    const partitionTableName = PreAggregationPartitionRangeLoader.partitionTableName(
      this.preAggregation.tableName, this.preAggregation.partitionGranularity, range
    );
    return Promise.all(
      (this.preAggregation.invalidateKeyQueries || []).map(
        (sqlQuery) => (
          this.loadCache.keyQueryResult(
            this.replacePartitionSqlAndParams(sqlQuery, range, partitionTableName), this.waitForRenew, this.priority(10)
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
          // Timestamp is in local timezone, so we need to convert to utc with desired format
          return localTimestampToUtc(this.preAggregation.timezone, this.preAggregation.timestampFormat, dateRange[0]);
        } else if (dateRange && param === TO_PARTITION_RANGE) {
          return localTimestampToUtc(this.preAggregation.timezone, this.preAggregation.timestampFormat, dateRange[1]);
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
    const sealAt = addSecondsToLocalTimestamp(
      loadRange[1], this.preAggregation.timezone, this.preAggregation.updateWindowSeconds || 0
    ).toISOString();
    return {
      ...this.preAggregation,
      tableName: partitionTableName,
      structureVersionLoadSql: this.preAggregation.loadSql &&
        this.replacePartitionSqlAndParams(this.preAggregation.loadSql, range, partitionTableName),
      loadSql: this.preAggregation.loadSql &&
        this.replacePartitionSqlAndParams(this.preAggregation.loadSql, loadRange, partitionTableName),
      sql: this.preAggregation.sql &&
        this.replacePartitionSqlAndParams(this.preAggregation.sql, loadRange, partitionTableName),
      invalidateKeyQueries: (this.preAggregation.invalidateKeyQueries || [])
        .map(q => this.replacePartitionSqlAndParams(q, range, partitionTableName)),
      partitionInvalidateKeyQueries: this.preAggregation.partitionInvalidateKeyQueries?.map(q => this.replacePartitionSqlAndParams(q, range, partitionTableName)),
      indexesSql: (this.preAggregation.indexesSql || [])
        .map(q => ({ ...q, sql: this.replacePartitionSqlAndParams(q.sql, range, partitionTableName) })),
      previewSql: this.preAggregation.previewSql &&
        this.replacePartitionSqlAndParams(this.preAggregation.previewSql, range, partitionTableName),
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
      return {
        targetTableName: allTableTargetNames.length === 1 && !emptyResult ? allTableTargetNames[0] : `(${unionTargetTableName})`,
        refreshKeyValues: loadResults.map(t => t.refreshKeyValues),
        lastUpdatedAt,
        buildRangeEnd: !emptyResult && loadResults.length && loadResults[loadResults.length - 1].buildRangeEnd,
        lambdaTable,
        rollupLambdaId: this.preAggregation.rollupLambdaId,
      };
    } else {
      return new PreAggregationLoader(
        this.driverFactory,
        this.logger,
        this.queryCache,
        this.preAggregations,
        this.preAggregation,
        this.preAggregationsTablesToTempTables,
        this.loadCache,
        this.options
      ).loadPreAggregation(true);
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
