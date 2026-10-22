import R from 'ramda';
import { PreAggregationPartitionRangeLoader } from '@cubejs-backend/query-orchestrator';
import { BaseQuery } from '../../../src';
import type { BaseQueryOptions, Compilers } from '../../../src';

export type QueryWithParams = [string, unknown[]];

// preAggregationInvalidateKeyQueries() entries carry refresh key options as a third element
export type InvalidateKeyQuery = [string, unknown[], Record<string, unknown>?];

/**
 * The subset of PreAggregations.preAggregationsDescription() items the integration tests read.
 * FullPreAggregationDescription is still an `any` alias in src.
 */
export type TestPreAggregationDescription = {
  preAggregationId: string;
  tableName: string;
  type: string;
  timezone: string;
  timestampPrecision: number;
  loadSql: QueryWithParams;
  invalidateKeyQueries: InvalidateKeyQuery[];
  indexesSql?: { indexName: string; sql: QueryWithParams }[];
  matchedTimeDimensionDateRange?: [string, string];
  usageMapping?: Record<string, unknown>;
} & ({
  partitionGranularity?: undefined;
  preAggregationStartEndQueries?: [QueryWithParams, QueryWithParams];
  dateRange?: undefined;
} | {
  partitionGranularity: string;
  preAggregationStartEndQueries: [QueryWithParams, QueryWithParams];
  // Assigned by evaluateQueryWithPreAggregations
  dateRange?: [string, string];
});

export type TestDbConnection = {
  testQueries(queries: unknown[][], fixture?: unknown): Promise<unknown>;
  close?(): Promise<unknown>;
};

export class BaseDbRunner {
  protected containerLazyInitPromise: any = null;

  protected connectionLazyInitPromise: any = null;

  protected container: any;

  protected connection: any;

  protected nextSeed: number = 1;

  public testQuery(query: unknown[], fixture: unknown = null) {
    return this.testQueries([query], fixture);
  }

  protected newTestQuery(_compilers: Compilers, _query: BaseQueryOptions): BaseQuery {
    throw new Error('newTestQuery not implemented');
  }

  public async runQueryTest(q: BaseQueryOptions & Record<string, unknown>, expectedResult: Record<string, unknown>[], compilers: Compilers) {
    await compilers.compile();
    const query = this.newTestQuery(compilers, q);

    console.log(query.buildSqlAndParams());
    const sqlAndParams = query.buildSqlAndParams();

    const res = await this.testQuery(sqlAndParams);
    console.log(JSON.stringify(res));

    expect(res).toEqual(
      expectedResult
    );
    return sqlAndParams;
  }

  public async testQueries(queries: unknown[][], fixture: unknown = null) {
    queries.forEach(q => {
      console.log(q[0]);
      console.log(q[1]);
    });
    if (this.containerLazyInitPromise) {
      await this.containerLazyInitPromise;
    }

    if (!this.container && !process.env.TEST_LOCAL) {
      console.log('[Container] Starting');

      this.containerLazyInitPromise = this.containerLazyInit();

      try {
        this.container = await this.containerLazyInitPromise;

        console.log(`[Container] Started ${this.container.getId()}`);
      } finally {
        this.containerLazyInitPromise = null;
      }
    }

    if (this.connectionLazyInitPromise) {
      await this.connectionLazyInitPromise;
    }

    if (!this.connection) {
      const port = this.container ? this.container.getMappedPort(this.port()) : this.port();
      console.log('[Connection] Initializing');

      this.connectionLazyInitPromise = this.connectionLazyInit(port);

      try {
        this.connection = await this.connectionLazyInitPromise;
      } finally {
        this.connectionLazyInitPromise = null;
      }

      console.log('[Connection] Initialized');
    }
    return this.connection.testQueries(queries, fixture);
  }

  public replaceTableName(query: QueryWithParams, preAggregation: TestPreAggregationDescription | TestPreAggregationDescription[], suffix: string | number): QueryWithParams {
    const [toReplace, params] = query;
    preAggregation = Array.isArray(preAggregation) ? preAggregation : [preAggregation];
    return [
      preAggregation.reduce(
        // `any`: matchedTimeDimensionDateRange is undefined when unset, which intersectDateRanges' signature omits
        (replacedQuery: string, desc: any) => {
          const partitionUnion = desc.dateRange && PreAggregationPartitionRangeLoader.timeSeries(
            desc.partitionGranularity,
            PreAggregationPartitionRangeLoader.intersectDateRanges(desc.dateRange, desc.matchedTimeDimensionDateRange),
            desc.timestampPrecision
          ).map(
            range => `SELECT * FROM ${PreAggregationPartitionRangeLoader.partitionTableName(desc.tableName, desc.partitionGranularity, range)}_${suffix}`
          ).join(' UNION ALL ');
          const targetTableName = desc.dateRange ? `(${partitionUnion})` : `${desc.tableName}_${suffix}`;
          // Replace usage-suffixed placeholders first (e.g. tableName__usage_0)
          if (desc.usageMapping) {
            for (const usageSuffix of Object.keys(desc.usageMapping)) {
              replacedQuery = replacedQuery.replace(
                new RegExp(`${desc.tableName}${usageSuffix.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\s+`, 'g'),
                `${targetTableName} `
              );
            }
          }
          // Replace base table name
          return replacedQuery.replace(
            new RegExp(`${desc.tableName}\\s+`, 'g'),
            `${targetTableName} `
          );
        },
        toReplace
      ),
      params
    ];
  }

  public replacePartitionName(
    query: QueryWithParams | InvalidateKeyQuery,
    desc: TestPreAggregationDescription,
    suffix: string | number,
    partitionGranularity: string | null = null,
    dateRange: [string, string] | null = null
  ): QueryWithParams {
    const [toReplace, params] = query;
    const tableName = partitionGranularity && dateRange ? PreAggregationPartitionRangeLoader.partitionTableName(
      desc.tableName, partitionGranularity, dateRange
    ) : desc.tableName;
    const replaced = toReplace
      .replace(new RegExp(desc.tableName, 'g'), `${tableName}_${suffix}`)
      .replace(/CREATE INDEX (?!i_)/, `CREATE INDEX i_${suffix}_`);
    // TODO can be reused from PreAggregationPartitionRangeLoader
    return [
      replaced,
      params.map(
        (param: unknown) => {
          if (dateRange && param === PreAggregationPartitionRangeLoader.FROM_PARTITION_RANGE) {
            return PreAggregationPartitionRangeLoader.inDbTimeZone(desc, dateRange[0]);
          } else if (dateRange && param === PreAggregationPartitionRangeLoader.TO_PARTITION_RANGE) {
            return PreAggregationPartitionRangeLoader.inDbTimeZone(desc, dateRange[1]);
          } else {
            return param;
          }
        },
      )
    ];
  }

  // `any`: callers hand over preAggregationsDescription() results held in untyped/possibly-unset locals
  public tempTablePreAggregations(preAggregationsDescriptions: any, seed = this.nextSeed++) {
    return R.unnest(preAggregationsDescriptions.map(
      // `any`: matchedTimeDimensionDateRange is undefined when unset, which intersectDateRanges' signature omits
      (desc: any) => {
        const loadSql = this.tempTableSql(desc);
        return desc.dateRange ? R.unnest(PreAggregationPartitionRangeLoader.timeSeries(
          desc.partitionGranularity,
          PreAggregationPartitionRangeLoader.intersectDateRanges(desc.dateRange, desc.matchedTimeDimensionDateRange),
          desc.timestampPrecision
        ).map(
          range => desc.invalidateKeyQueries.map(
            (sql: InvalidateKeyQuery) => this.replacePartitionName(sql, desc, seed, desc.partitionGranularity, range)
          ).concat([this.replaceTableName(this.replacePartitionName([
            loadSql,
            desc.loadSql[1]
          ], desc, seed, desc.partitionGranularity, range), preAggregationsDescriptions, seed)]).concat(
            (desc.indexesSql || []).map(
              ({ sql }: { sql: QueryWithParams }) => this.replacePartitionName(sql, desc, seed, desc.partitionGranularity, range)
            ),
          )
        )) : desc.invalidateKeyQueries.map(
          (sql: InvalidateKeyQuery) => this.replacePartitionName(sql, desc, seed)
        ).concat([this.replaceTableName(this.replacePartitionName(
          [loadSql, desc.loadSql[1]], desc, seed
        ), preAggregationsDescriptions, seed)]).concat(
          (desc.indexesSql || []).map(({ sql }: { sql: QueryWithParams }) => this.replacePartitionName(sql, desc, seed)),
        );
      }
    ));
  }

  protected tempTableSql(desc: TestPreAggregationDescription): string {
    return desc.loadSql[0].replace('CREATE TABLE', 'CREATE TEMP TABLE');
  }

  // `any`: some callers pass a query held in a loop-assigned `let`, which TS sees as possibly undefined
  public async evaluateQueryWithPreAggregations(query: any, seed = this.nextSeed++) {
    const preAggregationsDescription: TestPreAggregationDescription[] = query.preAggregations?.preAggregationsDescription();
    await Promise.all(preAggregationsDescription.map(
      async (desc: TestPreAggregationDescription) => {
        if (desc.partitionGranularity) {
          desc.dateRange = [
            PreAggregationPartitionRangeLoader.extractDate(
              await this.testQueries([desc.preAggregationStartEndQueries[0]]),
              desc.timezone,
            ),
            PreAggregationPartitionRangeLoader.extractDate(
              await this.testQueries([desc.preAggregationStartEndQueries[1]]),
              desc.timezone,
            )
          ];
          // console.log(desc);
        }
      }
    ));

    return this.testQueries(this.tempTablePreAggregations(preAggregationsDescription, seed).concat([
      this.replaceTableName(query.buildSqlAndParams(), preAggregationsDescription, seed)
    ]));
  }

  public async tearDown() {
    console.log('[TearDown] Starting');

    if (this.containerLazyInitPromise) {
      throw new Error('container was not resolved before tearDown');
    }

    if (this.connectionLazyInitPromise) {
      throw new Error('connection was not resolved before tearDown');
    }

    if (this.connection) {
      console.log('[Connection] Closing');

      if (this.connection.close) {
        try {
          await this.connection.close();
        } catch (e) {
          console.log(e);
        }
      }

      this.connection = null;

      console.log('[Connection] Closed');
    }

    if (this.container) {
      console.log(`[Container] Shutdown ${this.container.getId()}`);

      await this.container.stop();

      console.log(`[Container] Stopped ${this.container.getId()}`);

      this.container = null;
    }

    console.log('[TearDown] Finished');
  }

  // eslint-disable-next-line no-unused-vars,@typescript-eslint/no-unused-vars
  public async connectionLazyInit(port: number): Promise<TestDbConnection> {
    throw new Error('Not implemented connectionLazyInit');
  }

  public async containerLazyInit() {
    throw new Error('Not implemented containerLazyInit');
  }

  public port() {
    throw new Error('Not implemented port');
  }
}
