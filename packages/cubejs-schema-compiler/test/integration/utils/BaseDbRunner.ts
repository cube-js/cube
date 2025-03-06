import R from 'ramda';
import { PreAggregationPartitionRangeLoader } from '@cubejs-backend/query-orchestrator';
import { BaseQuery } from '../../../src';

export class BaseDbRunner {
  protected containerLazyInitPromise: any = null;

  protected connectionLazyInitPromise: any = null;

  protected container: any;

  protected connection: any;

  protected nextSeed: number = 1;

  public testQuery(query, fixture: any = null) {
    return this.testQueries([query], fixture);
  }

  protected newTestQuery(_compilers: any, _query: any): BaseQuery {
    throw new Error('newTestQuery not implemented');
  }

  public async runQueryTest(q, expectedResult, { compiler, joinGraph, cubeEvaluator }) {
    await compiler.compile();
    const query = this.newTestQuery({ joinGraph, cubeEvaluator, compiler }, q);

    console.log(query.buildSqlAndParams());

    const res = await this.testQuery(query.buildSqlAndParams());
    console.log(JSON.stringify(res));

    expect(res).toEqual(
      expectedResult
    );
  }

  public async testQueries(queries, fixture: any = null) {
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

  public replaceTableName(query, preAggregation, suffix) {
    const [toReplace, params] = query;
    preAggregation = Array.isArray(preAggregation) ? preAggregation : [preAggregation];
    return [
      preAggregation.reduce(
        (replacedQuery, desc) => {
          const partitionUnion = desc.dateRange && PreAggregationPartitionRangeLoader.timeSeries(
            desc.partitionGranularity,
            PreAggregationPartitionRangeLoader.intersectDateRanges(desc.dateRange, desc.matchedTimeDimensionDateRange),
            desc.timestampPrecision
          ).map(
            range => `SELECT * FROM ${PreAggregationPartitionRangeLoader.partitionTableName(desc.tableName, desc.partitionGranularity, range)}_${suffix}`
          ).join(' UNION ALL ');
          const targetTableName = desc.dateRange ? `(${partitionUnion})` : `${desc.tableName}_${suffix}`;
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
    query,
    desc,
    suffix,
    partitionGranularity: string | null = null,
    dateRange: [string, string] | null = null
  ) {
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
        param => {
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

  public tempTablePreAggregations(preAggregationsDescriptions, seed = this.nextSeed++) {
    return R.unnest(preAggregationsDescriptions.map(
      desc => {
        const loadSql = this.tempTableSql(desc);
        return desc.dateRange ? R.unnest(PreAggregationPartitionRangeLoader.timeSeries(
          desc.partitionGranularity,
          PreAggregationPartitionRangeLoader.intersectDateRanges(desc.dateRange, desc.matchedTimeDimensionDateRange),
          desc.timestampPrecision
        ).map(
          range => desc.invalidateKeyQueries.map(
            (sql) => this.replacePartitionName(sql, desc, seed, desc.partitionGranularity, range)
          ).concat([this.replaceTableName(this.replacePartitionName([
            loadSql,
            desc.loadSql[1]
          ], desc, seed, desc.partitionGranularity, range), preAggregationsDescriptions, seed)]).concat(
            (desc.indexesSql || []).map(
              ({ sql }) => this.replacePartitionName(sql, desc, seed, desc.partitionGranularity, range)
            ),
          )
        )) : desc.invalidateKeyQueries.map(
          (sql) => this.replacePartitionName(sql, desc, seed)
        ).concat([this.replaceTableName(this.replacePartitionName(
          [loadSql, desc.loadSql[1]], desc, seed
        ), preAggregationsDescriptions, seed)]).concat(
          (desc.indexesSql || []).map(({ sql }) => this.replacePartitionName(sql, desc, seed)),
        );
      }
    ));
  }

  protected tempTableSql(desc) {
    return desc.loadSql[0].replace('CREATE TABLE', 'CREATE TEMP TABLE');
  }

  public async evaluateQueryWithPreAggregations(query, seed = this.nextSeed++) {
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    // console.log(preAggregationsDescription);

    await Promise.all(preAggregationsDescription.map(
      async desc => {
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
  public async connectionLazyInit(port) {
    throw new Error('Not implemented connectionLazyInit');
  }

  public async containerLazyInit() {
    throw new Error('Not implemented containerLazyInit');
  }

  public port() {
    throw new Error('Not implemented port');
  }
}
