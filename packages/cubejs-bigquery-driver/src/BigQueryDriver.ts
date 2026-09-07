/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview The `BigQueryDriver` and related types declaration.
 */

import {
  getEnv,
  assertDataSource,
  extractRequestUUID,
  pausePromise,
  Required,
} from '@cubejs-backend/shared';
import R from 'ramda';
import {
  BigQuery,
  BigQueryOptions,
  Dataset,
  Job,
  QueryRowsResponse,
} from '@google-cloud/bigquery';
import { Bucket, Storage } from '@google-cloud/storage';
import {
  BaseDriver,
  DatabaseStructure,
  DriverCapabilities,
  DriverInterface,
  prependSqlPreamble,
  resolveSqlPreamble,
  trySplitSqlPreamble,
  QueryColumnsResult,
  QueryOptions,
  QuerySchemasResult,
  QueryTablesResult,
  StreamOptions,
  StreamTableData,
  TableCSVData,
} from '@cubejs-backend/base-driver';
import type { Query } from '@google-cloud/bigquery/build/src/bigquery';

import { HydrationStream, transformRow } from './HydrationStream';

import { version } from '../package.json';

interface BigQueryDriverOptions extends BigQueryOptions {
  readOnly?: boolean
  projectId?: string,
  keyFilename?: string,
  exportBucket?: string,
  location?: string,
  pollTimeout?: number,
  pollMaxInterval?: number,

  /**
   * The export bucket CSV file escape symbol.
   */
  exportBucketCsvEscapeSymbol?: string,
}

type BigQueryDriverOptionsInitialized =
  Required<BigQueryDriverOptions, 'pollTimeout' | 'pollMaxInterval'>;

// BigQuery type mappings for types not in the base DbTypeToGenericType
const BigQueryToGenericType: Record<string, string> = {
  bignumeric: 'decimal',
  bigdecimal: 'decimal',
  decimal: 'decimal'
};

/**
 * BigQuery driver.
 */
export class BigQueryDriver extends BaseDriver implements DriverInterface {
  /**
   * Returns default concurrency value.
   */
  public static getDefaultConcurrency(): number {
    return 10;
  }

  protected readonly options: BigQueryDriverOptionsInitialized;

  protected readonly bigquery: BigQuery;

  protected readonly storage: Storage | null = null;

  protected readonly bucket: Bucket | null = null;

  /**
   * Class constructor.
   */
  public constructor(
    config: BigQueryDriverOptions & {
      /**
       * Data source name.
       */
      dataSource?: string,

      /**
       * Whether this driver is used for pre-aggregations.
       */
      preAggregations?: boolean,
      preAggregationsSqlPreamble?: boolean,

      /**
       * Max pool size value for the [cube]<-->[db] pool.
       */
      maxPoolSize?: number,

      /**
       * Time to wait for a response from a connection after validation
       * request before determining it as not valid. Default - 10000 ms.
       */
      testConnectionTimeout?: number,

      /**
       * SQL prepended to every query on this connection.
       */
      sqlPreamble?: string,
    } = {}
  ) {
    const dataSource =
      config.dataSource ||
      assertDataSource('default');
    const preAggregations = config.preAggregations || false;
    // Pre-aggregation builds resolve the preamble from the pre-aggregation
    // namespace even when their credentials do not, since the preamble is not a
    // connection target. Falls back to `preAggregations` for a driver
    // constructed directly rather than through the server's driver factory.
    const preAggregationsSqlPreamble = config.preAggregationsSqlPreamble ?? preAggregations;

    super({
      testConnectionTimeout: config.testConnectionTimeout,
      sqlPreamble: resolveSqlPreamble(config, getEnv('dbSqlPreamble', { dataSource, preAggregations: preAggregationsSqlPreamble })),
    });

    this.options = {
      scopes: [
        'https://www.googleapis.com/auth/bigquery',
        'https://www.googleapis.com/auth/drive',
      ],
      projectId: getEnv('bigqueryProjectId', { dataSource, preAggregations }),
      keyFilename: getEnv('bigqueryKeyFile', { dataSource, preAggregations }),
      credentials: getEnv('bigqueryCredentials', { dataSource, preAggregations })
        ? JSON.parse(
          Buffer.from(
            getEnv('bigqueryCredentials', { dataSource, preAggregations }),
            'base64',
          ).toString('utf8')
        )
        : undefined,
      exportBucket:
        getEnv('dbExportBucket', { dataSource, preAggregations }) ||
        getEnv('bigqueryExportBucket', { dataSource, preAggregations }),
      location: getEnv('bigqueryLocation', { dataSource, preAggregations }),
      ...config,
      pollTimeout: (
        config.pollTimeout ||
        getEnv('dbPollTimeout', { dataSource, preAggregations }) ||
        getEnv('dbQueryTimeout', { dataSource, preAggregations })
      ) * 1000,
      pollMaxInterval: (
        config.pollMaxInterval ||
        getEnv('dbPollMaxInterval', { dataSource, preAggregations })
      ) * 1000,
      exportBucketCsvEscapeSymbol: getEnv('dbExportBucketCsvEscapeSymbol', { dataSource, preAggregations }),
      userAgent: `CubeDev_Cube/${version}`,
    };

    getEnv('dbExportBucketType', {
      dataSource,
      preAggregations,
      supported: ['gcp'],
    });

    this.bigquery = new BigQuery(this.options);
    if (this.options.exportBucket) {
      this.storage = new Storage(this.options);
      this.bucket = this.storage.bucket(this.options.exportBucket);
    }
  }

  /**
   * Returns the configurable driver options
   * Note: It returns the unprefixed option names.
   * In case of using multisources options need to be prefixed manually.
   */
  public static driverEnvVariables() {
    return [
      'CUBEJS_DB_BQ_PROJECT_ID',
      'CUBEJS_DB_BQ_KEY_FILE',
    ];
  }

  public async testConnection() {
    // From the BigQuery Docs:
    // You are not charged for list, get, patch, update and delete calls.
    // Examples include (but are not limited to): listing datasets, updating
    // a dataset's access control list, updating a table's description, or
    // listing user-defined functions in a dataset.
    // @see https://cloud.google.com/bigquery/pricing#free
    await this.bigquery.getDatasets();
  }

  /**
   * This driver applies `sql_preamble`.
   *
   * Prepended into the query text, since BigQuery is stateless.
   */
  public override supportsSqlPreamble(): boolean {
    return true;
  }

  public readOnly() {
    return !!this.options.readOnly;
  }

  public async query<R = unknown>(query: string, values: unknown[], options?: QueryOptions): Promise<R[]> {
    const data = await this.runQueryJob({
      query,
      params: values,
      parameterMode: 'positional',
      useLegacySql: false,
      wrapIntegers: true,
    }, options);

    return <any>(
      data[0] && data[0].map(
        row => transformRow(row)
      )
    );
  }

  protected async loadTablesForDataset(dataset: Dataset) {
    try {
      const result = await dataset.query({
        query: `
        SELECT
          columns.column_name as ${this.quoteIdentifier('column_name')},
          columns.table_name as ${this.quoteIdentifier('table_name')},
          columns.table_schema as ${this.quoteIdentifier('table_schema')},
          columns.data_type as ${this.quoteIdentifier('data_type')}
        FROM INFORMATION_SCHEMA.COLUMNS
      `
      });

      if (result.length) {
        return R.reduce(
          this.informationColumnsSchemaReducer, {}, this.informationColumnsSchemaSorter(result[0])
        );
      }

      return {};
    } catch (e) {
      if ((<any>e).message.includes('Permission bigquery.tables.get denied on table')) {
        return {};
      }

      throw e;
    }
  }

  public async tablesSchema(): Promise<DatabaseStructure> {
    const dataSets = await this.bigquery.getDatasets();
    const dataSetsColumns = await Promise.all(
      dataSets[0].map((dataSet) => this.loadTablesForDataset(dataSet))
    );

    return dataSetsColumns.reduce((prev, current) => Object.assign(prev, current), {});
  }

  public override async getSchemas(): Promise<QuerySchemasResult[]> {
    const dataSets = await this.bigquery.getDatasets();
    return dataSets[0].filter((dataSet) => dataSet.id).map((dataSet) => ({
      schema_name: dataSet.id!,
    }));
  }

  public override async getTablesForSpecificSchemas(schemas: QuerySchemasResult[]): Promise<QueryTablesResult[]> {
    try {
      const allTablePromises = schemas.map(async schema => {
        const tables = await this.getTablesQuery(schema.schema_name);
        return tables
          .filter(table => table.table_name)
          .map(table => ({ schema_name: schema.schema_name, table_name: table.table_name! }));
      });

      const allTables = await Promise.all(allTablePromises);

      return allTables.flat();
    } catch (e) {
      console.error('Error fetching tables for schemas:', e);
      throw e;
    }
  }

  public override async getColumnsForSpecificTables(tables: QueryTablesResult[]): Promise<QueryColumnsResult[]> {
    try {
      const allColumnPromises = tables.map(async table => {
        const tableName = `${table.schema_name}.${table.table_name}`;
        const columns = await this.tableColumnTypes(tableName);
        return columns.map((column: any) => ({
          schema_name: table.schema_name,
          table_name: table.table_name,
          data_type: column.type,
          column_name: column.name,
        }));
      });

      const allColumns = await Promise.all(allColumnPromises);

      return allColumns.flat();
    } catch (e) {
      console.error('Error fetching columns for tables:', e);
      throw e;
    }
  }

  public async getTablesQuery(schemaName: string) {
    try {
      const dataSet = await this.bigquery.dataset(schemaName);
      if (!dataSet) {
        return [];
      }
      const [tables] = await this.bigquery.dataset(schemaName).getTables();
      return tables.map(t => ({ table_name: t.id }));
    } catch (e) {
      if ((<any>e).toString().indexOf('Not found')) {
        return [];
      }
      throw e;
    }
  }

  public async tableColumnTypes(table: string) {
    const [schema, name] = table.split('.');
    const [bigQueryTable] = await this.bigquery.dataset(schema).table(name).getMetadata();
    return bigQueryTable.schema.fields.map((c: any) => {
      // BigQuery NUMERIC is always (38, 9), BIGNUMERIC is (76, 38)
      // https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#decimal_types
      if (c.type === 'NUMERIC' || c.type === 'DECIMAL') {
        return { name: c.name, type: this.toGenericType(c.type, 38, 9) };
      }
      if (c.type === 'BIGNUMERIC' || c.type === 'BIGDECIMAL') {
        return { name: c.name, type: this.toGenericType(c.type, 76, 38) };
      }
      return { name: c.name, type: this.toGenericType(c.type) };
    });
  }

  public async createSchemaIfNotExists(schemaName: string): Promise<void> {
    await this.bigquery.dataset(schemaName).get({ autoCreate: true });
  }

  public async isUnloadSupported() {
    return this.bucket !== null;
  }

  public async stream(
    query: string,
    values: unknown[],
    options?: StreamOptions
  ): Promise<StreamTableData> {
    const labels = this.buildQueryLabels(options);
    // Streaming does not go through runQueryJob, so it needs the preamble too.
    const stream = await this.bigquery.createQueryStream({
      query: prependSqlPreamble(query, this.sqlPreamble()),
      params: values,
      parameterMode: 'positional',
      useLegacySql: false,
      wrapIntegers: true,
      ...(labels ? { labels } : {}),
    });

    const rowStream = new HydrationStream();
    stream.pipe(rowStream);

    return {
      rowStream,
    };
  }

  public async unload(table: string): Promise<TableCSVData> {
    if (!this.bucket) {
      throw new Error('Unload is not configured');
    }

    const destination = this.bucket.file(`${table}-*.csv.gz`);
    const [schema, tableName] = table.split('.');
    const bigQueryTable = this.bigquery.dataset(schema).table(tableName);
    const [job] = await bigQueryTable.createExtractJob(destination, { format: 'CSV', gzip: true });
    await this.waitForJobResult(job, { table }, false);
    // There is an implementation for extracting and signing urls from S3
    // @see BaseDriver->extractUnloadedFilesFromS3()
    // Please use that if you need. Here is a different flow
    // because bigquery requires storage/bucket object for other things,
    // and there is no need to initiate another one (created in extractUnloadedFilesFromS3()).
    const [files] = await this.bucket.getFiles({ prefix: `${table}-` });
    const urls = await Promise.all(files.map(async file => {
      const [url] = await file.getSignedUrl({
        action: 'read',
        expires: new Date(new Date().getTime() + 60 * 60 * 1000),
      });
      return url;
    }));

    return {
      exportBucketCsvEscapeSymbol: this.options.exportBucketCsvEscapeSymbol,
      csvFile: urls,
    };
  }

  public async loadPreAggregationIntoTable(
    preAggregationTableName: string,
    loadSql: string,
    params: any,
    options: any
  ): Promise<any> {
    const [dataSet, tableName] = preAggregationTableName.split('.');

    const bigQueryQuery: Query = {
      query: loadSql,
      params,
      parameterMode: 'positional',
      destination: this.bigquery.dataset(dataSet).table(tableName),
      createDisposition: 'CREATE_IF_NEEDED',
      useLegacySql: false
    };

    return this.runQueryJob(bigQueryQuery, options, false);
  }

  protected async awaitForJobStatus(job: Job, options: any, withResults: boolean) {
    const [result] = await job.getMetadata();

    if (result.status && result.status.state === 'DONE') {
      if (result.status.errorResult) {
        throw new Error(
          result.status.errorResult.message ?
            result.status.errorResult.message :
            JSON.stringify(result.status.errorResult)
        );
      }
      this.reportQueryUsage(result.statistics, options);
    } else {
      return null;
    }

    return withResults ? job.getQueryResults({ wrapIntegers: true }) : true;
  }

  /**
   * @see https://cloud.google.com/bigquery/docs/labels-intro#requirements
   */
  protected buildQueryLabels(options?: QueryOptions): { [k: string]: string } | undefined {
    const requestId = options?.requestId;
    if (!requestId) {
      return undefined;
    }

    const queryUuid = extractRequestUUID(String(requestId));

    const value = queryUuid.toLowerCase().replace(/[^a-z0-9_-]/g, '_').slice(0, 63);
    if (!value) {
      return undefined;
    }

    return { cube_request_id: value };
  }

  /**
   * BigQuery is stateless — there is no session to carry a preamble — so the
   * preamble is prepended into the query text and travels in the same job.
   * That is the only placement under which a `CREATE TEMP FUNCTION` is visible
   * to the query, since a temporary UDF lives for exactly one query.
   *
   * Sessions would be the alternative and are unusable here: BigQuery forbids
   * concurrent queries within a session, which a BI backend serving concurrent
   * users cannot accept.
   *
   * One caveat is enforced rather than documented. A multi-statement request is
   * a *script*, and a script job ignores `destinationTable` — so prepending onto
   * a pre-aggregation load would let the build report success while writing
   * nothing. BigQuery exempts one shape: statements that are all
   * `CREATE TEMP FUNCTION` followed by a single query. A preamble outside that
   * shape is therefore refused on destination jobs instead of silently
   * discarding the result.
   */
  protected withSqlPreamble(bigQueryQuery: Query): Query {
    const preamble = this.sqlPreamble();

    if (!preamble || typeof bigQueryQuery.query !== 'string') {
      return bigQueryQuery;
    }

    if (bigQueryQuery.destination && !BigQueryDriver.isScriptSafePreamble(preamble)) {
      throw new Error(
        'CUBEJS_DB_SQL_PREAMBLE cannot be applied to a pre-aggregation build on BigQuery unless it ' +
        'contains only CREATE TEMP FUNCTION statements whose boundaries are unambiguous. BigQuery ' +
        'runs any other multi-statement request as a script, and a script job ignores the ' +
        'destination table, so the build would write no rows. Restrict the preamble to CREATE TEMP ' +
        'FUNCTION — avoiding nested block comments and unterminated literals, which make the ' +
        'statement boundaries undecidable — or set a pre-aggregation specific preamble that does.'
      );
    }

    return { ...bigQueryQuery, query: prependSqlPreamble(bigQueryQuery.query, preamble) };
  }

  /**
   * True when every statement is a `CREATE TEMP FUNCTION`, the one multi-statement
   * shape BigQuery still runs as a normal query rather than a script.
   *
   * Fails closed on an ambiguous blob. The splitter hands back the whole blob as
   * one entry when it cannot find the boundaries confidently, and that entry may
   * still contain several statements — judging it script-safe because it *starts*
   * with `CREATE TEMP FUNCTION` would let a script onto a destination job, which
   * is the silent-empty-table case this guard exists to prevent.
   */
  protected static isScriptSafePreamble(preamble: string): boolean {
    const { statements, ambiguous } = trySplitSqlPreamble(preamble);

    if (ambiguous) {
      return false;
    }

    return statements.length > 0 && statements.every(
      statement => /^create\s+(or\s+replace\s+)?temp(orary)?\s+function\b/i
        .test(BigQueryDriver.withoutLeadingComments(statement))
    );
  }

  /**
   * Drops leading comments so a documented UDF definition is still recognized as
   * the script-exempt shape. Comments are kept in the statement text, and a
   * commented `CREATE TEMP FUNCTION` would otherwise be refused on a
   * pre-aggregation build with a message telling the user to do what they did.
   */
  private static withoutLeadingComments(statement: string): string {
    let rest = statement.trimStart();

    for (;;) {
      if (rest.startsWith('--') || rest.startsWith('#')) {
        const lineEnd = rest.indexOf('\n');
        if (lineEnd === -1) {
          return '';
        }
        rest = rest.slice(lineEnd + 1).trimStart();
      } else if (rest.startsWith('/*')) {
        const blockEnd = rest.indexOf('*/');
        if (blockEnd === -1) {
          return '';
        }
        rest = rest.slice(blockEnd + 2).trimStart();
      } else {
        return rest;
      }
    }
  }

  protected async runQueryJob<T = QueryRowsResponse>(
    bigQueryQuery: Query,
    options: any,
    withResults: boolean = true
  ): Promise<T> {
    const labels = this.buildQueryLabels(options);
    const withPreamble = this.withSqlPreamble(bigQueryQuery);
    const jobRequest: Query = labels
      ? { ...withPreamble, labels: { ...withPreamble.labels, ...labels } }
      : withPreamble;
    const [job] = await this.bigquery.createQueryJob(jobRequest);

    return <any> this.waitForJobResult(job, options, withResults);
  }

  protected async waitForJobResult(job: Job, options: any, withResults: boolean) {
    const startedTime = Date.now();

    for (let i = 0; Date.now() - startedTime <= this.options.pollTimeout; i++) {
      const result = await this.awaitForJobStatus(job, options, withResults);
      if (result) {
        return result;
      }

      await pausePromise(
        Math.min(this.options.pollMaxInterval, 200 * i),
      );
    }

    await job.cancel();

    throw new Error(
      `BigQuery job timeout reached ${this.options.pollTimeout}ms`,
    );
  }

  public quoteIdentifier(identifier: string) {
    const nestedFields = identifier.split('.');
    return nestedFields.map(name => {
      if (name.match(/^[a-z0-9_]+$/)) {
        return name;
      }
      return `\`${identifier}\``;
    }).join('.');
  }

  public capabilities(): DriverCapabilities {
    return {
      incrementalSchemaLoading: true,
    };
  }

  protected override toGenericType(columnType: string, precision?: number | null, scale?: number | null): string {
    const mappedType = BigQueryToGenericType[columnType.toLowerCase()] || columnType;
    return super.toGenericType(mappedType, precision, scale);
  }
}
