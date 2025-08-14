/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview The `AthenaDriver` and related types declaration.
 */

import {
  getEnv,
  assertDataSource,
  checkNonNullable,
  pausePromise,
  Required,
} from '@cubejs-backend/shared';
import {
  Athena,
  GetQueryResultsCommandOutput,
  ColumnInfo,
  StartQueryExecutionCommandInput,
} from '@aws-sdk/client-athena';
import * as stream from 'stream';
import {
  BaseDriver,
  DatabaseStructure,
  DownloadTableCSVData,
  DriverInterface,
  QueryOptions,
  UnloadOptions,
  StreamOptions,
  TableStructure,
  DriverCapabilities,
  Row,
  DownloadTableMemoryData,
  StreamTableDataWithTypes,
  DownloadQueryResultsResult,
  DownloadQueryResultsOptions,
} from '@cubejs-backend/base-driver';
import * as SqlString from 'sqlstring';
import { AthenaClientConfig } from '@aws-sdk/client-athena/dist-types/AthenaClient';
import { fromTemporaryCredentials } from '@aws-sdk/credential-providers';
import { URL } from 'url';

interface AthenaDriverOptions extends AthenaClientConfig {
  readOnly?: boolean
  accessKeyId?: string
  secretAccessKey?: string
  workGroup?: string
  catalog?: string
  schema?: string
  database?: string
  S3OutputLocation?: string
  exportBucket?: string
  pollTimeout?: number
  pollMaxInterval?: number

  /**
   * The export bucket CSV file escape symbol.
   */
  exportBucketCsvEscapeSymbol?: string
}

type AthenaDriverOptionsInitialized = Required<AthenaDriverOptions, 'pollTimeout' | 'pollMaxInterval'>;

export interface AthenaQueryId {
  QueryExecutionId: string;
}

function applyParams(query: string, params: any[]): string {
  return SqlString.format(query, params);
}

interface AthenaTable {
  schema: string
  name: string
}

export class AthenaDriver extends BaseDriver implements DriverInterface {
  /**
   * Returns default concurrency value.
   */
  public static getDefaultConcurrency(): number {
    return 10;
  }

  private config: AthenaDriverOptionsInitialized;

  private athena: Athena;

  private schema: string;

  /**
   * Class constructor.
   */
  public constructor(
    config: AthenaDriverOptions & {
      /**
       * Data source name.
       */
      dataSource?: string,

      /**
       * Max pool size value for the [cube]<-->[db] pool.
       */
      maxPoolSize?: number,

      /**
       * Time to wait for a response from a connection after validation
       * request before determining it as not valid. Default - 10000 ms.
       */
      testConnectionTimeout?: number,
    } = {},
  ) {
    super({
      testConnectionTimeout: config.testConnectionTimeout,
    });

    const dataSource =
      config.dataSource ||
      assertDataSource('default');

    const accessKeyId =
      config.accessKeyId ||
      getEnv('athenaAwsKey', { dataSource });

    const secretAccessKey =
      config.secretAccessKey ||
      getEnv('athenaAwsSecret', { dataSource });

    const assumeRoleArn = getEnv('athenaAwsAssumeRoleArn', { dataSource });
    const assumeRoleExternalId = getEnv('athenaAwsAssumeRoleExternalId', { dataSource });

    const { schema, ...restConfig } = config;

    this.schema = schema ||
      getEnv('dbName', { dataSource }) ||
      getEnv('dbSchema', { dataSource });

    // Configure credentials based on authentication method
    let credentials;
    if (assumeRoleArn) {
      // Use assume role authentication
      credentials = fromTemporaryCredentials({
        params: {
          RoleArn: assumeRoleArn,
          ...(assumeRoleExternalId && { ExternalId: assumeRoleExternalId }),
        },
        ...(accessKeyId && secretAccessKey && {
          masterCredentials: { accessKeyId, secretAccessKey },
        }),
      });
    } else if (accessKeyId && secretAccessKey) {
      // If access key and secret are provided, use them as master credentials
      // Otherwise, let the SDK use the default credential chain (IRSA, instance profile, etc.)
      credentials = { accessKeyId, secretAccessKey };
    }

    this.config = {
      // If no credentials are provided, the SDK will use the default chain
      ...(credentials && { credentials }),
      ...restConfig,
      region:
        config.region ||
        getEnv('athenaAwsRegion', { dataSource }),
      S3OutputLocation:
        config.S3OutputLocation ||
        getEnv('athenaAwsS3OutputLocation', { dataSource }),
      workGroup:
        config.workGroup ||
        getEnv('athenaAwsWorkgroup', { dataSource }) ||
        'primary',
      catalog:
        config.catalog ||
        getEnv('athenaAwsCatalog', { dataSource }),
      database:
        config.database ||
        getEnv('dbName', { dataSource }),
      exportBucket:
        config.exportBucket ||
        getEnv('dbExportBucket', { dataSource }),
      pollTimeout: (
        config.pollTimeout ||
        getEnv('dbPollTimeout', { dataSource }) ||
        getEnv('dbQueryTimeout', { dataSource })
      ) * 1000,
      pollMaxInterval: (
        config.pollMaxInterval ||
        getEnv('dbPollMaxInterval', { dataSource })
      ) * 1000,
      exportBucketCsvEscapeSymbol:
        getEnv('dbExportBucketCsvEscapeSymbol', { dataSource }),
    };
    if (this.config.exportBucket) {
      this.config.exportBucket =
        AthenaDriver.normalizeS3Path(this.config.exportBucket);
    }

    if (typeof this.config.readOnly === 'undefined') {
      // If Export bucket configuration is in place we want to always use it instead of batching
      this.config.readOnly = !this.isUnloadSupported();
    }

    this.athena = new Athena(this.config);
  }

  /**
   * Driver read-only flag.
   */
  public readOnly(): boolean {
    return !!this.config.readOnly;
  }

  /**
   * Returns driver's capabilities object.
   */
  public capabilities(): DriverCapabilities {
    return {
      unloadWithoutTempTable: true,
      incrementalSchemaLoading: true,
    };
  }

  /**
   * Test driver's connection.
   */
  public async testConnection() {
    await this.athena.getWorkGroup({
      WorkGroup: this.config.workGroup
    });
  }

  /**
   * Executes a query and returns either query result memory data or
   * query result stream, depending on options.
   */
  public async downloadQueryResults(
    query: string,
    values: unknown[],
    options: DownloadQueryResultsOptions,
  ): Promise<DownloadQueryResultsResult> {
    if (!options.streamImport) {
      return this.memory(query, values);
    } else {
      return this.stream(query, values, options);
    }
  }

  /**
   * Executes query and returns table memory data that includes rows
   * and queried fields types.
   */
  public async memory(
    query: string,
    values: unknown[],
  ): Promise<DownloadTableMemoryData & { types: TableStructure }> {
    const qid = await this.startQuery(query, values);
    await this.waitForSuccess(qid);
    const iter = this.lazyRowIterator(qid, query, true);
    const types = <TableStructure><unknown>((await iter.next()).value);
    const rows: Row[] = [];
    for await (const row of iter) {
      rows.push(<Row>row);
    }
    return { types, rows };
  }

  /**
   * Returns stream table object that includes query result stream and
   * queried fields types.
   */
  public async stream(
    query: string,
    values: unknown[],
    options: StreamOptions,
  ): Promise<StreamTableDataWithTypes> {
    const qid = await this.startQuery(query, values);
    await this.waitForSuccess(qid);
    const iter = this.lazyRowIterator(qid, query, true);
    const types = <TableStructure><unknown>((await iter.next()).value);
    return {
      rowStream: stream.Readable.from(iter, {
        highWaterMark: options.highWaterMark,
      }),
      types,
      release: async () => { /* canceling is missed in the iter */ },
    };
  }

  /**
   * Executes query and rerutns queried rows.
   */
  public async query<R = unknown>(
    query: string,
    values: unknown[],
    _options?: QueryOptions,
  ): Promise<R[]> {
    const qid = await this.startQuery(query, values);
    await this.waitForSuccess(qid);
    const rows: R[] = [];
    for await (const row of this.lazyRowIterator<R>(qid, query)) {
      rows.push(row);
    }
    return rows;
  }

  /**
   * Executes query and returns async generator that yields queried
   * rows.
   */
  protected async* lazyRowIterator<R extends unknown>(
    qid: AthenaQueryId,
    query: string,
    withTypes?: boolean,
  ): AsyncGenerator<R> {
    let isFirstBatch = true;
    let columnInfo: { Name: string }[] = [];
    for (
      let results: GetQueryResultsCommandOutput | undefined =
        await this.athena.getQueryResults(qid);
      results;
      results = results.NextToken
        ? (await this.athena.getQueryResults({ ...qid, NextToken: results.NextToken }))
        : undefined
    ) {
      let rows = results.ResultSet?.Rows ?? [];
      if (isFirstBatch) {
        if (withTypes) {
          yield this.mapTypes(
            <ColumnInfo[]>results.ResultSet?.ResultSetMetadata?.ColumnInfo
          ) as R;
        }
        isFirstBatch = false;
        // Athena returns the columns names in first row, skip it.
        rows = rows.slice(1);
        columnInfo = /SHOW COLUMNS/.test(query) // Fix for getColumns method
          ? [{ Name: 'column' }]
          : checkNonNullable(
            'ColumnInfo',
            results.ResultSet?.ResultSetMetadata?.ColumnInfo,
          ).map(info => ({ Name: checkNonNullable('Name', info.Name) }));
      }
      for (const row of rows) {
        const fields: Record<string, any> = {};
        columnInfo
          .forEach((c, j) => {
            const r = row.Data;
            fields[c.Name] = (
              r === null ||
              r === undefined ||
              r[j].VarCharValue === undefined
            ) ? null : r[j].VarCharValue;
          });
        yield fields as R;
      }
    }
  }

  /**
   * Save pre-aggregation data into a temp table.
   */
  public async loadPreAggregationIntoTable(
    preAggregationTableName: string,
    loadSql: string,
    params: any,
  ): Promise<any> {
    if (this.config.S3OutputLocation === undefined) {
      throw new Error('Unload is not configured. Please define CUBEJS_AWS_S3_OUTPUT_LOCATION env var ');
    }

    const qid = await this.startQuery(loadSql, params);
    await this.waitForSuccess(qid);
  }

  /**
   * Determines whether export bucket feature is configured or not.
   */
  public async isUnloadSupported() {
    return this.config.exportBucket !== undefined;
  }

  /**
   * Returns to the Cubestore an object with links to unloaded to the
   * export bucket data.
   */
  public async unload(tableName: string, options: UnloadOptions): Promise<DownloadTableCSVData> {
    if (!this.config.exportBucket) {
      throw new Error('Export bucket is not configured.');
    }
    const types = options.query
      ? await this.unloadWithSql(tableName, options)
      : await this.unloadWithTable(tableName);
    const csvFile = await this.getCsvFiles(tableName);
    return {
      exportBucketCsvEscapeSymbol: this.config.exportBucketCsvEscapeSymbol,
      csvFile,
      types,
      csvNoHeader: true,
      csvDelimiter: '^A',
    };
  }

  /**
   * Unload data from a SQL query to an export bucket.
   */
  private async unloadWithSql(
    tableName: string,
    unloadOptions: UnloadOptions,
  ): Promise<TableStructure> {
    const columns = await this.queryColumnTypes(unloadOptions.query!.sql, unloadOptions.query!.params);
    const unloadSql = `
      UNLOAD (${unloadOptions.query!.sql})
      TO '${this.config.exportBucket}/${tableName}'
      WITH (
        format = 'TEXTFILE',
        compression='GZIP'
      )`;
    const qid = await this.startQuery(unloadSql, unloadOptions.query!.params);
    await this.waitForSuccess(qid);
    await this.athena.getQueryResults(qid);
    return columns;
  }

  /**
   * Unload data from a temp table to an export bucket.
   */
  private async unloadWithTable(tableName: string): Promise<TableStructure> {
    const types = await this.tableColumnTypes(tableName);
    const columns = types.map(t => t.name).join(', ');
    const unloadSql = `
      UNLOAD (SELECT ${columns} FROM ${tableName})
      TO '${this.config.exportBucket}/${tableName}'
      WITH (
        format = 'TEXTFILE',
        compression='GZIP'
      )`;
    const qid = await this.startQuery(unloadSql, []);
    await this.waitForSuccess(qid);
    return types;
  }

  /**
   * Returns an array of queried fields meta info.
   */
  public async queryColumnTypes(sql: string, params?: unknown[]): Promise<TableStructure> {
    const unloadSql = `${sql} LIMIT 0`;
    const qid = await this.startQuery(unloadSql, params || []);
    await this.waitForSuccess(qid);
    const results = await this.athena.getQueryResults(qid);
    const columns = this.mapTypes(
      <ColumnInfo[]>results.ResultSet?.ResultSetMetadata?.ColumnInfo,
    );
    return columns;
  }

  /**
   * Converts Athena to generic types and returns an array of queried
   * fields meta info.
   */
  public mapTypes(fields: ColumnInfo[]): TableStructure {
    return fields.map((field) => ({ name: <string>field.Name, type: this.toGenericType(field.Type || 'text') }));
  }

  /**
   * Returns an array of signed URLs of the unloaded csv files.
   */
  private async getCsvFiles(tableName: string): Promise<string[]> {
    const { bucket, prefix } = AthenaDriver.splitS3Path(
      `${this.config.exportBucket}/${tableName}`
    );

    return this.extractUnloadedFilesFromS3(
      {
        credentials: this.config.credentials,
        region: this.config.region,
      },
      bucket,
      prefix.slice(1),
    );
  }

  public informationSchemaQuery() {
    if (this.schema) {
      return `${super.informationSchemaQuery()} AND columns.table_schema = '${this.schema}'`;
    }
    return super.informationSchemaQuery();
  }

  public async tablesSchema(): Promise<DatabaseStructure> {
    const tablesSchema = await super.tablesSchema();
    const viewsSchema = await this.viewsSchema(tablesSchema);

    return this.mergeSchemas([tablesSchema, viewsSchema]);
  }

  protected async startQuery(query: string, values: unknown[]): Promise<AthenaQueryId> {
    const queryString = applyParams(
      query,
      (values || []).map(s => (typeof s === 'string' ? {
        toSqlString: () => SqlString.escape(s).replace(/\\\\([_%])/g, '\\$1').replace(/\\'/g, '\'\'')
      } : s))
    );
    const request: StartQueryExecutionCommandInput = {
      QueryString: queryString,
      WorkGroup: this.config.workGroup,
      ResultConfiguration: {
        OutputLocation: this.config.S3OutputLocation
      },
      ...(this.config.catalog || this.config.database ? {
        QueryExecutionContext: {
          Catalog: this.config.catalog,
          Database: this.config.database
        }
      } : {})
    };
    const { QueryExecutionId } = await this.athena.startQueryExecution(request);
    return { QueryExecutionId: checkNonNullable('StartQueryExecution', QueryExecutionId) };
  }

  protected async checkStatus(qid: AthenaQueryId): Promise<boolean> {
    const queryExecution = await this.athena.getQueryExecution(qid);

    const status = queryExecution.QueryExecution?.Status?.State;
    if (status === 'FAILED') {
      throw new Error(queryExecution.QueryExecution?.Status?.StateChangeReason);
    }

    if (status === 'CANCELLED') {
      throw new Error('Query has been cancelled');
    }

    return status === 'SUCCEEDED';
  }

  protected async waitForSuccess(qid: AthenaQueryId): Promise<void> {
    const startedTime = Date.now();
    for (let i = 0; Date.now() - startedTime <= this.config.pollTimeout; i++) {
      if (await this.checkStatus(qid)) {
        return;
      }
      await pausePromise(
        Math.min(this.config.pollMaxInterval, 500 * i)
      );
    }
    throw new Error(
      `Athena job timeout reached ${this.config.pollTimeout}ms`
    );
  }

  protected async viewsSchema(tablesSchema: DatabaseStructure): Promise<DatabaseStructure> {
    const isView = (table: AthenaTable) => !tablesSchema[table.schema]
      || !tablesSchema[table.schema][table.name];

    const allTables = await this.getAllTables();
    const arrViewsSchema = await Promise.all(
      allTables
        .filter(isView)
        .map(table => this.getColumns(table))
    );

    return this.mergeSchemas(arrViewsSchema);
  }

  protected async getAllTables(): Promise<AthenaTable[]> {
    let allTablesQuery = `
      SELECT table_schema AS schema, table_name AS name
      FROM information_schema.tables
      WHERE tables.table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
    `;
    if (this.schema) {
      allTablesQuery = `${allTablesQuery} AND tables.table_schema = '${this.schema}'`;
    }
    const rows = await this.query(
      allTablesQuery,
      []
    );

    return rows as AthenaTable[];
  }

  protected async getColumns(table: AthenaTable): Promise<DatabaseStructure> {
    const data: { column: string }[] = await this.query(`SHOW COLUMNS IN \`${table.schema}\`.\`${table.name}\``, []);

    return {
      [table.schema]: {
        [table.name]: data.map(({ column }) => {
          const [name, type] = column.split('\t');
          return { name, type, attributes: [] };
        })
      }
    };
  }

  protected mergeSchemas(arrSchemas: DatabaseStructure[]): DatabaseStructure {
    const result: DatabaseStructure = {};

    arrSchemas.forEach(schemas => {
      Object.keys(schemas).forEach(schema => {
        Object.keys(schemas[schema]).forEach((name) => {
          if (!result[schema]) result[schema] = {};
          if (!result[schema][name]) result[schema][name] = schemas[schema][name];
        });
      });
    });

    return result;
  }

  public static normalizeS3Path(path: string): string {
    // Remove trailing /
    path = path.replace(/\/+$/, '');
    // Prepend s3:// prefix to plain bucket
    if (!path.startsWith('s3://')) {
      return `s3://${path}`;
    }
    return path;
  }

  public static splitS3Path(path: string) {
    const url = new URL(path);
    return {
      bucket: url.host,
      prefix: url.pathname
    };
  }
}
