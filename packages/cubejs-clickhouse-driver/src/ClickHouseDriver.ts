/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview The `ClickHouseDriver` and related types declaration.
 */

import {
  getEnv,
  assertDataSource,
} from '@cubejs-backend/shared';
import {
  BaseDriver,
  DownloadQueryResultsOptions,
  DownloadQueryResultsResult,
  DownloadTableCSVData,
  DriverCapabilities,
  DriverInterface,
  QueryOptions,
  QuerySchemasResult,
  StreamOptions,
  StreamTableDataWithTypes,
  TableColumn,
  TableQueryResult,
  TableStructure,
  UnloadOptions,
} from '@cubejs-backend/base-driver';

import { Readable } from 'node:stream';
import { ClickHouseClient, createClient } from '@clickhouse/client';
import type { ClickHouseSettings, ResponseJSON } from '@clickhouse/client';
import { v4 as uuidv4 } from 'uuid';
import sqlstring from 'sqlstring';

import { transformRow, transformStreamRow } from './HydrationStream';

const ClickhouseTypeToGeneric: Record<string, string> = {
  enum: 'text',
  string: 'text',
  datetime: 'timestamp',
  datetime64: 'timestamp',
  date: 'date',
  decimal: 'decimal',
  // integers
  int8: 'int',
  int16: 'int',
  int32: 'int',
  int64: 'bigint',
  // unsigned int
  uint8: 'int',
  uint16: 'int',
  uint32: 'int',
  uint64: 'bigint',
  // floats
  float32: 'float',
  float64: 'double',
  // We don't support enums
  enum8: 'text',
  enum16: 'text',
};

export interface ClickHouseDriverOptions {
  host?: string,
  port?: string,
  username?: string,
  password?: string,
  protocol?: string,
  database?: string,
  readOnly?: boolean,
  /**
   * Timeout in milliseconds for requests to ClickHouse.
   * Default is 10 minutes
   */
  requestTimeout?: number,

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
}

interface ClickhouseDriverExportRequiredAWS {
  bucketType: 's3',
  bucketName: string,
  region: string,
}

interface ClickhouseDriverExportKeySecretAWS extends ClickhouseDriverExportRequiredAWS {
  keyId: string,
  secretKey: string,
}

interface ClickhouseDriverExportAWS extends ClickhouseDriverExportKeySecretAWS {
}

type ClickHouseDriverConfig = {
  url: string,
  username: string,
  password: string,
  readOnly: boolean,
  database: string,
  requestTimeout: number,
  exportBucket: ClickhouseDriverExportAWS | null,
  clickhouseSettings: ClickHouseSettings,
};

export class ClickHouseDriver extends BaseDriver implements DriverInterface {
  /**
   * Returns default concurrency value.
   */
  public static getDefaultConcurrency(): number {
    return 5;
  }

  // ClickHouseClient has internal pool of several sockets, no need for generic-pool
  protected readonly client: ClickHouseClient;

  protected readonly readOnlyMode: boolean;

  protected readonly config: ClickHouseDriverConfig;

  /**
   * Class constructor.
   */
  public constructor(
    config: ClickHouseDriverOptions = {},
  ) {
    super({
      testConnectionTimeout: config.testConnectionTimeout,
    });

    const dataSource = config.dataSource ?? assertDataSource('default');
    const host = config.host ?? getEnv('dbHost', { dataSource });
    const port = config.port ?? getEnv('dbPort', { dataSource }) ?? 8123;
    const protocol = config.protocol ?? getEnv('dbSsl', { dataSource }) ? 'https:' : 'http:';
    const url = `${protocol}//${host}:${port}`;

    const username = config.username ?? getEnv('dbUser', { dataSource });
    const password = config.password ?? getEnv('dbPass', { dataSource });
    const database = config.database ?? (getEnv('dbName', { dataSource }) as string) ?? 'default';

    // TODO this is a bit inconsistent with readOnly
    this.readOnlyMode =
      getEnv('clickhouseReadOnly', { dataSource }) === 'true';

    // Expect that getEnv('dbQueryTimeout') will always return a value
    const requestTimeoutEnv: number = getEnv('dbQueryTimeout', { dataSource }) * 1000;
    const requestTimeout = config.requestTimeout ?? requestTimeoutEnv;

    this.config = {
      url,
      username,
      password,
      database,
      exportBucket: this.getExportBucket(dataSource),
      readOnly: !!config.readOnly,
      requestTimeout,
      clickhouseSettings: {
        // If ClickHouse user's permissions are restricted with "readonly = 1",
        // change settings queries are not allowed. Thus, "join_use_nulls" setting
        // can not be changed
        ...(this.readOnlyMode ? {} : { join_use_nulls: 1 }),
      },
    };

    const maxPoolSize = config.maxPoolSize ?? getEnv('dbMaxPoolSize', { dataSource }) ?? 8;

    this.client = this.createClient(maxPoolSize);
  }

  protected withCancel<T>(fn: (con: ClickHouseClient, queryId: string, signal: AbortSignal) => Promise<T>): Promise<T> {
    const queryId = uuidv4();

    const abortController = new AbortController();
    const { signal } = abortController;

    const promise = (async () => {
      const pingResult = await this.client.ping();
      if (!pingResult.success) {
        // TODO replace string formatting with proper cause
        // pingResult.error can be AggregateError when ClickHouse hostname resolves to multiple addresses
        let errorMessage = pingResult.error.toString();
        if (pingResult.error instanceof AggregateError) {
          errorMessage = `Aggregate error: ${pingResult.error.message}; errors: ${pingResult.error.errors.join('; ')}`;
        }
        throw new Error(`Connection check failed: ${errorMessage}`);
      }
      signal.throwIfAborted();
      // Queries sent by `fn` can hit a timeout error, would _not_ get killed, and continue running in ClickHouse
      // TODO should we kill those as well?
      const result = await fn(this.client, queryId, signal);
      signal.throwIfAborted();
      return result;
    })();
    (promise as any).cancel = async () => {
      abortController.abort();
      // Use separate client for kill query, usual pool may be busy
      const killClient = this.createClient(1);
      try {
        await killClient.command({
          query: `KILL QUERY WHERE query_id = '${queryId}'`,
        });
      } finally {
        await killClient.close();
      }
    };

    return promise;
  }

  protected createClient(maxPoolSize: number): ClickHouseClient {
    return createClient({
      url: this.config.url,
      username: this.config.username,
      password: this.config.password,
      database: this.config.database,
      clickhouse_settings: this.config.clickhouseSettings,
      request_timeout: this.config.requestTimeout,
      max_open_connections: maxPoolSize,
    });
  }

  public async testConnection() {
    await this.query('SELECT 1', []);
  }

  public readOnly() {
    return (this.config.readOnly != null || this.readOnlyMode) ?
      (!!this.config.readOnly || this.readOnlyMode) :
      true;
  }

  public async query<R = unknown>(query: string, values: unknown[]): Promise<R[]> {
    const response = await this.queryResponse(query, values);
    return this.normaliseResponse(response);
  }

  protected queryResponse(query: string, values: unknown[]): Promise<ResponseJSON<Record<string, unknown>>> {
    const formattedQuery = sqlstring.format(query, values);

    return this.withCancel(async (connection, queryId, signal) => {
      try {
        const format = 'JSON';

        const resultSet = await connection.query({
          query: formattedQuery,
          query_id: queryId,
          format,
          clickhouse_settings: this.config.clickhouseSettings,
          abort_signal: signal,
        });

        if (resultSet.response_headers['x-clickhouse-format'] !== format) {
          throw new Error(`Unexpected x-clickhouse-format in response: expected ${format}, received ${resultSet.response_headers['x-clickhouse-format']}`);
        }

        // We used format JSON, so we expect each row to be Record with column names as keys
        const results = await resultSet.json<Record<string, unknown>>();
        return results;
      } catch (e) {
        // TODO replace string formatting with proper cause
        throw new Error(`Query failed: ${e}; query id: ${queryId}`);
      }
    });
  }

  protected normaliseResponse<R = unknown>(res: ResponseJSON<Record<string, unknown>>): Array<R> {
    if (res.data) {
      const meta = (res.meta ?? []).reduce<Record<string, { name: string; type: string; }>>(
        (state, element) => ({ [element.name]: element, ...state }),
        {}
      );

      // TODO maybe use row-based format here as well?
      res.data.forEach((row) => {
        transformRow(row, meta);
      });
    }
    return res.data as Array<R>;
  }

  public async release() {
    await this.client.close();
  }

  public informationSchemaQuery() {
    return `
      SELECT name as column_name,
             table as table_name,
             database as table_schema,
             type as data_type
        FROM system.columns
       WHERE database = '${this.config.database}'
    `;
  }

  protected override getTablesForSpecificSchemasQuery(schemasPlaceholders: string) {
    const query = `
      SELECT database as schema_name,
            name as table_name
      FROM system.tables
      WHERE database IN (${schemasPlaceholders})
    `;
    return query;
  }

  protected override getColumnsForSpecificTablesQuery(conditionString: string) {
    const query = `
      SELECT name as ${this.quoteIdentifier('column_name')},
             table as ${this.quoteIdentifier('table_name')},
             database as ${this.quoteIdentifier('schema_name')},
             type as ${this.quoteIdentifier('data_type')}
      FROM system.columns
      WHERE ${conditionString}
    `;
    return query;
  }

  protected override getColumnNameForSchemaName() {
    return 'database';
  }

  protected override getColumnNameForTableName() {
    return 'table';
  }

  public override async getSchemas(): Promise<QuerySchemasResult[]> {
    return [{ schema_name: this.config.database }];
  }

  public async stream(
    query: string,
    values: unknown[],
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    { highWaterMark }: StreamOptions
  ): Promise<StreamTableDataWithTypes> {
    // Use separate client for this long-living query
    const client = this.createClient(1);
    const queryId = uuidv4();

    try {
      const formattedQuery = sqlstring.format(query, values);

      const format = 'JSONCompactEachRowWithNamesAndTypes';

      const resultSet = await client.query({
        query: formattedQuery,
        query_id: queryId,
        format,
        clickhouse_settings: this.config.clickhouseSettings,
      });

      if (resultSet.response_headers['x-clickhouse-format'] !== format) {
        throw new Error(`Unexpected x-clickhouse-format in response: expected ${format}, received ${resultSet.response_headers['x-clickhouse-format']}`);
      }

      // Array<unknown> is okay, because we use fixed JSONCompactEachRowWithNamesAndTypes format
      // And each row after first two will look like this: [42, "hello", [0,1]]
      // https://clickhouse.com/docs/en/interfaces/formats#jsoncompacteachrowwithnamesandtypes
      const resultSetStream = resultSet.stream<Array<unknown>>();

      const allRowsIter = (async function* allRowsIter() {
        for await (const rowsBatch of resultSetStream) {
          for (const row of rowsBatch) {
            yield row.json();
          }
        }
      }());

      const first = await allRowsIter.next();
      if (first.done) {
        throw new Error('Unexpected stream end before row with names');
      }
      // JSONCompactEachRowWithNamesAndTypes: expect first row to be column names as string
      const names = first.value as Array<string>;

      const second = await allRowsIter.next();
      if (second.done) {
        throw new Error('Unexpected stream end before row with types');
      }
      // JSONCompactEachRowWithNamesAndTypes: expect first row to be column names as string
      const types = second.value as Array<string>;

      if (names.length !== types.length) {
        throw new Error(`Unexpected names and types length mismatch; names ${names.length} vs types ${types.length}`);
      }

      const dataRowsIter = (async function* () {
        for await (const row of allRowsIter) {
          yield transformStreamRow(row, names, types);
        }
      }());
      const rowStream = Readable.from(dataRowsIter);

      return {
        rowStream,
        types: names.map((name, idx) => {
          const type = types[idx];
          return {
            name,
            type: this.toGenericType(type),
          };
        }),
        release: async () => {
          await client.close();
        }
      };
    } catch (e) {
      await client.close();
      // TODO replace string formatting with proper cause
      throw new Error(`Stream query failed: ${e}; query id: ${queryId}`);
    }
  }

  public async downloadQueryResults(
    query: string,
    values: unknown[],
    options: DownloadQueryResultsOptions
  ): Promise<DownloadQueryResultsResult> {
    if ((options ?? {}).streamImport) {
      return this.stream(query, values, options);
    }

    const response = await this.queryResponse(query, values);

    return {
      rows: this.normaliseResponse(response),
      types: (response.meta ?? []).map((field) => ({
        name: field.name,
        type: this.toGenericType(field.type),
      })),
    };
  }

  public toGenericType(columnType: string) {
    if (columnType.toLowerCase() in ClickhouseTypeToGeneric) {
      return ClickhouseTypeToGeneric[columnType.toLowerCase()];
    }

    /**
     * Example of types:
     *
     * Int64
     * Nullable(Int64) / Nullable(String)
     * Nullable(DateTime('UTC'))
     */
    if (columnType.includes('(')) {
      const types = columnType.toLowerCase().match(/([a-z0-9']+)/g);
      if (types) {
        for (const type of types) {
          if (type in ClickhouseTypeToGeneric) {
            return ClickhouseTypeToGeneric[type];
          }
        }
      }
    }

    return super.toGenericType(columnType);
  }

  public async createSchemaIfNotExists(schemaName: string): Promise<void> {
    await this.command(`CREATE DATABASE IF NOT EXISTS ${schemaName}`);
  }

  public getTablesQuery(schemaName: string): Promise<TableQueryResult[]> {
    return this.query('SELECT name as table_name FROM system.tables WHERE database = ?', [schemaName]);
  }

  public override async dropTable(tableName: string, _options?: QueryOptions): Promise<void> {
    await this.command(`DROP TABLE ${tableName}`);
  }

  protected getExportBucket(
    dataSource: string,
  ): ClickhouseDriverExportAWS | null {
    const supportedBucketTypes = ['s3'];

    const requiredExportBucket: ClickhouseDriverExportRequiredAWS = {
      bucketType: getEnv('dbExportBucketType', {
        supported: supportedBucketTypes,
        dataSource,
      }),
      bucketName: getEnv('dbExportBucket', { dataSource }),
      region: getEnv('dbExportBucketAwsRegion', { dataSource }),
    };

    const exportBucket: ClickhouseDriverExportAWS = {
      ...requiredExportBucket,
      keyId: getEnv('dbExportBucketAwsKey', { dataSource }),
      secretKey: getEnv('dbExportBucketAwsSecret', { dataSource }),
    };

    if (exportBucket.bucketType) {
      if (!supportedBucketTypes.includes(exportBucket.bucketType)) {
        throw new Error(
          `Unsupported EXPORT_BUCKET_TYPE, supported: ${supportedBucketTypes.join(',')}`
        );
      }

      // Make sure the required keys are set
      const emptyRequiredKeys = Object.keys(requiredExportBucket)
        .filter((key: string) => requiredExportBucket[<keyof ClickhouseDriverExportRequiredAWS>key] === undefined);
      if (emptyRequiredKeys.length) {
        throw new Error(
          `Unsupported configuration exportBucket, some configuration keys are empty: ${emptyRequiredKeys.join(',')}`
        );
      }

      return exportBucket;
    }

    return null;
  }

  public async isUnloadSupported() {
    if (this.config.exportBucket) {
      return true;
    }

    return false;
  }

  /**
   * Returns an array of queried fields meta info.
   */
  public async queryColumnTypes(sql: string, params: unknown[]): Promise<TableStructure> {
    // For DESCRIBE we expect that each row would have special structure
    // See https://clickhouse.com/docs/en/sql-reference/statements/describe-table
    // TODO complete this type
    type DescribeRow = {
      name: string,
      type: string
    };
    const columns = await this.query<DescribeRow>(`DESCRIBE ${sql}`, params);
    if (!columns) {
      throw new Error('Unable to describe table');
    }

    return columns.map((column) => ({
      name: column.name,
      type: this.toGenericType(column.type),
    }));
  }

  // This is only for use in tests
  public override async createTableRaw(query: string): Promise<void> {
    await this.command(query);
  }

  public override async createTable(quotedTableName: string, columns: TableColumn[]) {
    const createTableSql = this.createTableSql(quotedTableName, columns);
    try {
      await this.command(createTableSql);
    } catch (e) {
      // TODO replace string formatting with proper cause
      throw new Error(`Create table failed: ${e}`);
    }
  }

  /**
   * We use unloadWithoutTempTable strategy
   */
  public async unload(_tableName: string, options: UnloadOptions): Promise<DownloadTableCSVData> {
    if (!options.query?.sql) {
      throw new Error('Query must be defined in options');
    }

    return this.unloadFromQuery(
      options.query?.sql,
      options.query?.params,
      options
    );
  }

  public async unloadFromQuery(sql: string, params: unknown[], options: UnloadOptions): Promise<DownloadTableCSVData> {
    if (!this.config.exportBucket) {
      throw new Error('Unload is not configured');
    }

    const types = await this.queryColumnTypes(`(${sql})`, params);
    const exportPrefix = uuidv4();

    const formattedQuery = sqlstring.format(`
      INSERT INTO FUNCTION
         s3(
             'https://${this.config.exportBucket.bucketName}.s3.${this.config.exportBucket.region}.amazonaws.com/${exportPrefix}/export.csv.gz',
             '${this.config.exportBucket.keyId}',
             '${this.config.exportBucket.secretKey}',
             'CSV'
          )
      ${sql}
    `, params);

    await this.command(formattedQuery);

    const csvFile = await this.extractUnloadedFilesFromS3(
      {
        credentials: {
          accessKeyId: this.config.exportBucket.keyId,
          secretAccessKey: this.config.exportBucket.secretKey,
        },
        region: this.config.exportBucket.region,
      },
      this.config.exportBucket.bucketName,
      exportPrefix,
    );

    return {
      csvFile,
      types,
      csvNoHeader: true,
      // Can be controlled via SET format_csv_delimiter
      csvDelimiter: ','
    };
  }

  public capabilities(): DriverCapabilities {
    return {
      unloadWithoutTempTable: true,
      incrementalSchemaLoading: true,
    };
  }

  // This is not part of a driver interface, and marked public only for testing
  public async command(query: string): Promise<void> {
    await this.withCancel(async (connection, queryId, signal) => {
      await connection.command({
        query,
        query_id: queryId,
        abort_signal: signal,
      });
    });
  }

  // This is not part of a driver interface, and marked public only for testing
  public async insert(table: string, values: Array<Array<unknown>>): Promise<void> {
    await this.withCancel(async (connection, queryId, signal) => {
      await connection.insert({
        table,
        values,
        format: 'JSONCompactEachRow',
        query_id: queryId,
        abort_signal: signal,
      });
    });
  }
}
