/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview Native Trino REST driver. Talks to `/v1/statement` directly
 * (no `presto-client`) with progressive backoff while waiting for the first
 * rows, then configurable drain polling.
 */

import fetch, { RequestInit } from 'node-fetch';
import * as http from 'http';
import * as https from 'https';
import { Transform, TransformCallback } from 'stream';
import {
  BaseDriver,
  DownloadQueryResultsOptions,
  DownloadQueryResultsResult,
  DriverCapabilities,
  DriverInterface,
  StreamOptions,
  StreamTableData,
  TableStructure,
  UnloadOptions,
} from '@cubejs-backend/base-driver';
import {
  getEnv,
  assertDataSource,
  formatAnsi,
} from '@cubejs-backend/shared';
import { TrinoQuery } from '@cubejs-backend/schema-compiler';

import {
  TrinoDriverConfiguration,
  PollBackoffConfig,
  TrinoQueryResponse,
  TrinoColumn,
} from './types';

const TRINO_HEADERS = {
  USER: 'X-Trino-User',
  SOURCE: 'X-Trino-Source',
  CATALOG: 'X-Trino-Catalog',
  SCHEMA: 'X-Trino-Schema',
  TIME_ZONE: 'X-Trino-Time-Zone',
  SESSION: 'X-Trino-Session',
};

const DEFAULT_POLL_BACKOFF: Required<PollBackoffConfig> = {
  initialInterval: 50,
  incrementStep: 50,
  maxInterval: 500,
  triesBeforeIncrement: 10,
};

const DEFAULT_MAX_TRANSIENT_RETRIES = 5;

const SUPPORTED_BUCKET_TYPES = ['gcs', 's3'];

const TRANSIENT_HTTP_STATUSES = [502, 503, 504];

export class TrinoDriver extends BaseDriver implements DriverInterface {
  public static getDefaultConcurrency() {
    return 2;
  }

  public static dialectClass() {
    return TrinoQuery;
  }

  protected readonly config: TrinoDriverConfiguration;

  protected readonly catalog: string | undefined;

  protected readonly pollBackoff: Required<PollBackoffConfig>;

  protected readonly drainInterval: number;

  protected readonly maxTransientRetries: number;

  protected readonly useSelectTestConnection: boolean;

  private readonly baseUrl: string;

  private readonly httpAgent: http.Agent;

  private readonly httpsAgent: https.Agent;

  public constructor(config: TrinoDriverConfiguration = {}) {
    super();

    const dataSource = config.dataSource || assertDataSource('default');
    const preAggregations = config.preAggregations || false;

    const dbUser = getEnv('dbUser', { dataSource, preAggregations });
    const dbPassword = getEnv('dbPass', { dataSource, preAggregations });
    const authToken = getEnv('prestoAuthToken', { dataSource, preAggregations });

    if (authToken && dbPassword) {
      throw new Error('Both user/password and auth token are set. Please remove password or token.');
    }

    this.useSelectTestConnection = config.useSelectTestConnection ??
      getEnv('dbUseSelectTestConnection', { dataSource, preAggregations });

    this.config = {
      host: getEnv('dbHost', { dataSource, preAggregations }),
      port: getEnv('dbPort', { dataSource, preAggregations }),
      catalog:
        getEnv('prestoCatalog', { dataSource, preAggregations }) ||
        getEnv('dbCatalog', { dataSource, preAggregations }),
      schema:
        getEnv('dbName', { dataSource, preAggregations }) ||
        getEnv('dbSchema', { dataSource, preAggregations }),
      user: dbUser,
      // Match presto-client: X-Trino-Source defaults to `nodejs-client`.
      source: getEnv('trinoSource', { dataSource, preAggregations }) || 'nodejs-client',
      ...(authToken ? { custom_auth: `Bearer ${authToken}` } : {}),
      ...(dbPassword ? { basic_auth: { user: dbUser, password: dbPassword } } : {}),
      ssl: this.getSslOptions(dataSource, preAggregations),
      bucketType: getEnv('dbExportBucketType', { supported: SUPPORTED_BUCKET_TYPES, dataSource, preAggregations }),
      exportBucket: getEnv('dbExportBucket', { dataSource, preAggregations }),
      accessKeyId: getEnv('dbExportBucketAwsKey', { dataSource, preAggregations }),
      secretAccessKey: getEnv('dbExportBucketAwsSecret', { dataSource, preAggregations }),
      exportBucketRegion: getEnv('dbExportBucketAwsRegion', { dataSource, preAggregations }),
      credentials: getEnv('dbExportGCSCredentials', { dataSource, preAggregations }),
      queryTimeout: getEnv('dbQueryTimeout', { dataSource, preAggregations }),
      ...config,
    };

    if (this.config.custom_auth && this.config.basic_auth) {
      throw new Error('Please do not specify basic_auth and custom_auth at the same time.');
    }

    if (!this.config.source) {
      this.config.source = 'nodejs-client';
    }

    // X-Trino-User must match the authenticated principal unless impersonation
    // is explicit. driverFactory often passes only basic_auth (as the stock
    // integration tests do); defaulting the session user to `cube` then fails
    // with PERMISSION_DENIED on current Trino.
    if (!this.config.user && this.config.basic_auth?.user) {
      this.config.user = this.config.basic_auth.user;
    }

    this.catalog = this.config.catalog;

    const envPollBackoff: Required<PollBackoffConfig> = {
      initialInterval: getEnv('trinoPollInitialInterval', { dataSource, preAggregations }),
      incrementStep: getEnv('trinoPollIncrementStep', { dataSource, preAggregations }),
      maxInterval: getEnv('trinoPollMaxInterval', { dataSource, preAggregations }),
      triesBeforeIncrement: getEnv('trinoPollTriesBeforeIncrement', { dataSource, preAggregations }),
    };

    const useLegacyCheckInterval = config.checkInterval != null && config.pollBackoff == null;
    if (useLegacyCheckInterval) {
      this.pollBackoff = {
        initialInterval: config.checkInterval as number,
        incrementStep: 0,
        maxInterval: config.checkInterval as number,
        triesBeforeIncrement: 1,
      };
      this.drainInterval = config.drainInterval ?? (config.checkInterval as number);
    } else {
      this.pollBackoff = {
        ...DEFAULT_POLL_BACKOFF,
        ...envPollBackoff,
        ...this.config.pollBackoff,
      };
      this.drainInterval = this.config.drainInterval ?? 0;
    }

    this.maxTransientRetries = this.config.maxTransientRetries ?? DEFAULT_MAX_TRANSIENT_RETRIES;
    this.validatePollingConfig();

    const protocol = this.config.ssl ? 'https' : 'http';
    this.baseUrl = `${protocol}://${this.config.host}:${this.config.port}`;

    const keepAlive = this.config.keepAlive !== false;
    const sslOptions = (this.config.ssl && typeof this.config.ssl === 'object') ? this.config.ssl : {};
    this.httpsAgent = new https.Agent({ ...sslOptions, keepAlive });
    this.httpAgent = new http.Agent({ keepAlive });
  }

  private static assertNonNegativeInt(name: string, value: number) {
    if (typeof value !== 'number' || !Number.isInteger(value) || value < 0) {
      throw new Error(`${name} must be a non-negative integer, got ${value}`);
    }
  }

  private validatePollingConfig() {
    TrinoDriver.assertNonNegativeInt('pollBackoff.initialInterval', this.pollBackoff.initialInterval);
    TrinoDriver.assertNonNegativeInt('pollBackoff.incrementStep', this.pollBackoff.incrementStep);
    TrinoDriver.assertNonNegativeInt('pollBackoff.maxInterval', this.pollBackoff.maxInterval);
    TrinoDriver.assertNonNegativeInt('pollBackoff.triesBeforeIncrement', this.pollBackoff.triesBeforeIncrement);
    if (this.pollBackoff.triesBeforeIncrement < 1) {
      throw new Error('pollBackoff.triesBeforeIncrement must be >= 1');
    }
    if (this.pollBackoff.initialInterval > this.pollBackoff.maxInterval) {
      throw new Error(
        `pollBackoff.initialInterval (${this.pollBackoff.initialInterval}) must be <= maxInterval (${this.pollBackoff.maxInterval})`
      );
    }
    TrinoDriver.assertNonNegativeInt('drainInterval', this.drainInterval);
    TrinoDriver.assertNonNegativeInt('maxTransientRetries', this.maxTransientRetries);
  }

  private getAuthHeader(): string | undefined {
    if (this.config.custom_auth) {
      return this.config.custom_auth;
    }
    if (this.config.basic_auth) {
      const { user, password } = this.config.basic_auth;
      return `Basic ${Buffer.from(`${user}:${password}`).toString('base64')}`;
    }
    return undefined;
  }

  private getBaseHeaders(): Record<string, string> {
    const headers: Record<string, string> = {
      [TRINO_HEADERS.USER]: this.config.user || 'cube',
      [TRINO_HEADERS.SOURCE]: this.config.source || 'nodejs-client',
      ...this.config.headers,
    };

    if (this.config.catalog) {
      headers[TRINO_HEADERS.CATALOG] = this.config.catalog;
    }
    if (this.config.schema) {
      headers[TRINO_HEADERS.SCHEMA] = this.config.schema;
    }
    if (this.config.timezone) {
      headers[TRINO_HEADERS.TIME_ZONE] = this.config.timezone;
    }

    const auth = this.getAuthHeader();
    if (auth) {
      headers.Authorization = auth;
    }

    return headers;
  }

  private getSessionHeader(): string | undefined {
    const parts: string[] = [];
    if (this.config.queryTimeout) {
      parts.push(`query_max_run_time=${this.config.queryTimeout}s`);
    }
    if (typeof this.config.session === 'string' && this.config.session) {
      parts.push(this.config.session);
    } else if (this.config.session && typeof this.config.session === 'object') {
      Object.entries(this.config.session).forEach(([key, value]) => {
        parts.push(`${key}=${value}`);
      });
    }
    return parts.length ? parts.join(',') : undefined;
  }

  private getFetchOptions(url?: string): RequestInit {
    const isHttps = url ? url.startsWith('https:') : Boolean(this.config.ssl);
    return { agent: isHttps ? this.httpsAgent : this.httpAgent };
  }

  public async testConnection(): Promise<void> {
    if (this.useSelectTestConnection) {
      await this.testConnectionViaSelect();
      return;
    }

    const headers = this.getBaseHeaders();
    const response = await fetch(`${this.baseUrl}/v1/info`, {
      method: 'GET',
      headers,
      ...this.getFetchOptions(),
    });

    if (!response.ok) {
      const text = await response.text();
      throw new Error(`Connection test failed: ${response.status} ${response.statusText} - ${text}`);
    }
  }

  protected async testConnectionViaSelect() {
    await this.queryPromised('SELECT 1', false);
  }

  public query(query: string, values: unknown[]): Promise<any[]> {
    return <Promise<any[]>> this.queryPromised(this.prepareQueryWithParams(query, values), false);
  }

  protected prepareQueryWithParams(query: string, values: unknown[]) {
    return formatAnsi(query, values || []);
  }

  /**
   * Two-phase polling:
   * - Wait phase: no rows yet → backoff before each nextUri poll.
   * - Drain phase: after the first non-empty data page → `drainInterval`
   *   (default 0, i.e. poll as fast as Trino produces pages).
   */
  public queryPromised(query: string, streaming: boolean): Promise<any[] | StreamTableData> {
    if (streaming) {
      return this.executeStreaming(query);
    }
    return this.executeBuffered(query);
  }

  private static hasRowData(response: TrinoQueryResponse): boolean {
    return Array.isArray(response.data) && response.data.length > 0;
  }

  private createPollBackoffState(): { intervalMs: number; holdsAtCurrent: number } {
    return {
      intervalMs: this.pollBackoff.initialInterval,
      holdsAtCurrent: 0,
    };
  }

  private async maybeBackoffBeforePoll(
    hasReceivedData: boolean,
    state: { intervalMs: number; holdsAtCurrent: number }
  ): Promise<void> {
    if (hasReceivedData) {
      if (this.drainInterval > 0) {
        await this.sleep(this.drainInterval);
      }
      return;
    }
    await this.sleep(state.intervalMs);
    state.holdsAtCurrent += 1;
    if (state.holdsAtCurrent >= this.pollBackoff.triesBeforeIncrement) {
      state.holdsAtCurrent = 0;
      state.intervalMs = Math.min(
        state.intervalMs + this.pollBackoff.incrementStep,
        this.pollBackoff.maxInterval
      );
    }
  }

  private async executeStatement(query: string): Promise<TrinoQueryResponse> {
    const headers: Record<string, string> = {
      ...this.getBaseHeaders(),
      'Content-Type': 'text/plain',
    };

    const session = this.getSessionHeader();
    if (session) {
      headers[TRINO_HEADERS.SESSION] = session;
    }

    const response = await fetch(`${this.baseUrl}/v1/statement`, {
      method: 'POST',
      headers,
      body: query,
      ...this.getFetchOptions(),
    });

    if (!response.ok) {
      const text = await response.text();
      throw new Error(`Trino statement POST failed (${response.status}): ${text}`);
    }

    return response.json() as Promise<TrinoQueryResponse>;
  }

  private async fetchNextUri(uri: string): Promise<TrinoQueryResponse> {
    const headers = this.getBaseHeaders();

    for (let attempt = 0; attempt <= this.maxTransientRetries; attempt++) {
      const response = await fetch(uri, {
        method: 'GET',
        headers,
        ...this.getFetchOptions(uri),
      });

      if (TRANSIENT_HTTP_STATUSES.includes(response.status)) {
        if (attempt === this.maxTransientRetries) {
          throw new Error(
            `Trino poll failed after ${this.maxTransientRetries} retries (last status: ${response.status})`
          );
        }
        const baseDelay = 50 * (2 ** attempt);
        await this.sleep(baseDelay + Math.floor(Math.random() * baseDelay));
      } else if (!response.ok) {
        const text = await response.text();
        throw new Error(`Trino poll failed (${response.status}): ${text}`);
      } else {
        return response.json() as Promise<TrinoQueryResponse>;
      }
    }

    throw new Error('Trino poll failed: exhausted retries');
  }

  private async executeBuffered(query: string): Promise<any[]> {
    let response = await this.executeStatement(query);
    this.checkError(response);

    let fullData: any[] = [];
    let columns: TrinoColumn[] | undefined;
    let hasReceivedData = TrinoDriver.hasRowData(response);
    const pollBackoffState = this.createPollBackoffState();

    while (response.nextUri) {
      if (response.columns && !columns) {
        columns = response.columns;
      }

      if (TrinoDriver.hasRowData(response)) {
        hasReceivedData = true;
        const normalData = this.normalizeResultOverColumns(response.data!, columns || []);
        fullData = fullData.concat(normalData);
      }

      await this.maybeBackoffBeforePoll(hasReceivedData, pollBackoffState);
      response = await this.fetchNextUri(response.nextUri);
      this.checkError(response);
    }

    if (response.columns && !columns) {
      columns = response.columns;
    }
    if (TrinoDriver.hasRowData(response)) {
      const normalData = this.normalizeResultOverColumns(response.data!, columns || []);
      fullData = fullData.concat(normalData);
    }

    return fullData;
  }

  private async executeStreaming(query: string): Promise<StreamTableData> {
    const rowStream = new Transform({
      writableObjectMode: true,
      readableObjectMode: true,
      transform(obj: any, _encoding: string, callback: TransformCallback) {
        callback(null, obj);
      },
    });

    let response = await this.executeStatement(query);
    this.checkError(response);

    let columns: TrinoColumn[] | undefined;
    let columnsResolved = false;
    let hasReceivedData = TrinoDriver.hasRowData(response);
    const pollBackoffState = this.createPollBackoffState();

    const resultPromise = new Promise<StreamTableData>((resolve, reject) => {
      const drain = async () => {
        try {
          while (response.nextUri) {
            if (response.columns && !columns) {
              columns = response.columns;
            }

            if (!columnsResolved && columns) {
              columnsResolved = true;
              resolve({ rowStream: rowStream as unknown as NodeJS.ReadableStream, types: columns as TableStructure });
            }

            if (TrinoDriver.hasRowData(response)) {
              hasReceivedData = true;
              const normalData = this.normalizeResultOverColumns(response.data!, columns || []);
              for (const obj of normalData) {
                rowStream.write(obj);
              }
            }

            await this.maybeBackoffBeforePoll(hasReceivedData, pollBackoffState);
            response = await this.fetchNextUri(response.nextUri);
            this.checkError(response);
          }

          if (response.columns && !columns) {
            columns = response.columns;
          }
          if (!columnsResolved) {
            columnsResolved = true;
            resolve({ rowStream: rowStream as unknown as NodeJS.ReadableStream, types: (columns || []) as TableStructure });
          }
          if (TrinoDriver.hasRowData(response)) {
            const normalData = this.normalizeResultOverColumns(response.data!, columns || []);
            for (const obj of normalData) {
              rowStream.write(obj);
            }
          }

          rowStream.end();
        } catch (error) {
          const err = error instanceof Error ? error : new Error(String(error));
          rowStream.destroy(err);
          if (!columnsResolved) {
            reject(err);
          }
        }
      };

      drain().catch(() => {
        // Errors are handled inside drain(); this prevents unhandled rejection
      });
    });

    return resultPromise;
  }

  private checkError(response: TrinoQueryResponse): void {
    if (response.error) {
      const msg = response.error.message || 'Unknown Trino error';
      const errorName = response.error.errorName ? ` (${response.error.errorName})` : '';
      throw new Error(`Trino query error${errorName}: ${msg}`);
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => {
      setTimeout(resolve, ms);
    });
  }

  public normalizeResultOverColumns(data: any[][], columns: TrinoColumn[]): Record<string, any>[] {
    const columnNames = (columns || []).map((c) => c.name);
    return (data || []).map((row) => {
      const obj: Record<string, any> = {};
      for (let i = 0; i < columnNames.length; i++) {
        obj[columnNames[i]] = row[i];
      }
      return obj;
    });
  }

  protected informationSchemaQuery() {
    const catalogPrefix = this.catalog ? `${this.catalog}.` : '';
    const schemaFilter = this.config.schema ? ` AND columns.table_schema = '${this.config.schema}'` : '';

    return `
      SELECT columns.column_name as ${this.quoteIdentifier('column_name')},
             columns.table_name as ${this.quoteIdentifier('table_name')},
             columns.table_schema as ${this.quoteIdentifier('table_schema')},
             columns.data_type as ${this.quoteIdentifier('data_type')}
      FROM ${catalogPrefix}information_schema.columns
      WHERE columns.table_schema NOT IN ('pg_catalog', 'information_schema', 'mysql', 'performance_schema', 'sys', 'INFORMATION_SCHEMA')${schemaFilter}
   `;
  }

  protected getSchemasQuery() {
    const catalogPrefix = this.catalog ? `${this.catalog}.` : '';

    return `
      SELECT table_schema as ${this.quoteIdentifier('schema_name')}
      FROM ${catalogPrefix}information_schema.tables
      WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'mysql', 'performance_schema', 'sys', 'INFORMATION_SCHEMA')
      GROUP BY table_schema
    `;
  }

  protected getTablesForSpecificSchemasQuery(schemasPlaceholders: string) {
    const catalogPrefix = this.catalog ? `${this.catalog}.` : '';

    return `
      SELECT table_schema as ${this.quoteIdentifier('schema_name')},
            table_name as ${this.quoteIdentifier('table_name')}
      FROM ${catalogPrefix}information_schema.tables as columns
      WHERE table_schema IN (${schemasPlaceholders})
    `;
  }

  protected getColumnsForSpecificTablesQuery(conditionString: string) {
    const catalogPrefix = this.catalog ? `${this.catalog}.` : '';

    return `
      SELECT columns.column_name as ${this.quoteIdentifier('column_name')},
             columns.table_name as ${this.quoteIdentifier('table_name')},
             columns.table_schema as ${this.quoteIdentifier('schema_name')},
             columns.data_type as ${this.quoteIdentifier('data_type')}
      FROM ${catalogPrefix}information_schema.columns as columns
      WHERE ${conditionString}
    `;
  }

  public downloadQueryResults(
    query: string,
    values: unknown[],
    options: DownloadQueryResultsOptions
  ): Promise<DownloadQueryResultsResult> {
    if (options.streamImport) {
      return <Promise<DownloadQueryResultsResult>> this.stream(query, values, options);
    }
    return super.downloadQueryResults(query, values, options);
  }

  public stream(query: string, values: unknown[], _options: StreamOptions): Promise<StreamTableData> {
    const queryWithParams = this.prepareQueryWithParams(query, values);
    return <Promise<StreamTableData>> this.queryPromised(queryWithParams, true);
  }

  public capabilities(): DriverCapabilities {
    return {
      unloadWithoutTempTable: true,
    };
  }

  public async createSchemaIfNotExists(schemaName: string) {
    await this.query(`CREATE SCHEMA IF NOT EXISTS ${this.config.catalog}.${schemaName}`, []);
  }

  public async isUnloadSupported() {
    return this.config.exportBucket !== undefined;
  }

  public async unload(tableName: string, options: UnloadOptions) {
    if (!this.config.exportBucket) {
      throw new Error('Export bucket is not configured.');
    }

    if (!SUPPORTED_BUCKET_TYPES.includes(this.config.bucketType as string)) {
      throw new Error(`Unsupported export bucket type: ${this.config.bucketType}`);
    }

    const types = options.query
      ? await this.unloadWithSql(tableName, options.query.sql, options.query.params)
      : await this.unloadWithTable(tableName);

    const csvFile = await this.getCsvFiles(tableName);

    return {
      exportBucketCsvEscapeSymbol: this.config.exportBucketCsvEscapeSymbol,
      csvFile,
      types,
      csvNoHeader: true,
    };
  }

  public async queryColumnTypes(sql: string, params: unknown[]): Promise<{ name: string; type: string }[]> {
    const response = await this.stream(`${sql} LIMIT 0`, params || [], { highWaterMark: 1 });
    const result = [];
    for (const column of response.types || []) {
      result.push({ name: column.name, type: this.toGenericType(column.type) });
    }
    return result;
  }

  private splitTableFullName(tableFullName: string) {
    const [schema, tableName] = tableFullName.split('.');
    return { schema, tableName };
  }

  private generateTableColumnsForExport(types: { name: string; type: string }[]) {
    return types.map((c) => `CAST(${c.name} AS varchar) ${c.name}`).join(', ');
  }

  private async unloadWithSql(tableFullName: string, sql: string, params: any[]) {
    return this.unloadGeneric({
      tableFullName,
      typeSql: sql,
      typeParams: params,
      fromSql: sql,
      fromParams: params,
    });
  }

  private async unloadWithTable(tableFullName: string) {
    return this.unloadGeneric({
      tableFullName,
      typeSql: `SELECT * FROM ${tableFullName}`,
      typeParams: [],
      fromSql: tableFullName,
      fromParams: [],
    });
  }

  private async unloadGeneric(params: {
    tableFullName: string;
    typeSql: string;
    typeParams: any[];
    fromSql: string;
    fromParams: any[];
  }) {
    if (!this.config.exportBucket) {
      throw new Error('Export bucket is not configured.');
    }

    const { bucketType, exportBucket } = this.config;
    const types = await this.queryColumnTypes(params.typeSql, params.typeParams);

    const { schema, tableName } = this.splitTableFullName(params.tableFullName);
    const tableWithCatalogAndSchema = `${this.config.catalog}.${schema}.${tableName}`;

    const protocol = {
      gcs: 'gs',
      s3: this.config.exportBucketS3AdvancedFS ? 's3a' : 's3',
    }[bucketType || 'gcs'];

    const externalLocation = `${protocol}://${exportBucket}/${schema}/${tableName}`;
    const withParams = `( external_location = '${externalLocation}', format = 'CSV')`;
    const select = `SELECT ${this.generateTableColumnsForExport(types)} FROM (${params.fromSql})`;
    const createTableQuery = `CREATE TABLE ${tableWithCatalogAndSchema} WITH ${withParams} AS (${select})`;

    try {
      await this.query(createTableQuery, params.fromParams);
    } finally {
      await this.query(`DROP TABLE IF EXISTS ${tableWithCatalogAndSchema}`, []);
    }

    return types;
  }

  private async getCsvFiles(tableFullName: string): Promise<string[]> {
    if (!this.config.exportBucket) {
      throw new Error('Export bucket is not configured.');
    }
    const { bucketType, exportBucket } = this.config;
    const { schema, tableName } = this.splitTableFullName(tableFullName);

    switch (bucketType) {
      case 'gcs':
        return this.extractFilesFromGCS(
          { credentials: this.config.credentials },
          exportBucket,
          `${schema}/${tableName}`
        );
      case 's3':
        return this.extractUnloadedFilesFromS3(
          {
            credentials: this.config.accessKeyId && this.config.secretAccessKey
              ? { accessKeyId: this.config.accessKeyId, secretAccessKey: this.config.secretAccessKey }
              : undefined,
            region: this.config.exportBucketRegion,
          },
          exportBucket,
          `${schema}/${tableName}`
        );
      default:
        throw new Error(`Unsupported export bucket type: ${bucketType}`);
    }
  }

  public async release() {
    this.httpAgent.destroy();
    this.httpsAgent.destroy();
  }
}
