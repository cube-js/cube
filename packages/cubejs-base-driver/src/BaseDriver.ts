/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview The `BaseDriver` and related types declaration.
 */

import * as stream from 'stream';
import type { ConnectionOptions as TLSConnectionOptions } from 'tls';

import {
  getEnv,
  keyByDataSource,
  isFilePath,
  isSslKey,
  isSslCert,
} from '@cubejs-backend/shared';
import fs from 'fs';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import { S3, GetObjectCommand, S3ClientConfig } from '@aws-sdk/client-s3';
import { Storage } from '@google-cloud/storage';
import {
  BlobServiceClient,
  StorageSharedKeyCredential,
  ContainerSASPermissions,
  SASProtocol,
  generateBlobSASQueryParameters,
} from '@azure/storage-blob';
import {
  DefaultAzureCredential,
  ClientSecretCredential,
} from '@azure/identity';

import { cancelCombinator } from './utils';
import {
  ExternalCreateTableOptions,
  DownloadQueryResultsOptions,
  DownloadQueryResultsResult,
  DownloadTableData,
  DriverInterface,
  ExternalDriverCompatibilities,
  IndexesSQL,
  isDownloadTableMemoryData,
  QueryOptions,
  Row,
  TableColumn,
  TableColumnQueryResult,
  TableQueryResult,
  TableStructure,
  DriverCapabilities,
  QuerySchemasResult,
  QueryTablesResult,
  QueryColumnsResult,
  TableMemoryData,
  PrimaryKeysQueryResult,
  ForeignKeysQueryResult,
  DatabaseStructure,
} from './driver.interface';

/**
 * @see {@link DefaultAzureCredential} constructor options
 */
export type AzureStorageClientConfig = {
  azureKey?: string,
  sasToken?: string,
  /**
   * The client ID of a Microsoft Entra app registration.
   * In case of DefaultAzureCredential flow if it is omitted
   * the Azure library will try to use the AZURE_CLIENT_ID env
   */
  clientId?: string,
  /**
   * ID of the application's Microsoft Entra tenant. Also called its directory ID.
   * In case of DefaultAzureCredential flow if it is omitted
   * the Azure library will try to use the AZURE_TENANT_ID env
   */
  tenantId?: string,
  /**
   * Azure service principal client secret.
   * Enables authentication to Microsoft Entra ID using a client secret that was generated
   * for an App Registration. More information on how to configure a client secret can be found here:
   * https://learn.microsoft.com/entra/identity-platform/quickstart-configure-app-access-web-apis#add-credentials-to-your-web-application
   * In case of DefaultAzureCredential flow if it is omitted
   * the Azure library will try to use the AZURE_CLIENT_SECRET env
   */
  clientSecret?: string,
  /**
   * The path to a file containing a Kubernetes service account token that authenticates the identity.
   * In case of DefaultAzureCredential flow if it is omitted
   * the Azure library will try to use the AZURE_FEDERATED_TOKEN_FILE env
   */
  tokenFilePath?: string,
};

export type GoogleStorageClientConfig = {
  credentials: any,
};

const sortByKeys = (unordered: any) => {
  const ordered: any = {};

  Object.keys(unordered).sort().forEach((key) => {
    ordered[key] = unordered[key];
  });

  return ordered;
};

const DbTypeToGenericType: Record<string, string> = {
  'timestamp without time zone': 'timestamp',
  'character varying': 'text',
  varchar: 'text',
  integer: 'int',
  nvarchar: 'text',
  text: 'text',
  string: 'text',
  boolean: 'boolean',
  bigint: 'bigint',
  time: 'string',
  datetime: 'timestamp',
  date: 'date',
  enum: 'text',
  'double precision': 'double',
  // PostgreSQL aliases, but maybe another databases support it
  int8: 'bigint',
  int4: 'int',
  int2: 'int',
  bool: 'boolean',
  float4: 'float',
  float8: 'double',
};

const DB_BIG_INT_MAX = BigInt('9223372036854775807');
const DB_BIG_INT_MIN = BigInt('-9223372036854775808');

const DB_INT_MAX = 2147483647;
const DB_INT_MIN = -2147483648;

// Order of keys is important here: from more specific to less specific
const DbTypeValueMatcher: Record<string, ((v: any) => boolean)> = {
  timestamp: (v) => v instanceof Date || v.toString().match(/^\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d/),
  date: (v) => v instanceof Date || v.toString().match(/^\d\d\d\d-\d\d-\d\d$/),
  int: (v) => {
    if (Number.isInteger(v)) {
      return (v <= DB_INT_MAX && v >= DB_INT_MIN);
    }

    if (v.toString().match(/^[-]?\d+$/)) {
      const value = BigInt(v.toString());

      return value <= DB_INT_MAX && value >= DB_INT_MIN;
    }

    return false;
  },
  bigint: (v) => {
    if (Number.isInteger(v)) {
      return (v <= DB_BIG_INT_MAX && v >= DB_BIG_INT_MIN);
    }

    if (v.toString().match(/^[-]?\d+$/)) {
      const value = BigInt(v.toString());

      return value <= DB_BIG_INT_MAX && value >= DB_BIG_INT_MIN;
    }

    return false;
  },
  decimal: (v) => {
    if (v instanceof Number) {
      return true;
    }

    return v.toString().match(/^[-]?\d+(\.\d+)?$/);
  },
  boolean: (v) => v === false || v === true || v.toString().toLowerCase() === 'true' || v.toString().toLowerCase() === 'false',
  string: (v) => v.length < 256,
  text: () => true
};

/**
 * Base driver class.
 */
export abstract class BaseDriver implements DriverInterface {
  private readonly testConnectionTimeoutValue: number = 10000;

  protected logger: any;

  /**
   * Class constructor.
   */
  public constructor(_options: {
    /**
     * Time to wait for a response from a connection after validation
     * request before determining it as not valid. Default - 10000 ms.
     */
    testConnectionTimeout?: number,
  } = {}) {
    this.testConnectionTimeoutValue = _options.testConnectionTimeout || 10000;
  }

  protected informationSchemaQuery() {
    return `
      SELECT columns.column_name as ${this.quoteIdentifier('column_name')},
             columns.table_name as ${this.quoteIdentifier('table_name')},
             columns.table_schema as ${this.quoteIdentifier('table_schema')},
             columns.data_type as ${this.quoteIdentifier('data_type')}
      FROM information_schema.columns
      WHERE columns.table_schema NOT IN ('pg_catalog', 'information_schema', 'mysql', 'performance_schema', 'sys', 'INFORMATION_SCHEMA')
   `;
  }

  protected getSchemasQuery() {
    return `
      SELECT table_schema as ${this.quoteIdentifier('schema_name')}
      FROM information_schema.tables
      WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'mysql', 'performance_schema', 'sys', 'INFORMATION_SCHEMA')
      GROUP BY table_schema
    `;
  }

  protected getTablesForSpecificSchemasQuery(schemasPlaceholders: string) {
    const query = `
      SELECT table_schema as ${this.quoteIdentifier('schema_name')},
            table_name as ${this.quoteIdentifier('table_name')}
      FROM information_schema.tables as columns
      WHERE table_schema IN (${schemasPlaceholders})
    `;
    return query;
  }

  protected getColumnsForSpecificTablesQuery(conditionString: string) {
    const query = `
      SELECT columns.column_name as ${this.quoteIdentifier('column_name')},
             columns.table_name as ${this.quoteIdentifier('table_name')},
             columns.table_schema as ${this.quoteIdentifier('schema_name')},
             columns.data_type as ${this.quoteIdentifier('data_type')}
      FROM information_schema.columns as columns
      WHERE ${conditionString}
    `;

    return query;
  }

  protected primaryKeysQuery(_?: string): string | null {
    return null;
  }

  protected foreignKeysQuery(_?: string): string | null {
    return null;
  }

  protected async primaryKeys(conditionString?: string, params?: string[]): Promise<PrimaryKeysQueryResult[]> {
    const query = this.primaryKeysQuery(conditionString);

    if (!query) {
      return [];
    }

    try {
      return (await this.query<PrimaryKeysQueryResult>(query, params));
    } catch (error: any) {
      if (this.logger) {
        this.logger('Primary Keys Query failed. Primary Keys will be defined by heuristics', {
          error: (error.stack || error).toString()
        });
      }
      return [];
    }
  }

  protected async foreignKeys(conditionString?: string, params?: string[]): Promise<ForeignKeysQueryResult[]> {
    const query = this.foreignKeysQuery(conditionString);

    if (!query) {
      return [];
    }

    try {
      return (await this.query<ForeignKeysQueryResult>(query, params));
    } catch (error: any) {
      if (this.logger) {
        this.logger('Foreign Keys Query failed. Joins will be defined by heuristics', {
          error: (error.stack || error).toString()
        });
      }
      return [];
    }
  }

  protected getColumnNameForSchemaName() {
    return 'columns.table_schema';
  }

  protected getColumnNameForTableName() {
    return 'columns.table_name';
  }

  protected getSslOptions(dataSource: string): TLSConnectionOptions | undefined {
    if (
      getEnv('dbSsl', { dataSource }) ||
      getEnv('dbSslRejectUnauthorized', { dataSource })
    ) {
      const sslOptions = [{
        name: 'ca',
        canBeFile: true,
        envKey: keyByDataSource('CUBEJS_DB_SSL_CA', dataSource),
        validate: isSslCert,
      }, {
        name: 'cert',
        canBeFile: true,
        envKey: keyByDataSource('CUBEJS_DB_SSL_CERT', dataSource),
        validate: isSslCert,
      }, {
        name: 'key',
        canBeFile: true,
        envKey: keyByDataSource('CUBEJS_DB_SSL_KEY', dataSource),
        validate: isSslKey,
      }, {
        name: 'ciphers',
        envKey: keyByDataSource('CUBEJS_DB_SSL_CIPHERS', dataSource),
      }, {
        name: 'passphrase',
        envKey: keyByDataSource('CUBEJS_DB_SSL_PASSPHRASE', dataSource),
      }, {
        name: 'servername',
        envKey: keyByDataSource('CUBEJS_DB_SSL_SERVERNAME', dataSource),
      }];

      const ssl: TLSConnectionOptions = sslOptions.reduce(
        (agg, { name, envKey, canBeFile, validate }) => {
          const value = process.env[envKey];
          if (value) {
            if (validate && validate(value)) {
              return {
                ...agg,
                ...{ [name]: value }
              };
            }

            if (canBeFile && isFilePath(value)) {
              if (!fs.existsSync(value)) {
                throw new Error(
                  `Unable to find ${name} from path: "${value}"`,
                );
              }

              const file = fs.readFileSync(value, 'utf8');
              if (validate(file)) {
                return {
                  ...agg,
                  ...{ [name]: file }
                };
              }

              throw new Error(
                `Content of the file from ${envKey} is not a valid SSL ${name}.`,
              );
            }

            throw new Error(
              `${envKey} is not a valid SSL ${name}. If it's a path, please specify it correctly`,
            );
          }

          return agg;
        },
        {}
      );

      ssl.rejectUnauthorized = getEnv('dbSslRejectUnauthorized', { dataSource });

      return ssl;
    }

    return undefined;
  }

  abstract testConnection(): Promise<void>;

  abstract query<R = unknown>(_query: string, _values?: unknown[], _options?: QueryOptions): Promise<R[]>;

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  public async streamQuery(sql: string, values: string[]): Promise<stream.Readable> {
    throw new TypeError('Driver\'s .streamQuery() method is not implemented yet.');
  }

  public async downloadQueryResults(query: string, values: unknown[], _options: DownloadQueryResultsOptions): Promise<DownloadQueryResultsResult> {
    const rows = await this.query<Row>(query, values);
    if (rows.length === 0) {
      throw new Error(
        'Unable to detect column types for pre-aggregation on empty values in readOnly mode.'
      );
    }

    const fields = Object.keys(rows[0]);

    const types = fields.map(field => ({
      name: field,
      type: Object.keys(DbTypeValueMatcher).find(
        type => !rows.filter(row => field in row).find(row => !DbTypeValueMatcher[type](row[field])) &&
          rows.find(row => field in row)
      ) || 'text'
    }));

    return {
      rows,
      types,
    };
  }

  public readOnly() {
    return false;
  }

  protected informationColumnsSchemaReducer(result: any, i: any): DatabaseStructure {
    let schema = (result[i.table_schema] || {});
    const columns = (schema[i.table_name] || []);

    columns.push({
      name: i.column_name,
      type: i.data_type,
      attributes: i.key_type ? ['primaryKey'] : []
    });

    columns.sort();
    schema[i.table_name] = columns;
    schema = sortByKeys(schema);
    result[i.table_schema] = schema;

    return sortByKeys(result);
  }

  public tablesSchema(): Promise<DatabaseStructure> {
    const query = this.informationSchemaQuery();

    return this.query(query, []).then(data => data.reduce<DatabaseStructure>(this.informationColumnsSchemaReducer, {}));
  }

  // Extended version of tablesSchema containing primary and foreign keys
  public async tablesSchemaV2() {
    const tablesSchema = await this.tablesSchema();
    const [primaryKeys, foreignKeys] = await Promise.all([this.primaryKeys(), this.foreignKeys()]);

    for (const pk of primaryKeys) {
      if (Array.isArray(tablesSchema?.[pk.table_schema]?.[pk.table_name])) {
        tablesSchema[pk.table_schema][pk.table_name] = tablesSchema[pk.table_schema][pk.table_name].map((it: any) => {
          if (it.name === pk.column_name) {
            it.attributes = ['primaryKey'];
          }
          return it;
        });
      }
    }

    for (const foreignKey of foreignKeys) {
      if (Array.isArray(tablesSchema?.[foreignKey.table_schema]?.[foreignKey.table_name])) {
        tablesSchema[foreignKey.table_schema][foreignKey.table_name] = tablesSchema[foreignKey.table_schema][foreignKey.table_name].map((it: any) => {
          if (it.name === foreignKey.column_name) {
            it.foreign_keys = [...(it.foreign_keys || []), {
              target_table: foreignKey.target_table,
              target_column: foreignKey.target_column
            }];
          }
          return it;
        });
      }
    }

    return tablesSchema;
  }

  public async createSchemaIfNotExists(schemaName: string): Promise<void> {
    const schemas = await this.query(
      `SELECT schema_name FROM information_schema.schemata WHERE schema_name = ${this.param(0)}`,
      [schemaName]
    );

    if (schemas.length === 0) {
      await this.query(`CREATE SCHEMA IF NOT EXISTS ${schemaName}`);
    }
  }

  public getSchemas(): Promise<QuerySchemasResult[]> {
    const query = this.getSchemasQuery();
    return this.query<QuerySchemasResult>(query);
  }

  public getTablesForSpecificSchemas(schemas: QuerySchemasResult[]): Promise<QueryTablesResult[]> {
    const schemasPlaceholders = schemas.map((_, idx) => this.param(idx)).join(', ');
    const schemaNames = schemas.map(s => s.schema_name);

    const query = this.getTablesForSpecificSchemasQuery(schemasPlaceholders);
    return this.query<QueryTablesResult>(query, schemaNames);
  }

  public async getColumnsForSpecificTables(tables: QueryTablesResult[]): Promise<QueryColumnsResult[]> {
    const groupedBySchema: Record<string, string[]> = {};
    tables.forEach((t) => {
      if (!groupedBySchema[t.schema_name]) {
        groupedBySchema[t.schema_name] = [];
      }
      groupedBySchema[t.schema_name].push(t.table_name);
    });

    const conditions: string[] = [];
    const parameters: any[] = [];

    for (const [schema, tableNames] of Object.entries(groupedBySchema)) {
      const schemaPlaceholder = this.param(parameters.length);
      parameters.push(schema);

      const tablePlaceholders = tableNames.map((_, idx) => this.param(parameters.length + idx)).join(', ');
      parameters.push(...tableNames);

      conditions.push(`(${this.getColumnNameForSchemaName()} = ${schemaPlaceholder} AND ${this.getColumnNameForTableName()} IN (${tablePlaceholders}))`);
    }

    const conditionString = conditions.join(' OR ');

    const query = this.getColumnsForSpecificTablesQuery(conditionString);

    const [primaryKeys, foreignKeys] = await Promise.all([
      this.primaryKeys(conditionString, parameters),
      this.foreignKeys(conditionString, parameters)
    ]);

    const columns = await this.query<QueryColumnsResult>(query, parameters);

    for (const column of columns) {
      if (primaryKeys.some(pk => pk.table_schema === column.schema_name && pk.table_name === column.table_name && pk.column_name === column.column_name)) {
        column.attributes = ['primaryKey'];
      }

      column.foreign_keys = foreignKeys.filter(fk => fk.table_schema === column.schema_name && fk.table_name === column.table_name && fk.column_name === column.column_name).map(fk => ({
        target_table: fk.target_table,
        target_column: fk.target_column
      }));
    }

    return columns;
  }

  public getTablesQuery(schemaName: string) {
    return this.query<TableQueryResult>(
      `SELECT table_name FROM information_schema.tables WHERE table_schema = ${this.param(0)}`,
      [schemaName]
    );
  }

  public loadPreAggregationIntoTable(_preAggregationTableName: string, loadSql: string, params: any, options: any) {
    return this.query(loadSql, params, options);
  }

  public dropTable(tableName: string, options?: QueryOptions): Promise<unknown> {
    return this.query(`DROP TABLE ${tableName}`, [], options);
  }

  public param(_paramIndex: number): string {
    return '?';
  }

  public testConnectionTimeout() {
    return this.testConnectionTimeoutValue;
  }

  public async downloadTable(table: string, _options: ExternalDriverCompatibilities): Promise<TableMemoryData> {
    return { rows: await this.query(`SELECT * FROM ${table}`) };
  }

  public async uploadTable(table: string, columns: TableStructure, tableData: DownloadTableData) {
    return this.uploadTableWithIndexes(table, columns, tableData, [], null, [], {});
  }

  public async uploadTableWithIndexes(table: string, columns: TableStructure, tableData: DownloadTableData, indexesSql: IndexesSQL, _uniqueKeyColumns: string[] | null, _queryTracingObj: any, _externalOptions: ExternalCreateTableOptions) {
    if (!isDownloadTableMemoryData(tableData)) {
      throw new Error(`${this.constructor} driver supports only rows upload`);
    }

    await this.createTable(table, columns);
    try {
      if (isDownloadTableMemoryData(tableData)) {
        for (let i = 0; i < tableData.rows.length; i++) {
          await this.query(
            `INSERT INTO ${table}
          (${columns.map(c => this.quoteIdentifier(c.name)).join(', ')})
          VALUES (${columns.map((c, paramIndex) => this.param(paramIndex)).join(', ')})`,
            columns.map(c => this.toColumnValue(tableData.rows[i][c.name] as string, c.type))
          );
        }
        for (let i = 0; i < indexesSql.length; i++) {
          const [query, params] = indexesSql[i].sql;
          await this.query(query, params);
        }
      }
    } catch (e) {
      await this.dropTable(table);
      throw e;
    }
  }

  protected toColumnValue(value: string, _genericType: string): string | boolean {
    return value;
  }

  public async tableColumnTypes(table: string): Promise<TableStructure> {
    const [schema, name] = table.split('.');

    const columns = await this.query<TableColumnQueryResult>(
      `SELECT columns.column_name as ${this.quoteIdentifier('column_name')},
             columns.table_name as ${this.quoteIdentifier('table_name')},
             columns.table_schema as ${this.quoteIdentifier('table_schema')},
             columns.data_type  as ${this.quoteIdentifier('data_type')}
      FROM information_schema.columns
      WHERE table_name = ${this.param(0)} AND table_schema = ${this.param(1)}
      ${getEnv('fetchColumnsByOrdinalPosition') ? 'ORDER BY columns.ordinal_position' : ''}`,
      [name, schema]
    );

    return columns.map(c => ({ name: c.column_name, type: this.toGenericType(c.data_type) }));
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  public async queryColumnTypes(sql: string, params: unknown[]): Promise<{ name: any; type: string; }[]> {
    return [];
  }

  // This is only for use in tests
  public async createTableRaw(query: string): Promise<void> {
    await this.query(query);
  }

  public async createTable(quotedTableName: string, columns: TableColumn[]): Promise<void> {
    const createTableSql = this.createTableSql(quotedTableName, columns);
    await this.query(createTableSql, []).catch(e => {
      e.message = `Error during create table: ${createTableSql}: ${e.message}`;
      throw e;
    });
  }

  protected createTableSql(quotedTableName: string, columns: TableColumn[]) {
    const columnNames = columns.map(c => `${this.quoteIdentifier(c.name)} ${this.fromGenericType(c.type)}`);
    return `CREATE TABLE ${quotedTableName} (${columnNames.join(', ')})`;
  }

  protected toGenericType(columnType: string): string {
    return DbTypeToGenericType[columnType.toLowerCase()] || columnType;
  }

  protected fromGenericType(columnType: string): string {
    return columnType;
  }

  protected quoteIdentifier(identifier: string): string {
    return `"${identifier}"`;
  }

  protected cancelCombinator(fn: any) {
    return cancelCombinator(fn);
  }

  public setLogger(logger: any) {
    this.logger = logger;
  }

  protected reportQueryUsage(usage: any, queryOptions: any) {
    if (this.logger) {
      this.logger('SQL Query Usage', {
        ...usage,
        ...queryOptions
      });
    }
  }

  protected databasePoolError(error: any) {
    if (this.logger) {
      this.logger('Database Pool Error', {
        error: (error.stack || error).toString()
      });
    }
  }

  public async release() {
    // override, if it's needed
  }

  public capabilities(): DriverCapabilities {
    return {};
  }

  public nowTimestamp() {
    return Date.now();
  }

  public wrapQueryWithLimit(query: { query: string, limit: number}) {
    query.query = `SELECT * FROM (${query.query}) AS t LIMIT ${query.limit}`;
  }

  /**
   * Returns an array of signed AWS S3 URLs of the unloaded csv files.
   */
  protected async extractUnloadedFilesFromS3(
    clientOptions: S3ClientConfig,
    bucketName: string,
    prefix: string
  ): Promise<string[]> {
    const storage = new S3(clientOptions);
    // It looks that different driver configurations use different formats
    // for the bucket - some expect only names, some - full url-like names.
    // So we unify this.
    bucketName = bucketName.replace(/^[a-zA-Z]+:\/\//, '');

    const list = await storage.listObjectsV2({
      Bucket: bucketName,
      Prefix: prefix,
    });
    if (list) {
      if (!list.Contents) {
        return [];
      } else {
        const csvFile = await Promise.all(
          list.Contents.map(async (file) => {
            const command = new GetObjectCommand({
              Bucket: bucketName,
              Key: file.Key,
            });
            return getSignedUrl(storage, command, { expiresIn: 3600 });
          })
        );
        return csvFile;
      }
    }

    throw new Error('Unable to retrieve list of files from S3 storage after unloading.');
  }

  /**
   * Returns an array of signed GCS URLs of the unloaded csv files.
   */
  protected async extractFilesFromGCS(
    gcsConfig: GoogleStorageClientConfig,
    bucketName: string,
    tableName: string
  ): Promise<string[]> {
    const storage = new Storage({
      credentials: gcsConfig.credentials,
      projectId: gcsConfig.credentials.project_id
    });
    const bucket = storage.bucket(bucketName);
    const [files] = await bucket.getFiles({ prefix: `${tableName}/` });
    if (files.length) {
      const csvFile = await Promise.all(files.map(async (file) => {
        const [url] = await file.getSignedUrl({
          action: 'read',
          expires: new Date(new Date().getTime() + 60 * 60 * 1000)
        });
        return url;
      }));
      return csvFile;
    } else {
      return [];
    }
  }

  protected async extractFilesFromAzure(
    azureConfig: AzureStorageClientConfig,
    bucketName: string,
    tableName: string
  ): Promise<string[]> {
    const splitter = bucketName.includes('blob.core') ? '.blob.core.windows.net/' : '.dfs.core.windows.net/';
    const parts = bucketName.split(splitter);
    const account = parts[0];
    const container = parts[1].split('/')[0];
    let credential: StorageSharedKeyCredential | ClientSecretCredential | DefaultAzureCredential;
    let blobServiceClient: BlobServiceClient;
    let getSas;

    if (azureConfig.azureKey) {
      credential = new StorageSharedKeyCredential(account, azureConfig.azureKey);
      getSas = async (name: string, startsOn: Date, expiresOn: Date) => generateBlobSASQueryParameters(
        {
          containerName: container,
          blobName: name,
          permissions: ContainerSASPermissions.parse('r'),
          startsOn,
          expiresOn,
          protocol: SASProtocol.Https,
          version: '2020-08-04',
        },
        credential as StorageSharedKeyCredential
      ).toString();
    } else if (azureConfig.clientSecret && azureConfig.tenantId && azureConfig.clientId) {
      credential = new ClientSecretCredential(
        azureConfig.tenantId,
        azureConfig.clientId,
        azureConfig.clientSecret,
      );
      getSas = async (name: string, startsOn: Date, expiresOn: Date) => {
        const userDelegationKey = await blobServiceClient.getUserDelegationKey(startsOn, expiresOn);
        return generateBlobSASQueryParameters(
          {
            containerName: container,
            blobName: name,
            permissions: ContainerSASPermissions.parse('r'),
            startsOn,
            expiresOn,
            protocol: SASProtocol.Https,
            version: '2020-08-04',
          },
          userDelegationKey,
          account
        ).toString();
      };
    } else {
      const opts = {
        tenantId: azureConfig.tenantId,
        clientId: azureConfig.clientId,
        tokenFilePath: azureConfig.tokenFilePath,
      };
      credential = new DefaultAzureCredential(opts);
      getSas = async (name: string, startsOn: Date, expiresOn: Date) => {
        // getUserDelegationKey works only for authorization with Microsoft Entra ID
        const userDelegationKey = await blobServiceClient.getUserDelegationKey(startsOn, expiresOn);
        return generateBlobSASQueryParameters(
          {
            containerName: container,
            blobName: name,
            permissions: ContainerSASPermissions.parse('r'),
            startsOn,
            expiresOn,
            protocol: SASProtocol.Https,
            version: '2020-08-04',
          },
          userDelegationKey,
          account,
        ).toString();
      };
    }

    const url = `https://${account}.blob.core.windows.net`;
    blobServiceClient = azureConfig.sasToken ?
      new BlobServiceClient(`${url}?${azureConfig.sasToken}`) :
      new BlobServiceClient(url, credential);

    const csvFiles: string[] = [];
    const containerClient = blobServiceClient.getContainerClient(container);
    const blobsList = containerClient.listBlobsFlat({ prefix: `${tableName}` });
    for await (const blob of blobsList) {
      if (blob.name && (blob.name.endsWith('.csv.gz') || blob.name.endsWith('.csv'))) {
        const starts = new Date();
        const expires = new Date(starts.valueOf() + 1000 * 60 * 60);
        const sas = await getSas(blob.name, starts, expires);
        csvFiles.push(`${url}/${container}/${blob.name}?${sas}`);
      }
    }

    return csvFiles;
  }
}
