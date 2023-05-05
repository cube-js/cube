/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview The `SnowflakeDriver` and related types declaration.
 */

import {
  getEnv,
  assertDataSource,
} from '@cubejs-backend/shared';
import snowflake, { Column, Connection, Statement } from 'snowflake-sdk';
import {
  BaseDriver,
  DownloadTableCSVData,
  DriverInterface,
  GenericDataBaseType,
  TableStructure,
  UnloadOptions,
  StreamOptions,
  StreamTableDataWithTypes,
  DownloadTableMemoryData,
  DownloadQueryResultsResult,
  DownloadQueryResultsOptions,
  DriverCapabilities,
} from '@cubejs-backend/base-driver';
import { formatToTimeZone } from 'date-fns-timezone';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import { S3, GetObjectCommand } from '@aws-sdk/client-s3';
import { Storage } from '@google-cloud/storage';
import { HydrationMap, HydrationStream } from './HydrationStream';

// eslint-disable-next-line import/order
const util = require('snowflake-sdk/lib/util');

// TODO Remove when https://github.com/snowflakedb/snowflake-connector-nodejs/pull/158 is resolved
util.construct_hostname = (region: any, account: any) => {
  let host;
  if (region === 'us-west-2') {
    region = null;
  }
  if (account.indexOf('.') > 0) {
    account = account.substring(0, account.indexOf('.'));
  }
  if (region) {
    host = `${account}.${region}.snowflakecomputing.com`;
  } else {
    host = `${account}.snowflakecomputing.com`;
  }
  return host;
};

type HydrationConfiguration = {
  types: string[], toValue: (column: Column) => ((value: any) => any) | null
};

type UnloadResponse = {
  // eslint-disable-next-line camelcase
  rows_unloaded: string
};

// It's not possible to declare own map converters by passing config to snowflake-sdk
const hydrators: HydrationConfiguration[] = [
  {
    types: ['fixed', 'real'],
    toValue: (column) => {
      if (column.isNullable()) {
        return (value) => {
          // We use numbers as strings by fetchAsString
          if (value === 'NULL') {
            return null;
          }

          return value;
        };
      }

      // Nothing to fix, let's skip this field
      return null;
    },
  },
  {
    // The TIMESTAMP_* variation associated with TIMESTAMP, default to TIMESTAMP_NTZ
    types: [
      'date',
      // TIMESTAMP_LTZ internally stores UTC time with a specified precision.
      'timestamp_ltz',
      // TIMESTAMP_NTZ internally stores “wallclock” time with a specified precision.
      // All operations are performed without taking any time zone into account.
      'timestamp_ntz',
      // TIMESTAMP_TZ internally stores UTC time together with an associated time zone offset.
      // When a time zone is not provided, the session time zone offset is used.
      'timestamp_tz'
    ],
    toValue: () => (value) => {
      if (!value) {
        return null;
      }

      return formatToTimeZone(
        value,
        'YYYY-MM-DDTHH:mm:ss.SSS',
        {
          timeZone: 'UTC'
        }
      );
    },
  }
];

const SnowflakeToGenericType: Record<string, GenericDataBaseType> = {
  // It's a limitation for now, because anyway we dont work with JSON objects in Cube Store.
  object: 'HLL_SNOWFLAKE',
  number: 'decimal',
  timestamp_ntz: 'timestamp'
};

// User can create own stage to pass permission restrictions.
interface SnowflakeDriverExportAWS {
  bucketType: 's3',
  bucketName: string,
  keyId: string,
  secretKey: string,
  region: string,
}

interface SnowflakeDriverExportGCS {
  bucketType: 'gcs',
  integrationName: string,
  bucketName: string,
  credentials: object,
}

export type SnowflakeDriverExportBucket = SnowflakeDriverExportAWS | SnowflakeDriverExportGCS;

interface SnowflakeDriverOptions {
  account: string,
  username: string,
  password: string,
  region?: string,
  warehouse?: string,
  role?: string,
  clientSessionKeepAlive?: boolean,
  database?: string,
  authenticator?: string,
  privateKeyPath?: string,
  privateKeyPass?: string,
  privateKey?: string,
  resultPrefetch?: number,
  exportBucket?: SnowflakeDriverExportBucket,
  executionTimeout?: number,
  application: string,
  readOnly?: boolean,

  /**
   * The export bucket CSV file escape symbol.
   */
  exportBucketCsvEscapeSymbol?: string,
}

/**
 * Snowflake driver class.
 *
 * Attention:
 * Snowflake is using UPPER_CASE for table, schema and column names
 * Similar to data in response, column_name will be COLUMN_NAME
 */
export class SnowflakeDriver extends BaseDriver implements DriverInterface {
  /**
   * Returns default concurrency value.
   */
  public static getDefaultConcurrency(): number {
    return 5;
  }

  public static driverEnvVariables() {
    // TODO (buntarb): check how this method can/must be used with split
    // names by the data source.
    return [
      'CUBEJS_DB_NAME',
      'CUBEJS_DB_USER',
      'CUBEJS_DB_PASS',
      'CUBEJS_DB_SNOWFLAKE_ACCOUNT',
      'CUBEJS_DB_SNOWFLAKE_REGION',
      'CUBEJS_DB_SNOWFLAKE_WAREHOUSE',
      'CUBEJS_DB_SNOWFLAKE_ROLE',
      'CUBEJS_DB_SNOWFLAKE_CLIENT_SESSION_KEEP_ALIVE',
      'CUBEJS_DB_SNOWFLAKE_AUTHENTICATOR',
      'CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY_PATH',
      'CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY_PASS'
    ];
  }

  protected connection: Promise<Connection> | null = null;

  protected readonly config: SnowflakeDriverOptions;

  /**
   * Class constructor.
   */
  public constructor(
    config: Partial<SnowflakeDriverOptions> & {
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
    } = {}
  ) {
    super({
      testConnectionTimeout: config.testConnectionTimeout,
    });

    const dataSource =
      config.dataSource ||
      assertDataSource('default');

    let privateKey = getEnv('snowflakePrivateKey', { dataSource });
    if (privateKey && !privateKey.endsWith('\n')) {
      privateKey += '\n';
    }

    this.config = {
      readOnly: false,
      account: getEnv('snowflakeAccount', { dataSource }),
      region: getEnv('snowflakeRegion', { dataSource }),
      warehouse: getEnv('snowflakeWarehouse', { dataSource }),
      role: getEnv('snowflakeRole', { dataSource }),
      clientSessionKeepAlive: getEnv('snowflakeSessionKeepAlive', { dataSource }),
      database: getEnv('dbName', { dataSource }),
      username: getEnv('dbUser', { dataSource }),
      password: getEnv('dbPass', { dataSource }),
      authenticator: getEnv('snowflakeAuthenticator', { dataSource }),
      privateKeyPath: getEnv('snowflakePrivateKeyPath', { dataSource }),
      privateKeyPass: getEnv('snowflakePrivateKeyPass', { dataSource }),
      privateKey,
      exportBucket: this.getExportBucket(dataSource),
      resultPrefetch: 1,
      executionTimeout: getEnv('dbQueryTimeout', { dataSource }),
      exportBucketCsvEscapeSymbol: getEnv('dbExportBucketCsvEscapeSymbol', { dataSource }),
      application: 'CubeDev_Cube',
      ...config
    };
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
    return { unloadWithoutTempTable: true };
  }

  protected createExportBucket(
    dataSource: string,
    bucketType: string,
  ): SnowflakeDriverExportBucket {
    if (bucketType === 's3') {
      return {
        bucketType,
        bucketName: getEnv('dbExportBucket', { dataSource }),
        keyId: getEnv('dbExportBucketAwsKey', { dataSource }),
        secretKey: getEnv('dbExportBucketAwsSecret', { dataSource }),
        region: getEnv('dbExportBucketAwsRegion', { dataSource }),
      };
    }

    if (bucketType === 'gcs') {
      return {
        bucketType,
        bucketName: getEnv('dbExportBucket', { dataSource }),
        integrationName: getEnv('dbExportIntegration', { dataSource }),
        credentials: getEnv('dbExportGCSCredentials', { dataSource }),
      };
    }

    throw new Error(
      `Unsupported EXPORT_BUCKET_TYPE, supported: ${['s3', 'gcs'].join(',')}`
    );
  }

  /**
   * @todo Move to BaseDriver in the future?
   */
  protected getExportBucket(
    dataSource: string,
  ): SnowflakeDriverExportBucket | undefined {
    const bucketType = getEnv('dbExportBucketType', {
      dataSource,
      supported: ['s3', 'gcs'],
    });
    if (bucketType) {
      const exportBucket = this.createExportBucket(
        dataSource,
        bucketType,
      );

      const emptyKeys = Object.keys(exportBucket)
        .filter((key: string) => exportBucket[<keyof SnowflakeDriverExportBucket>key] === undefined);
      if (emptyKeys.length) {
        throw new Error(
          `Unsupported configuration exportBucket, some configuration keys are empty: ${emptyKeys.join(',')}`
        );
      }

      return exportBucket;
    }

    return undefined;
  }

  /**
   * Test driver's connection.
   */
  public async testConnection() {
    const connection = snowflake.createConnection(this.config);
    await new Promise(
      (resolve, reject) => connection.connect((err, conn) => (err ? reject(err) : resolve(conn)))
    );
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const { password, ...rest } = this.config;
    if (!connection.isUp()) {
      throw new Error(`Can't connect to the Snowflake instance: ${JSON.stringify(rest)}`);
    }
    await new Promise(
      (resolve, reject) => connection.destroy((err, conn) => (err ? reject(err) : resolve(conn)))
    );
  }

  /**
   * Initializes and resolves connection to the Snowflake.
   */
  protected async initConnection() {
    try {
      const connection = snowflake.createConnection(this.config);
      await new Promise(
        (resolve, reject) => connection.connect((err, conn) => (err ? reject(err) : resolve(conn)))
      );

      await this.execute(connection, 'ALTER SESSION SET TIMEZONE = \'UTC\'', [], false);
      await this.execute(connection, `ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = ${this.config.executionTimeout}`, [], false);

      return connection;
    } catch (e) {
      this.connection = null;

      throw e;
    }
  }

  /**
   * Resolves connection to the Snowflake.
   */
  protected async getConnection(): Promise<Connection> {
    if (this.connection) {
      const connection = await this.connection;

      // Return a connection if not in a fatal state.
      if (connection.isUp()) {
        return connection;
      }
    }

    // eslint-disable-next-line no-return-assign
    return this.connection = this.initConnection();
  }

  /**
   * Executes query and rerutns queried rows.
   */
  public async query<R = unknown>(query: string, values?: unknown[]): Promise<R> {
    return this.getConnection().then((connection) => this.execute<R>(connection, query, values));
  }

  /**
   * Determines whether export bucket feature is configured or not.
   */
  public async isUnloadSupported() {
    if (!this.config.exportBucket) {
      return false;
    }
    return true;
  }

  /**
   * Returns to the Cubestore an object with links to unloaded to the
   * export bucket data.
   */
  public async unload(
    tableName: string,
    options: UnloadOptions,
  ): Promise<DownloadTableCSVData> {
    if (!this.config.exportBucket) {
      throw new Error('Export bucket is not configured.');
    }
    const types = options.query
      ? await this.unloadWithSql(tableName, options)
      : await this.unloadWithTable(tableName, options);
    const csvFile = await this.getCsvFiles(tableName);
    return {
      exportBucketCsvEscapeSymbol: this.config.exportBucketCsvEscapeSymbol,
      csvFile,
      types,
      csvNoHeader: true,
    };
  }

  /**
   * Unload data from a SQL query to an export bucket.
   */
  private async unloadWithSql(
    tableName: string,
    options: UnloadOptions,
  ): Promise<TableStructure> {
    if (!options.query) {
      throw new Error('Unload query is missed.');
    } else {
      const types = await this.queryColumnTypes(options.query.sql);
      const connection = await this.getConnection();
      const { bucketType, bucketName } =
        <SnowflakeDriverExportBucket> this.config.exportBucket;
      const unloadSql = `
        COPY INTO '${bucketType}://${bucketName}/${tableName}/'
        FROM (${options.query.sql})
        ${this.exportOptionsClause(options)}`;
      const result = await this.execute<UnloadResponse[]>(
        connection,
        unloadSql,
        [],
        false,
      );
      if (!result) {
        throw new Error('Missing `COPY INTO` query result.');
      }
      return types;
    }
  }

  /**
   * Returns an array of queried fields meta info.
   */
  public async queryColumnTypes(sql: string): Promise<TableStructure> {
    const connection = await this.getConnection();
    return new Promise((resolve, reject) => connection.execute({
      sqlText: `${sql} LIMIT 0`,
      binds: [],
      fetchAsString: ['Number'],
      complete: (err, stmt) => {
        if (err) {
          reject(err);
          return;
        }
        const types: {name: string, type: string}[] =
          this.getTypes(stmt);
        resolve(types);
      },
    }));
  }

  /**
   * Unload data from a temp table to an export bucket.
   */
  private async unloadWithTable(
    tableName: string,
    options: UnloadOptions,
  ): Promise<TableStructure> {
    const types = await this.tableColumnTypes(tableName);
    const connection = await this.getConnection();
    const { bucketType, bucketName } =
      <SnowflakeDriverExportBucket> this.config.exportBucket;
    const unloadSql = `
      COPY INTO '${bucketType}://${bucketName}/${tableName}/'
      FROM ${tableName}
      ${this.exportOptionsClause(options)}`;
    const result = await this.execute<UnloadResponse[]>(
      connection,
      unloadSql,
      [],
      false,
    );
    if (!result) {
      throw new Error('Missing `COPY INTO` query result.');
    }
    return types;
  }

  /**
   * Returns an array of table fields meta info.
   */
  public async tableColumnTypes(table: string) {
    const [schema, name] = table.split('.');
    const columns = await this.query<{
      COLUMN_NAME: string,
      DATA_TYPE: string
    }[]>(
      `SELECT COLUMNS.COLUMN_NAME,
        CASE
          WHEN
            COLUMNS.NUMERIC_SCALE = 0 AND
            COLUMNS.DATA_TYPE = 'NUMBER'
          THEN 'int'
          ELSE COLUMNS.DATA_TYPE
        END as DATA_TYPE
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE
        TABLE_NAME = ${this.param(0)} AND
        TABLE_SCHEMA = ${this.param(1)}
      ORDER BY ORDINAL_POSITION`,
      [name.toUpperCase(), schema.toUpperCase()]
    );
    return columns.map(c => ({
      name: c.COLUMN_NAME,
      type: this.toGenericType(c.DATA_TYPE),
    }));
  }

  /**
   * Returns export options clause.
   */
  private exportOptionsClause(options: UnloadOptions): string {
    const { bucketType } =
      <SnowflakeDriverExportBucket> this.config.exportBucket;
    const optionsToExport: Record<string, string> = {
      HEADER: 'false',
      INCLUDE_QUERY_ID: 'true',
      MAX_FILE_SIZE: (options.maxFileSize * 1024 * 1024).toFixed(),
      FILE_FORMAT: '(' +
        'TYPE = CSV, ' +
        'COMPRESSION = GZIP, ' +
        'FIELD_OPTIONALLY_ENCLOSED_BY = \'"\'' +
        ')',
    };
    switch (bucketType) {
      case 's3':
        optionsToExport.CREDENTIALS = `(AWS_KEY_ID = '${
          (
            <SnowflakeDriverExportAWS> this.config.exportBucket
          ).keyId
        }' AWS_SECRET_KEY = '${
          (
            <SnowflakeDriverExportAWS> this.config.exportBucket
          ).secretKey
        }')`;
        break;
      case 'gcs':
        optionsToExport.STORAGE_INTEGRATION = (
          <SnowflakeDriverExportGCS> this.config.exportBucket
        ).integrationName;
        break;
      default:
        throw new Error('Unsupported export bucket type.');
    }
    const clause = Object.entries(optionsToExport)
      .map(([key, value]) => `${key} = ${value}`)
      .join(' ');
    return clause;
  }

  /**
   * Returns an array of signed URLs of the unloaded csv files.
   */
  private async getCsvFiles(tableName: string): Promise<string[]> {
    const { bucketType, bucketName } =
      <SnowflakeDriverExportBucket> this.config.exportBucket;
    switch (bucketType) {
      case 's3':
        return this.extractFilesFromS3(
          new S3({
            credentials: {
              accessKeyId: (
                <SnowflakeDriverExportAWS> this.config.exportBucket
              ).keyId,
              secretAccessKey: (
                <SnowflakeDriverExportAWS> this.config.exportBucket
              ).secretKey,
            },
            region: (
              <SnowflakeDriverExportAWS> this.config.exportBucket
            ).region,
          }),
          bucketName,
          tableName,
        );
      case 'gcs':
        return this.extractFilesFromGCS(
          new Storage({
            credentials: (
              <SnowflakeDriverExportGCS> this.config.exportBucket
            ).credentials,
          }),
          bucketName,
          tableName,
        );
      default:
        throw new Error('Unsupported export bucket type.');
    }
  }

  /**
   * Returns an array of signed AWS S3 URLs of the unloaded csv files.
   */
  protected async extractFilesFromS3(
    storage: S3,
    bucketName: string,
    tableName: string
  ): Promise<string[]> {
    const list = await storage.listObjectsV2({
      Bucket: bucketName,
      Prefix: tableName,
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
    throw new Error('Unable to unload.');
  }

  /**
   * Returns an array of signed GCS URLs of the unloaded csv files.
   */
  protected async extractFilesFromGCS(
    storage: Storage,
    bucketName: string,
    tableName: string
  ): Promise<string[]> {
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
    const connection = await this.getConnection();
    return new Promise((resolve, reject) => connection.execute({
      sqlText: query,
      binds: <string[] | undefined>values,
      fetchAsString: ['Number'],
      complete: (err, stmt, rows) => {
        if (err) {
          reject(err);
          return;
        }
        const hydrationMap = this.generateHydrationMap(stmt.getColumns());
        const types: {name: string, type: string}[] =
          this.getTypes(stmt);
        if (rows && rows.length && Object.keys(hydrationMap).length) {
          for (const row of rows) {
            for (const [field, toValue] of Object.entries(hydrationMap)) {
              if (row.hasOwnProperty(field)) {
                row[field] = toValue(row[field]);
              }
            }
          }
        }
        resolve({ types, rows: rows || [] });
      }
    }));
  }

  /**
   * Returns stream table object that includes query result stream and
   * queried fields types.
   */
  public async stream(
    query: string,
    values: unknown[],
    _options: StreamOptions,
  ): Promise<StreamTableDataWithTypes> {
    const connection = await this.getConnection();
    const stmt = await new Promise<Statement>((resolve, reject) => connection.execute({
      sqlText: query,
      binds: <string[] | undefined>values,
      fetchAsString: [
        // It's not possible to store big numbers in Number, It's a common way how to handle it in Cube.js
        'Number',
        // VARIANT, OBJECT, ARRAY are mapped to JSON type in Snowflake SDK
        'JSON'
      ],
      streamResult: true,
      complete: (err, statement) => {
        if (err) {
          reject(err);
          return;
        }

        resolve(statement);
      }
    }));
    const types: {name: string, type: string}[] =
      this.getTypes(stmt);
    const hydrationMap = this.generateHydrationMap(stmt.getColumns());
    if (Object.keys(hydrationMap).length) {
      const rowStream = new HydrationStream(hydrationMap);
      stmt.streamRows().pipe(rowStream);
      return {
        rowStream,
        types,
        release: async () => {
          //
        }
      };
    }
    return {
      rowStream: stmt.streamRows(),
      types,
      release: async () => {
        //
      }
    };
  }

  private getTypes(stmt: Statement) {
    return stmt.getColumns().map((column) => {
      const type = {
        name: column.getName().toLowerCase(),
        type: '',
      };
      if (column.isNumber()) {
        // @ts-ignore
        if (column.getScale() === 0) {
          type.type = 'int';
        } else if (column.getScale() && column.getScale() <= 10) {
          type.type = 'decimal';
        } else {
          type.type = this.toGenericType(column.getType());
        }
      } else {
        type.type = this.toGenericType(column.getType());
      }
      return type;
    });
  }

  protected generateHydrationMap(columns: Column[]): HydrationMap {
    const hydrationMap: Record<string, any> = {};

    for (const column of columns) {
      for (const hydrator of hydrators) {
        if (hydrator.types.includes(column.getType())) {
          const fnOrNull = hydrator.toValue(column);
          if (fnOrNull) {
            hydrationMap[column.getName()] = fnOrNull;
          }
        }
      }
    }

    return hydrationMap;
  }

  protected async execute<R = unknown>(
    connection: Connection,
    query: string,
    values?: unknown[],
    rehydrate: boolean = true
  ): Promise<R> {
    return new Promise((resolve, reject) => connection.execute({
      sqlText: query,
      binds: <string[] | undefined>values,
      fetchAsString: ['Number'],
      complete: (err, stmt, rows) => {
        if (err) {
          reject(err);
          return;
        }

        if (rehydrate && rows?.length) {
          const hydrationMap = this.generateHydrationMap(stmt.getColumns());
          if (Object.keys(hydrationMap).length) {
            for (const row of rows) {
              for (const [field, toValue] of Object.entries(hydrationMap)) {
                if (row.hasOwnProperty(field)) {
                  row[field] = toValue(row[field]);
                }
              }
            }
          }
        }

        resolve(<any>rows);
      }
    }));
  }

  public informationSchemaQuery() {
    return `
        SELECT COLUMNS.COLUMN_NAME as "column_name",
               COLUMNS.TABLE_NAME as "table_name",
               COLUMNS.TABLE_SCHEMA as "table_schema",
               CASE WHEN COLUMNS.NUMERIC_SCALE = 0 AND COLUMNS.DATA_TYPE = 'NUMBER' THEN 'int' ELSE COLUMNS.DATA_TYPE END as "data_type"
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE COLUMNS.TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA')
     `;
  }

  public async release(): Promise<void> {
    if (this.connection) {
      this.connection.then((connection) => new Promise<void>(
        (resolve, reject) => connection.destroy((err) => (err ? reject(err) : resolve()))
      ));
    }
  }

  public toGenericType(columnType: string) {
    return SnowflakeToGenericType[columnType.toLowerCase()] || super.toGenericType(columnType);
  }

  public async getTablesQuery(schemaName: string) {
    const tables = await super.getTablesQuery(schemaName.toUpperCase());
    return tables.map(t => ({ table_name: t.TABLE_NAME && t.TABLE_NAME.toLowerCase() }));
  }
}
