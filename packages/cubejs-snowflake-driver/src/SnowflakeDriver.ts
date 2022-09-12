/* eslint-disable no-restricted-syntax */
import snowflake, { Column, Connection, Statement } from 'snowflake-sdk';
import {
  BaseDriver, DownloadTableCSVData,
  DriverInterface,
  GenericDataBaseType,
  StreamTableData,
  UnloadOptions,
} from '@cubejs-backend/base-driver';
import * as crypto from 'crypto';
import { formatToTimeZone } from 'date-fns-timezone';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import { S3, GetObjectCommand } from '@aws-sdk/client-s3';
import { Storage } from '@google-cloud/storage';
import { getEnv } from '@cubejs-backend/shared';

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
}

/**
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

  protected connection: Promise<Connection> | null = null;

  protected readonly config: SnowflakeDriverOptions;

  public constructor(config: Partial<SnowflakeDriverOptions> = {}) {
    super();

    let privateKey = process.env.CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY;
    if (privateKey && !privateKey.endsWith('\n')) {
      privateKey += '\n';
    }
    this.config = {
      account: <string>process.env.CUBEJS_DB_SNOWFLAKE_ACCOUNT,
      region: process.env.CUBEJS_DB_SNOWFLAKE_REGION,
      warehouse: process.env.CUBEJS_DB_SNOWFLAKE_WAREHOUSE,
      role: process.env.CUBEJS_DB_SNOWFLAKE_ROLE,
      clientSessionKeepAlive: process.env.CUBEJS_DB_SNOWFLAKE_CLIENT_SESSION_KEEP_ALIVE === 'true',
      database: process.env.CUBEJS_DB_NAME,
      username: <string>process.env.CUBEJS_DB_USER,
      password: <string>process.env.CUBEJS_DB_PASS,
      authenticator: process.env.CUBEJS_DB_SNOWFLAKE_AUTHENTICATOR,
      privateKeyPath: process.env.CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY_PATH,
      privateKeyPass: process.env.CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY_PASS,
      privateKey,
      exportBucket: this.getExportBucket(),
      resultPrefetch: 1,
      executionTimeout: getEnv('dbQueryTimeout'),
      ...config
    };
  }

  protected createExportBucket(bucketType: string): SnowflakeDriverExportBucket {
    if (bucketType === 's3') {
      return {
        bucketType,
        bucketName: getEnv('dbExportBucket'),
        keyId: getEnv('dbExportBucketAwsKey'),
        secretKey: getEnv('dbExportBucketAwsSecret'),
        region: getEnv('dbExportBucketAwsRegion'),
      };
    }

    if (bucketType === 'gcs') {
      return {
        bucketType,
        bucketName: getEnv('dbExportBucket'),
        integrationName: getEnv('dbExportIntegration'),
        credentials: getEnv('dbExportGCSCredentials'),
      };
    }

    throw new Error(
      `Unsupported EXPORT_BUCKET_TYPE, supported: ${['s3', 'gcs'].join(',')}`
    );
  }

  /**
   * @todo Move to BaseDriver in the future?
   */
  protected getExportBucket(): SnowflakeDriverExportBucket | undefined {
    const bucketType = getEnv('dbExportBucketType', {
      supported: ['s3', 'gcs']
    });
    if (bucketType) {
      const exportBucket = this.createExportBucket(
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

  public static driverEnvVariables() {
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

  public async query<R = unknown>(query: string, values?: unknown[]): Promise<R> {
    return this.getConnection().then((connection) => this.execute<R>(connection, query, values));
  }

  public async isUnloadSupported() {
    if (!this.config.exportBucket) {
      return false;
    }

    return true;
  }

  public async unload(tableName: string, options: UnloadOptions): Promise<DownloadTableCSVData> {
    const connection = await this.getConnection();

    if (!this.config.exportBucket) {
      throw new Error('Unload is not configured');
    }

    const { bucketType, bucketName } = this.config.exportBucket;

    const exportPathName = crypto.randomBytes(10).toString('hex');

    // @link https://docs.snowflake.com/en/sql-reference/sql/copy-into-location.html
    const optionsToExport: Record<string, string> = {
      HEADER: 'true',
      INCLUDE_QUERY_ID: 'true',
      // the upper size limit (in bytes) of each file to be generated in parallel per thread
      MAX_FILE_SIZE: (options.maxFileSize * 1024 * 1024).toFixed(),
      FILE_FORMAT: '(TYPE = CSV, COMPRESSION = GZIP, FIELD_OPTIONALLY_ENCLOSED_BY = \'"\')',
    };

    let unloadExtractor: () => Promise<DownloadTableCSVData> = async () => {
      throw new Error('Unsupported');
    };

    // eslint-disable-next-line default-case
    switch (bucketType) {
      case 's3':
        {
          const { keyId, secretKey, region } = <SnowflakeDriverExportAWS> this.config.exportBucket;

          optionsToExport.CREDENTIALS = `(AWS_KEY_ID = '${keyId}' AWS_SECRET_KEY = '${secretKey}')`;
          unloadExtractor = () => this.extractFilesFromS3(
            new S3({
              credentials: {
                accessKeyId: keyId,
                secretAccessKey: secretKey,
              },
              region,
            }),
            bucketName,
            exportPathName,
          );
        }
        break;
      case 'gcs':
        {
          const { integrationName, credentials } = <SnowflakeDriverExportGCS> this.config.exportBucket;

          optionsToExport.STORAGE_INTEGRATION = `${integrationName}`;
          unloadExtractor = () => this.extractFilesFromGCS(
            new Storage({
              credentials
            }),
            bucketName,
            exportPathName,
          );
        }
        break;
      default:
        throw new Error(
          `Unsupported EXPORT_BUCKET_TYPE, supported: ${['s3', 'gcs'].join(',')}`
        );
    }

    const optionsPart = Object.entries(optionsToExport)
      .map(([key, value]) => `${key} = ${value}`)
      .join(' ');

    const result = await this.execute<UnloadResponse[]>(
      connection,
      `COPY INTO '${bucketType}://${bucketName}/${exportPathName}/' FROM ${tableName} ${optionsPart}`,
      [],
      false
    );
    if (!result) {
      throw new Error('Snowflake doesn\'t return anything on UNLOAD operation');
    }

    if (result[0].rows_unloaded === '0') {
      return {
        csvFile: [],
      };
    }

    return unloadExtractor();
  }

  protected async extractFilesFromS3(
    storage: S3,
    bucketName: string,
    exportPathName: string
  ): Promise<DownloadTableCSVData> {
    const list = await storage.listObjectsV2({
      Bucket: bucketName,
      Prefix: exportPathName,
    });
    if (list && list.Contents) {
      const csvFile = await Promise.all(
        list.Contents.map(async (file) => {
          const command = new GetObjectCommand({
            Bucket: bucketName,
            Key: file.Key,
          });
          return getSignedUrl(storage, command, { expiresIn: 3600 });
        })
      );

      return {
        csvFile,
      };
    }

    throw new Error('Unable to UNLOAD table, there are no files in S3 storage');
  }

  protected async extractFilesFromGCS(
    storage: Storage,
    bucketName: string,
    exportPathName: string
  ): Promise<DownloadTableCSVData> {
    const bucket = storage.bucket(bucketName);

    const [files] = await bucket.getFiles({ prefix: `${exportPathName}/` });
    if (files.length) {
      const csvFile = await Promise.all(files.map(async (file) => {
        const [url] = await file.getSignedUrl({
          action: 'read',
          expires: new Date(new Date().getTime() + 60 * 60 * 1000)
        });

        return url;
      }));

      return { csvFile };
    }

    throw new Error('Unable to UNLOAD table, there are no files in GCS storage');
  }

  public async stream(
    query: string,
    values: unknown[],
  ): Promise<StreamTableData> {
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

    const hydrationMap = this.generateHydrationMap(stmt.getColumns());
    if (Object.keys(hydrationMap).length) {
      const rowStream = new HydrationStream(hydrationMap);
      stmt.streamRows().pipe(rowStream);

      return {
        rowStream,
        release: async () => {
          //
        }
      };
    }

    return {
      rowStream: stmt.streamRows(),
      release: async () => {
        //
      }
    };
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

  public async tableColumnTypes(table: string) {
    const [schema, name] = table.split('.');

    const columns = await this.query<{ COLUMN_NAME: string, DATA_TYPE: string }[]>(
      `SELECT COLUMNS.COLUMN_NAME,
             CASE WHEN COLUMNS.NUMERIC_SCALE = 0 AND COLUMNS.DATA_TYPE = 'NUMBER' THEN 'int' ELSE COLUMNS.DATA_TYPE END as DATA_TYPE
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_NAME = ${this.param(0)} AND TABLE_SCHEMA = ${this.param(1)}
      ORDER BY ORDINAL_POSITION`,
      [name.toUpperCase(), schema.toUpperCase()]
    );

    return columns.map(c => ({ name: c.COLUMN_NAME, type: this.toGenericType(c.DATA_TYPE) }));
  }

  public async getTablesQuery(schemaName: string) {
    const tables = await super.getTablesQuery(schemaName.toUpperCase());
    return tables.map(t => ({ table_name: t.TABLE_NAME && t.TABLE_NAME.toLowerCase() }));
  }
}
