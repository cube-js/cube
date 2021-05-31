/* eslint-disable no-restricted-syntax */
import snowflake, { Column, Connection, Statement } from 'snowflake-sdk';
import {
  BaseDriver, DownloadTableCSVData,
  DriverInterface,
  GenericDataBaseType,
  StreamTableData,
  UnloadOptions,
} from '@cubejs-backend/query-orchestrator';
import * as crypto from 'crypto';
import { formatToTimeZone } from 'date-fns-timezone';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import { S3, GetObjectCommand } from '@aws-sdk/client-s3';
import { getEnv } from '@cubejs-backend/shared';

import { HydrationMap, HydrationStream } from './HydrationStream';

type HydrationConfiguration = {
  types: string[], toValue: (column: Column) => ((value: any) => any)|null
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
  exportBucket?: SnowflakeDriverExportAWS,
}

/**
 * Attention:
 * Snowflake is using UPPER_CASE for table, schema and column names
 * Similar to data in response, column_name will be COLUMN_NAME
 */
export class SnowflakeDriver extends BaseDriver implements DriverInterface {
  protected connection: Promise<Connection>|null = null;

  protected readonly config: SnowflakeDriverOptions;

  public constructor(config: Partial<SnowflakeDriverOptions> = {}) {
    super();

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
      exportBucket: this.getExportBucket(),
      ...config
    };
  }

  /**
   * @todo Move to BaseDriver in the future?
   */
  protected getExportBucket(): SnowflakeDriverExportAWS|undefined {
    const exportBucket: Partial<SnowflakeDriverExportAWS> = {
      bucketType: getEnv('dbExportBucketType', {
        supported: ['s3']
      }),
      bucketName: getEnv('dbExportBucket'),
      keyId: getEnv('dbExportBucketAwsKey'),
      secretKey: getEnv('dbExportBucketAwsSecret'),
      region: getEnv('dbExportBucketAwsRegion'),
    };

    if (exportBucket.bucketType) {
      const supportedBucketTypes = ['s3'];

      if (!supportedBucketTypes.includes(exportBucket.bucketType)) {
        throw new Error(
          `Unsupported EXPORT_BUCKET_TYPE, supported: ${supportedBucketTypes.join(',')}`
        );
      }

      const emptyKeys = Object.keys(exportBucket)
        .filter((key: string) => exportBucket[<keyof SnowflakeDriverExportAWS>key] === undefined);
      if (emptyKeys.length) {
        throw new Error(
          `Unsupported configuration exportBucket, some configuration keys are empty: ${emptyKeys.join(',')}`
        );
      }

      return <SnowflakeDriverExportAWS>exportBucket;
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
    await this.query('SELECT 1 as number');
  }

  protected async getConnection(): Promise<Connection> {
    if (this.connection) {
      return this.connection;
    }

    // eslint-disable-next-line no-return-assign
    return this.connection = (async () => {
      try {
        const connection = snowflake.createConnection(this.config);
        await new Promise(
          (resolve, reject) => connection.connect((err, conn) => (err ? reject(err) : resolve(conn)))
        );

        await this.execute(connection, 'ALTER SESSION SET TIMEZONE = \'UTC\'', [], false);
        await this.execute(connection, 'ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 600', [], false);

        return connection;
      } catch (e) {
        this.connection = null;

        throw e;
      }
    })();
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

    const { bucketType, bucketName, keyId, secretKey, region } = this.config.exportBucket;

    const exportPathName = crypto.randomBytes(10).toString('hex');

    const optionsToExport = {
      HEADER: 'true',
      INCLUDE_QUERY_ID: 'true',
      MAX_FILE_SIZE: options.maxFileSize,
      CREDENTIALS: `(AWS_KEY_ID = '${keyId}' AWS_SECRET_KEY = '${secretKey}')`,
      FILE_FORMAT: '(TYPE = CSV, COMPRESSION = GZIP)',
    };
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

    const client = new S3({
      credentials: {
        accessKeyId: keyId,
        secretAccessKey: secretKey,
      },
      region,
    });
    const list = await client.listObjectsV2({
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
          return getSignedUrl(client, command, { expiresIn: 3600 });
        })
      );

      return {
        csvFile,
      };
    }

    throw new Error('Unable to UNLOAD table, there are no files in S3 storage');
  }

  public async stream(
    query: string,
    values: unknown[],
  ): Promise<StreamTableData> {
    const connection = await this.getConnection();

    const stmt = await new Promise<Statement>((resolve, reject) => connection.execute({
      sqlText: query,
      binds: <string[]|undefined>values,
      fetchAsString: ['Number'],
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
      binds: <string[]|undefined>values,
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
        SELECT columns.column_name as "column_name",
               columns.table_name as "table_name",
               columns.table_schema as "table_schema",
               columns.data_type as "data_type"
        FROM information_schema.columns
        WHERE columns.table_schema NOT IN ('INFORMATION_SCHEMA')
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
      `SELECT columns.column_name,
             columns.table_name,
             columns.table_schema,
             columns.data_type
      FROM information_schema.columns
      WHERE table_name = ${this.param(0)} AND table_schema = ${this.param(1)}`,
      [name.toUpperCase(), schema.toUpperCase()]
    );

    return columns.map(c => ({ name: c.COLUMN_NAME, type: this.toGenericType(c.DATA_TYPE) }));
  }

  public async getTablesQuery(schemaName: string) {
    const tables = await super.getTablesQuery(schemaName.toUpperCase());
    return tables.map(t => ({ table_name: t.TABLE_NAME && t.TABLE_NAME.toLowerCase() }));
  }
}
