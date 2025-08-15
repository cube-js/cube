/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview The `SnowflakeDriver` and related types declaration.
 */

import { assertDataSource, getEnv, } from '@cubejs-backend/shared';
import snowflake, { Column, Connection, RowStatement } from 'snowflake-sdk';
import {
  BaseDriver,
  DownloadQueryResultsOptions,
  DownloadQueryResultsResult,
  DownloadTableCSVData,
  DownloadTableMemoryData,
  DriverCapabilities,
  DriverInterface,
  GenericDataBaseType,
  StreamOptions,
  StreamTableDataWithTypes,
  TableStructure,
  UnloadOptions,
} from '@cubejs-backend/base-driver';
import { formatToTimeZone } from 'date-fns-timezone';
import fs from 'fs/promises';
import crypto from 'crypto';
import { HydrationMap, HydrationStream } from './HydrationStream';

const SUPPORTED_BUCKET_TYPES = ['s3', 'gcs', 'azure'];

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
  },
  {
    types: ['object'], // Workaround for HLL_SNOWFLAKE
    toValue: () => (value) => {
      if (!value) {
        return null;
      }

      return JSON.stringify(value);
    },
  }
];

const SnowflakeToGenericType: Record<string, GenericDataBaseType> = {
  // It's a limitation for now, because anyway we don't work with JSON objects in Cube Store.
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
  integrationName?: string,
}

interface SnowflakeDriverExportGCS {
  bucketType: 'gcs',
  integrationName: string,
  bucketName: string,
  credentials: any,
}

interface SnowflakeDriverExportAzure {
  bucketType: 'azure',
  bucketName: string,
  azureKey?: string,
  sasToken?: string,
  integrationName?: string,
  /**
   * The client ID of a Microsoft Entra app registration.
   */
  clientId?: string,
  /**
   * ID of the application's Microsoft Entra tenant. Also called its directory ID.
   */
  tenantId?: string,
  /**
   * The path to a file containing a Kubernetes service account token that authenticates the identity.
   */
  tokenFilePath?: string,
}

export type SnowflakeDriverExportBucket = SnowflakeDriverExportAWS | SnowflakeDriverExportGCS
  | SnowflakeDriverExportAzure;

interface SnowflakeDriverOptions {
  host?: string,
  account: string,
  username: string,
  password: string,
  region?: string,
  warehouse?: string,
  role?: string,
  clientSessionKeepAlive?: boolean,
  database?: string,
  authenticator?: string,
  oauthToken?: string,
  oauthTokenPath?: string,
  token?: string,
  privateKeyPath?: string,
  privateKeyPass?: string,
  privateKey?: string,
  resultPrefetch?: number,
  exportBucket?: SnowflakeDriverExportBucket,
  executionTimeout?: number,
  identIgnoreCase?: boolean,
  application: string,
  readOnly?: boolean,
  queryTag?: string,

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
    return 8;
  }

  /**
   * Returns the configurable driver options
   * Note: It returns the unprefixed option names.
   * In case of using multi-sources options need to be prefixed manually.
   */
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
      'CUBEJS_DB_SNOWFLAKE_OAUTH_TOKEN',
      'CUBEJS_DB_SNOWFLAKE_OAUTH_TOKEN_PATH',
      'CUBEJS_DB_SNOWFLAKE_HOST',
      'CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY',
      'CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY_PATH',
      'CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY_PASS',
      'CUBEJS_DB_SNOWFLAKE_QUOTED_IDENTIFIERS_IGNORE_CASE',
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

    if (privateKey) {
      // If the private key is encrypted - we need to decrypt it before passing to
      // snowflake sdk.
      if (privateKey.includes('BEGIN ENCRYPTED PRIVATE KEY')) {
        const keyPasswd = getEnv('snowflakePrivateKeyPass', { dataSource });

        if (!keyPasswd) {
          throw new Error(
            'Snowflake encrypted private key provided, but no passphrase was given.'
          );
        }

        const privateKeyObject = crypto.createPrivateKey({
          key: privateKey,
          format: 'pem',
          passphrase: keyPasswd
        });

        privateKey = privateKeyObject.export({
          format: 'pem',
          type: 'pkcs8'
        });
      }
    }

    snowflake.configure({ logLevel: 'OFF' });

    this.config = {
      readOnly: false,
      host: getEnv('snowflakeHost', { dataSource }),
      account: getEnv('snowflakeAccount', { dataSource }),
      region: getEnv('snowflakeRegion', { dataSource }),
      warehouse: getEnv('snowflakeWarehouse', { dataSource }),
      role: getEnv('snowflakeRole', { dataSource }),
      clientSessionKeepAlive: getEnv('snowflakeSessionKeepAlive', { dataSource }),
      database: getEnv('dbName', { dataSource }),
      username: getEnv('dbUser', { dataSource }),
      password: getEnv('dbPass', { dataSource }),
      authenticator: getEnv('snowflakeAuthenticator', { dataSource }),
      oauthToken: getEnv('snowflakeOAuthToken', { dataSource }),
      oauthTokenPath: getEnv('snowflakeOAuthTokenPath', { dataSource }),
      privateKeyPath: getEnv('snowflakePrivateKeyPath', { dataSource }),
      privateKeyPass: getEnv('snowflakePrivateKeyPass', { dataSource }),
      ...(privateKey ? { privateKey } : {}),
      exportBucket: this.getExportBucket(dataSource),
      resultPrefetch: 1,
      executionTimeout: getEnv('dbQueryTimeout', { dataSource }),
      identIgnoreCase: getEnv('snowflakeQuotedIdentIgnoreCase', { dataSource }),
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
    return {
      unloadWithoutTempTable: true,
      incrementalSchemaLoading: true,
    };
  }

  protected createExportBucket(
    dataSource: string,
    bucketType: string,
  ): SnowflakeDriverExportBucket {
    if (bucketType === 's3') {
      // integrationName is optional for s3
      const integrationName = getEnv('dbExportIntegration', { dataSource });

      return {
        bucketType,
        bucketName: getEnv('dbExportBucket', { dataSource }),
        keyId: getEnv('dbExportBucketAwsKey', { dataSource }),
        secretKey: getEnv('dbExportBucketAwsSecret', { dataSource }),
        region: getEnv('dbExportBucketAwsRegion', { dataSource }),
        ...(integrationName !== undefined && { integrationName }),
      };
    }

    if (bucketType === 'gcs') {
      // integrationName is required for gcs as the only possible way in snowflake
      return {
        bucketType,
        bucketName: getEnv('dbExportBucket', { dataSource }),
        integrationName: getEnv('dbExportIntegration', { dataSource }),
        credentials: getEnv('dbExportGCSCredentials', { dataSource }),
      };
    }

    if (bucketType === 'azure') {
      // integrationName is optional for azure
      const integrationName = getEnv('dbExportIntegration', { dataSource });
      // sasToken is optional for azure if storage integration is used
      const sasToken = getEnv('dbExportAzureSasToken', { dataSource });

      if (!integrationName && !sasToken) {
        throw new Error(
          'Unsupported exportBucket configuration, some keys are empty: integrationName|sasToken'
        );
      }

      // azureKey is optional if DefaultAzureCredential() is used
      const azureKey = getEnv('dbExportBucketAzureKey', { dataSource });

      // These 3 options make sense in case you want to authorize to Azure from
      // application running in the k8s environment.
      const clientId = getEnv('dbExportBucketAzureClientId', { dataSource });
      const tenantId = getEnv('dbExportBucketAzureTenantId', { dataSource });
      const tokenFilePath = getEnv('dbExportBucketAzureTokenFilePAth', { dataSource });

      return {
        bucketType,
        bucketName: getEnv('dbExportBucket', { dataSource }),
        ...(integrationName !== undefined && { integrationName }),
        ...(sasToken !== undefined && { sasToken }),
        ...(azureKey !== undefined && { azureKey }),
        ...(clientId !== undefined && { clientId }),
        ...(tenantId !== undefined && { tenantId }),
        ...(tokenFilePath !== undefined && { tokenFilePath }),
      };
    }

    throw new Error(
      `Unsupported EXPORT_BUCKET_TYPE, supported: ${SUPPORTED_BUCKET_TYPES.join(',')}`
    );
  }

  protected getExportBucket(
    dataSource: string,
  ): SnowflakeDriverExportBucket | undefined {
    const bucketType = getEnv('dbExportBucketType', {
      dataSource,
      supported: SUPPORTED_BUCKET_TYPES,
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

  private async readOAuthToken() {
    const tokenPath = this.config.oauthTokenPath || '/snowflake/session/token';

    try {
      await fs.access(tokenPath);
    } catch (error) {
      throw new Error(`File ${tokenPath} provided by CUBEJS_DB_SNOWFLAKE_OAUTH_TOKEN_PATH does not exist.`);
    }

    const token = await fs.readFile(tokenPath, 'utf8');
    return token.trim();
  }

  private async prepareConnectOptions(): Promise<snowflake.ConnectionOptions> {
    const config: Record<string, any> = {
      account: this.config.account,
      region: this.config.region,
      host: this.config.host,
      application: this.config.application,
      authenticator: this.config.authenticator,
      clientSessionKeepAlive: this.config.clientSessionKeepAlive,
      database: this.config.database,
      warehouse: this.config.warehouse,
      role: this.config.role,
      resultPrefetch: this.config.resultPrefetch,
    };

    if (this.config.queryTag) {
      config.queryTag = this.config.queryTag;
    }

    if (this.config.authenticator?.toUpperCase() === 'OAUTH') {
      config.token = this.config.oauthToken || await this.readOAuthToken();
    } else if (this.config.authenticator?.toUpperCase() === 'SNOWFLAKE_JWT') {
      config.username = this.config.username;
      config.privateKey = this.config.privateKey;
      config.privateKeyPath = this.config.privateKeyPath;
      config.privateKeyPass = this.config.privateKeyPass;
    } else {
      config.username = this.config.username;
      config.password = this.config.password;
    }

    return config as snowflake.ConnectionOptions;
  }

  private async createConnection() {
    const config = await this.prepareConnectOptions();

    return snowflake.createConnection(config);
  }

  /**
   * Test driver's connection.
   */
  public async testConnection() {
    const connection = await this.createConnection();

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
      const connection = await this.createConnection();

      await new Promise(
        (resolve, reject) => connection.connect((err, conn) => (err ? reject(err) : resolve(conn)))
      );

      await this.execute(connection, 'ALTER SESSION SET TIMEZONE = \'UTC\'', [], false);
      await this.execute(connection, `ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = ${this.config.executionTimeout}`, [], false);
      await this.execute(connection, `ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = ${this.config.identIgnoreCase}`, [], false);
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

    this.connection = this.initConnection();
    return this.connection;
  }

  /**
   * Executes query and returns queried rows.
   */
  public async query<R = unknown>(query: string, values?: unknown[]): Promise<R> {
    return this.getConnection().then((connection) => this.execute<R>(connection, query, values));
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
  public async unload(
    tableName: string,
    options: UnloadOptions,
  ): Promise<DownloadTableCSVData> {
    if (!this.config.exportBucket) {
      throw new Error('Export bucket is not configured.');
    }
    if (!SUPPORTED_BUCKET_TYPES.includes(this.config.exportBucket.bucketType as string)) {
      throw new Error(`Unsupported export bucket type: ${
        this.config.exportBucket.bucketType
      }`);
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
      const types = await this.queryColumnTypes(options.query.sql, options.query.params);
      const connection = await this.getConnection();
      const { bucketType } =
        <SnowflakeDriverExportBucket> this.config.exportBucket;

      let bucketName: string;
      let exportPrefix: string;
      let path: string;

      if (bucketType === 'azure') {
        ({ bucketName, path } = this.parseBucketUrl(this.config.exportBucket!.bucketName));
        const pathArr = path.split('/');
        bucketName = `${bucketName}/${pathArr[0]}`;
        exportPrefix = pathArr.length > 1 ? `${pathArr.slice(1).join('/')}/${tableName}` : tableName;
      } else {
        ({ bucketName, path } = this.parseBucketUrl(this.config.exportBucket!.bucketName));
        exportPrefix = path ? `${path}/${tableName}` : tableName;
      }

      const unloadSql = `
        COPY INTO '${bucketType}://${bucketName}/${exportPrefix}/'
        FROM (${options.query.sql})
        ${this.exportOptionsClause(options)}`;
      const result = await this.execute<UnloadResponse[]>(
        connection,
        unloadSql,
        options.query.params,
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
  public async queryColumnTypes(sql: string, params: unknown[]): Promise<TableStructure> {
    const connection = await this.getConnection();
    return new Promise((resolve, reject) => connection.execute({
      sqlText: `${sql} LIMIT 0`,
      binds: <string[] | undefined>params,
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
    const { bucketType } =
      <SnowflakeDriverExportBucket> this.config.exportBucket;

    let bucketName: string;
    let exportPrefix: string;
    let path: string;

    if (bucketType === 'azure') {
      ({ bucketName, path } = this.parseBucketUrl(this.config.exportBucket!.bucketName));
      const pathArr = path.split('/');
      bucketName = `${bucketName}/${pathArr[0]}`;
      exportPrefix = pathArr.length > 1 ? `${pathArr.slice(1).join('/')}/${tableName}` : tableName;
    } else {
      ({ bucketName, path } = this.parseBucketUrl(this.config.exportBucket!.bucketName));
      exportPrefix = path ? `${path}/${tableName}` : tableName;
    }

    const unloadSql = `
      COPY INTO '${bucketType}://${bucketName}/${exportPrefix}/'
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
        'DATE_FORMAT = \'YYYY-MM-DD\', ' +
        'TIMESTAMP_FORMAT = \'YYYY-MM-DD"T"HH24:MI:SS.FF3TZH:TZM\', ' +
        'FIELD_OPTIONALLY_ENCLOSED_BY = \'"\'' +
        ')',
    };
    if (bucketType === 's3') {
      const conf = <SnowflakeDriverExportAWS> this.config.exportBucket;

      // Storage integration export flow takes precedence over direct auth if it is defined
      if (conf.integrationName) {
        optionsToExport.STORAGE_INTEGRATION = conf.integrationName;
      } else {
        optionsToExport.CREDENTIALS = `(AWS_KEY_ID = '${conf.keyId}' AWS_SECRET_KEY = '${conf.secretKey}')`;
      }
    } else if (bucketType === 'gcs') {
      optionsToExport.STORAGE_INTEGRATION = (
        <SnowflakeDriverExportGCS> this.config.exportBucket
      ).integrationName;
    } else if (bucketType === 'azure') {
      // @see https://docs.snowflake.com/en/sql-reference/sql/copy-into-location
      // @see https://docs.snowflake.com/en/user-guide/data-unload-azure
      const conf = <SnowflakeDriverExportAzure> this.config.exportBucket;

      // Storage integration export flow takes precedence over direct auth if it is defined
      if (conf.integrationName) {
        optionsToExport.STORAGE_INTEGRATION = conf.integrationName;
      } else {
        optionsToExport.CREDENTIALS = `(AZURE_SAS_TOKEN = '${conf.sasToken}')`;
      }
    } else {
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
    const { bucketType } =
      <SnowflakeDriverExportBucket> this.config.exportBucket;

    if (bucketType === 's3') {
      const { keyId, secretKey, region } = <SnowflakeDriverExportAWS> this.config.exportBucket;

      const { bucketName, path } = this.parseBucketUrl(this.config.exportBucket!.bucketName);
      const exportPrefix = path ? `${path}/${tableName}` : tableName;

      return this.extractUnloadedFilesFromS3(
        {
          credentials: {
            accessKeyId: keyId,
            secretAccessKey: secretKey,
          },
          region,
        },
        bucketName,
        exportPrefix,
      );
    } else if (bucketType === 'gcs') {
      const { credentials } = (
        <SnowflakeDriverExportGCS> this.config.exportBucket
      );

      const { bucketName, path } = this.parseBucketUrl(this.config.exportBucket!.bucketName);
      const exportPrefix = path ? `${path}/${tableName}` : tableName;

      return this.extractFilesFromGCS({ credentials }, bucketName, exportPrefix);
    } else if (bucketType === 'azure') {
      const { azureKey, sasToken, clientId, tenantId, tokenFilePath } = (
        <SnowflakeDriverExportAzure> this.config.exportBucket
      );

      const { bucketName, path } = this.parseBucketUrl(this.config.exportBucket!.bucketName);
      const pathArr = path.split('/');
      const azureBucketPath = `${bucketName}/${pathArr[0]}`;

      const exportPrefix = pathArr.length > 1 ? `${pathArr.slice(1).join('/')}/${tableName}` : tableName;

      return this.extractFilesFromAzure(
        { azureKey, sasToken, clientId, tenantId, tokenFilePath },
        azureBucketPath,
        exportPrefix,
      );
    } else {
      throw new Error(`Unsupported export bucket type: ${bucketType}`);
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
        if (rows?.length && Object.keys(hydrationMap).length) {
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
    const stmt = await new Promise<RowStatement>((resolve, reject) => connection.execute({
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

  private getTypes(stmt: RowStatement) {
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
    return tables.map(t => ({ table_name: t.TABLE_NAME?.toLowerCase() }));
  }
}
