/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview The `DatabricksDriver` and related types declaration.
 */

import {
  getEnv,
  assertDataSource,
} from '@cubejs-backend/shared';
import fs from 'fs';
import path from 'path';
import { S3, GetObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import {
  BlobServiceClient,
  StorageSharedKeyCredential,
  ContainerSASPermissions,
  SASProtocol,
  generateBlobSASQueryParameters,
} from '@azure/storage-blob';
import { DriverCapabilities, QueryOptions, UnloadOptions } from '@cubejs-backend/base-driver';
import {
  JDBCDriver,
  JDBCDriverConfiguration,
} from '@cubejs-backend/jdbc-driver';
import { DatabricksQuery } from './DatabricksQuery';
import { downloadJDBCDriver } from './installer';

export type DatabricksDriverConfiguration = JDBCDriverConfiguration &
  {
    /**
     * Driver read-only mode flag.
     */
    readOnly?: boolean,

    /**
     * Export bucket type.
     */
    bucketType?: string,

    /**
     * Export bucket path.
     */
    exportBucket?: string,

    /**
     * Export bucket DBFS mount directory.
     */
    exportBucketMountDir?: string,

    /**
     * Poll interval.
     */
    pollInterval?: number,

    /**
     * The export bucket CSV file escape symbol.
     */
    exportBucketCsvEscapeSymbol?: string,

    /**
     * Export bucket AWS account key.
     */
    awsKey?: string,

    /**
     * Export bucket AWS account secret.
     */
    awsSecret?: string,

    /**
     * Export bucket AWS account region.
     */
    awsRegion?: string,
    
    /**
     * Export bucket Azure account key.
     */
    azureKey?: string,

    /**
     * Databricks catalog name.
     * https://www.databricks.com/product/unity-catalog
     */
    catalog?: string,

    /**
     * Databricks security token (PWD).
     */
    token?: string,
  };

async function fileExistsOr(
  fsPath: string,
  fn: () => Promise<string>,
): Promise<string> {
  if (fs.existsSync(fsPath)) {
    return fsPath;
  }
  return fn();
}

type ShowTableRow = {
  database: string,
  tableName: string,
  isTemporary: boolean,
};

type ShowDatabasesRow = {
  databaseName: string,
};

const DatabricksToGenericType: Record<string, string> = {
  'decimal(10,0)': 'bigint',
};

async function resolveJDBCDriver(): Promise<string> {
  return fileExistsOr(
    path.join(process.cwd(), 'DatabricksJDBC42.jar'),
    async () => fileExistsOr(
      path.join(__dirname, '..', 'download', 'DatabricksJDBC42.jar'),
      async () => {
        const pathOrNull = await downloadJDBCDriver();
        if (pathOrNull) {
          return pathOrNull;
        }
        throw new Error(
          'Please download and place DatabricksJDBC42.jar inside your ' +
          'project directory'
        );
      }
    )
  );
}

/**
 * Databricks driver class.
 */
export class DatabricksDriver extends JDBCDriver {
  /**
   * Show warning message flag.
   */
  private showSparkProtocolWarn: boolean;

  /**
   * Read-only mode flag.
   */
  protected readonly config: DatabricksDriverConfiguration;

  public static dialectClass() {
    return DatabricksQuery;
  }

  /**
   * Returns default concurrency value.
   */
  public static getDefaultConcurrency(): number {
    return 2;
  }

  /**
   * Class constructor.
   */
  public constructor(
    conf: Partial<DatabricksDriverConfiguration> & {
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
    const dataSource =
      conf.dataSource ||
      assertDataSource('default');

    let showSparkProtocolWarn = false;
    let url: string =
      conf?.url ||
      getEnv('databrickUrl', { dataSource }) ||
      getEnv('jdbcUrl', { dataSource });
    if (url.indexOf('jdbc:spark://') !== -1) {
      showSparkProtocolWarn = true;
      url = url.replace('jdbc:spark://', 'jdbc:databricks://');
    }

    const config: DatabricksDriverConfiguration = {
      ...conf,
      url,
      dbType: 'databricks',
      drivername: 'com.databricks.client.jdbc.Driver',
      customClassPath: undefined,
      properties: {
        // PWD-parameter passed to the connection string has higher priority,
        // so we can set this one to an empty string to avoid a Java error.
        PWD:
          conf?.token ||
          getEnv('databrickToken', { dataSource }) ||
          '',
        UserAgentEntry: `CubeDev_Cube`,
      },
      catalog:
        conf?.catalog ||
        getEnv('databricksCatalog', { dataSource }),
      database: getEnv('dbName', { required: false, dataSource }),
      // common export bucket config
      bucketType:
        conf?.bucketType ||
        getEnv('dbExportBucketType', { supported: ['s3', 'azure'], dataSource }),
      exportBucket:
        conf?.exportBucket ||
        getEnv('dbExportBucket', { dataSource }),
      exportBucketMountDir:
        conf?.exportBucketMountDir ||
        getEnv('dbExportBucketMountDir', { dataSource }),
      pollInterval: (
        conf?.pollInterval ||
        getEnv('dbPollMaxInterval', { dataSource })
      ) * 1000,
      // AWS export bucket config
      awsKey:
        conf?.awsKey ||
        getEnv('dbExportBucketAwsKey', { dataSource }),
      awsSecret:
        conf?.awsSecret ||
        getEnv('dbExportBucketAwsSecret', { dataSource }),
      awsRegion:
        conf?.awsRegion ||
        getEnv('dbExportBucketAwsRegion', { dataSource }),
      // Azure export bucket
      azureKey:
        conf?.azureKey ||
        getEnv('dbExportBucketAzureKey', { dataSource }),
      exportBucketCsvEscapeSymbol:
        getEnv('dbExportBucketCsvEscapeSymbol', { dataSource }),
    };
    super(config);
    this.config = config;
    this.showSparkProtocolWarn = showSparkProtocolWarn;
  }

  /**
   * @override
   */
  public readOnly() {
    return !!this.config.readOnly;
  }

  /**
   * @override
   */
  public capabilities(): DriverCapabilities {
    return { unloadWithoutTempTable: true };
  }

  /**
   * @override
   */
  public setLogger(logger: any) {
    super.setLogger(logger);
    this.showDeprecations();
  }

  /**
   * @override
   */
  public async loadPreAggregationIntoTable(
    preAggregationTableName: string,
    loadSql: string,
    params: unknown[],
    _options: any,
  ) {
    if (this.config.catalog) {
      const [schema] = preAggregationTableName.split('.');
      return super.loadPreAggregationIntoTable(
        preAggregationTableName,
        loadSql.replace(
          new RegExp(`(?<=\\s)${schema}\\.(?=[^\\s]+)`, 'g'),
          `${this.config.catalog}.${schema}.`
        ),
        params,
        _options,
      );
    } else {
      return super.loadPreAggregationIntoTable(
        preAggregationTableName,
        loadSql,
        params,
        _options,
      );
    }
  }

  /**
   * @override
   */
  public async query<R = unknown>(
    query: string,
    values: unknown[],
  ): Promise<R[]> {
    if (this.config.catalog) {
      return super.query(
        query.replace(
          new RegExp(`(?<=\\s)${this.getPreaggsSchemaName()}\\.(?=[^\\s]+)`, 'g'),
          `${this.config.catalog}.${this.getPreaggsSchemaName()}.`
        ),
        values,
      );
    } else {
      return super.query(query, values);
    }
  }

  /**
   * Returns pre-aggregation schema name.
   */
  public getPreaggsSchemaName(): string {
    const schema = getEnv('preAggregationsSchema');
    if (schema) {
      return schema;
    } else {
      const devMode =
        process.env.NODE_ENV !== 'production' || getEnv('devMode');
      return devMode
        ? 'dev_pre_aggregations'
        : 'prod_pre_aggregations';
    }
  }

  /**
   * @override
   */
  public dropTable(tableName: string, options?: QueryOptions): Promise<unknown> {
    const tableFullName = `${
      this.config?.catalog ? `${this.config.catalog}.` : ''
    }${tableName}`;
    return super.dropTable(tableFullName, options);
  }

  public showDeprecations() {
    if (this.config.url) {
      const result = this.config.url
        .split(';')
        .find(node => /^PWD/i.test(node))
        ?.split('=')[1];

      if (result) {
        this.logger('PWD Parameter Deprecation in connection string', {
          warning:
            'PWD parameter is deprecated and will be ignored in future releases. ' +
            'Please migrate to the CUBEJS_DB_DATABRICKS_TOKEN environment variable.'
        });
      }
    }
    if (this.showSparkProtocolWarn) {
      this.logger('jdbc:spark protocol deprecation', {
        warning:
          'The `jdbc:spark` protocol is deprecated and will be ignored in future releases. ' +
          'Please migrate your CUBEJS_DB_DATABRICKS_URL environment variable to the ' +
          '`jdbc:databricks` protocol.'
      });
    }
  }

  /**
   * @override
   */
  protected async getCustomClassPath() {
    return resolveJDBCDriver();
  }

  /**
   * Execute create schema query.
   */
  public async createSchemaIfNotExists(schemaName: string) {
    await this.query(
      `CREATE SCHEMA IF NOT EXISTS ${
        this.getSchemaFullName(schemaName)
      }`,
      [],
    );
  }

  /**
   * Returns the list of the tables for the specified schema.
   */
  public async getTablesQuery(schemaName: string): Promise<{ 'table_name': string }[]> {
    const response = await this.query(
      `SHOW TABLES IN ${this.getSchemaFullName(schemaName)}`,
      [],
    );
    return response.map((row: any) => ({
      table_name: row.tableName,
    }));
  }

  /**
   * Returns tables meta data object.
   */
  public async tablesSchema(): Promise<Record<string, Record<string, object>>> {
    const tables = await this.getTables();

    const metadata: Record<string, Record<string, object>> = {};

    await Promise.all(tables.map(async ({ database, tableName }) => {
      if (!(database in metadata)) {
        metadata[database] = {};
      }

      const columns = await this.tableColumnTypes(`${database}.${tableName}`);
      metadata[database][tableName] = columns;
    }));

    return metadata;
  }

  /**
   * Returns list of accessible tables.
   */
  public async getTables(): Promise<ShowTableRow[]> {
    if (this.config.database) {
      return <any> this.query<ShowTableRow>(
        `SHOW TABLES IN ${
          this.getSchemaFullName(this.config.database)
        }`,
        [],
      );
    }

    const databases = await this.query<ShowDatabasesRow>(
      `SHOW DATABASES${
        this.config?.catalog
          ? ` IN ${this.quoteIdentifier(this.config.catalog)}`
          : ''
      }`,
      [],
    );

    const tables = await Promise.all(
      databases.map(async ({ databaseName }) => this.query<ShowTableRow>(
        `SHOW TABLES IN ${this.getSchemaFullName(databaseName)}`,
        []
      ))
    );

    return tables.flat();
  }

  /**
   * Returns table columns types.
   */
  public async tableColumnTypes(table: string): Promise<{ name: any; type: string; }[]> {
    let tableFullName = '';
    const tableArray = table.split('.');

    if (tableArray.length === 3) {
      tableFullName = `${
        this.quoteIdentifier(tableArray[0])
      }.${
        this.quoteIdentifier(tableArray[1])
      }.${
        this.quoteIdentifier(tableArray[2])
      }`;
    } else if (tableArray.length === 2 && this.config?.catalog) {
      tableFullName = `${
        this.quoteIdentifier(this.config.catalog)
      }.${
        this.quoteIdentifier(tableArray[0])
      }.${
        this.quoteIdentifier(tableArray[1])
      }`;
    } else {
      tableFullName = `${
        this.quoteIdentifier(tableArray[0])
      }.${
        this.quoteIdentifier(tableArray[1])
      }`;
    }

    const result = [];
    const response: any[] = await this.query(`DESCRIBE ${tableFullName}`, []);

    for (const column of response) {
      // Databricks describe additional info by default after empty line.
      if (column.col_name === '') {
        break;
      }
      result.push({
        name: column.col_name,
        type: this.toGenericType(column.data_type),
      });
    }

    return result;
  }

  /**
   * Returns query columns types.
   */
  public async queryColumnTypes(
    sql: string,
    params?: unknown[]
  ): Promise<{ name: any; type: string; }[]> {
    const result = [];
    // eslint-disable-next-line camelcase
    const response = await this.query<{col_name: string; data_type: string}>(
      `DESCRIBE QUERY ${sql}`,
      params || []
    );

    for (const column of response) {
      // Databricks describe additional info by default after empty line.
      if (column.col_name === '') {
        break;
      }

      result.push({ name: column.col_name, type: this.toGenericType(column.data_type) });
    }

    return result;
  }

  /**
   * Returns schema full name.
   */
  public getSchemaFullName(schema: string): string {
    if (this.config?.catalog) {
      return `${
        this.quoteIdentifier(this.config.catalog)
      }.${
        this.quoteIdentifier(schema)
      }`;
    } else {
      return `${this.quoteIdentifier(schema)}`;
    }
  }

  /**
   * Returns quoted string.
   */
  public quoteIdentifier(identifier: string): string {
    return `\`${identifier}\``;
  }

  /**
   * Returns the JS type by the Databricks type.
   */
  public toGenericType(columnType: string): string {
    return DatabricksToGenericType[columnType.toLowerCase()] || super.toGenericType(columnType);
  }

  /**
   * Determines whether export bucket feature is configured or not.
   */
  public async isUnloadSupported() {
    return this.config.exportBucket !== undefined;
  }

  /**
   * Returns to the Cubestore an object with links to unloaded to an
   * export bucket data.
   */
  public async unload(tableName: string, options: UnloadOptions) {
    if (!['azure', 's3'].includes(this.config.bucketType as string)) {
      throw new Error(`Unsupported export bucket type: ${
        this.config.bucketType
      }`);
    }
    const tableFullName = `${
      this.config.catalog
        ? `${this.config.catalog}.`
        : ''
    }${tableName}`;
    const types = options.query
      ? await this.unloadWithSql(
        tableFullName,
        options.query.sql,
        options.query.params,
      )
      : await this.unloadWithTable(tableFullName);
    const pathname = `${this.config.exportBucket}/${tableFullName}.csv`;
    const csvFile = await this.getCsvFiles(pathname);
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
  private async unloadWithSql(tableFullName: string, sql: string, params: unknown[]) {
    const types = await this.queryColumnTypes(sql, params);

    await this.createExternalTableFromSql(tableFullName, sql, params);
    
    return types;
  }

  /**
   * Unload data from a temp table to an export bucket.
   */
  private async unloadWithTable(tableFullName: string) {
    const types = await this.tableColumnTypes(tableFullName);
    const columns = types.map(t => t.name).join(', ');

    await this.createExternalTableFromTable(tableFullName, columns);
    
    return types;
  }

  /**
   * Returns an array of signed URLs of unloaded csv files.
   */
  private async getCsvFiles(
    pathname: string,
  ): Promise<string[]> {
    let res;
    switch (this.config.bucketType) {
      case 'azure':
        res = await this.getSignedAzureUrls(pathname);
        break;
      case 's3':
        res = await this.getSignedS3Urls(pathname);
        break;
      default:
        throw new Error(`Unsupported export bucket type: ${
          this.config.bucketType
        }`);
    }
    return res;
  }

  /**
   * Returns Azure signed URLs of unloaded scv files.
   */
  private async getSignedAzureUrls(
    pathname: string,
  ): Promise<string[]> {
    const csvFile: string[] = [];
    const [container, account] =
      pathname.split('wasbs://')[1].split('.blob')[0].split('@');
    const foldername =
      pathname.split(`${this.config.exportBucket}/`)[1];
    const expr = new RegExp(`${foldername}\\/.*\\.csv$`, 'i');

    const credential = new StorageSharedKeyCredential(
      account,
      this.config.azureKey as string,
    );
    const blobClient = new BlobServiceClient(
      `https://${account}.blob.core.windows.net`,
      credential,
    );
    const containerClient = blobClient.getContainerClient(container);
    const blobsList = containerClient.listBlobsFlat({ prefix: foldername });
    for await (const blob of blobsList) {
      if (blob.name && expr.test(blob.name)) {
        const sas = generateBlobSASQueryParameters(
          {
            containerName: container,
            blobName: blob.name,
            permissions: ContainerSASPermissions.parse('r'),
            startsOn: new Date(new Date().valueOf()),
            expiresOn:
              new Date(new Date().valueOf() + 1000 * 60 * 60),
            protocol: SASProtocol.Https,
            version: '2020-08-04',
          },
          credential,
        ).toString();
        csvFile.push(`https://${
          account
        }.blob.core.windows.net/${
          container
        }/${blob.name}?${sas}`);
      }
    }
    if (csvFile.length === 0) {
      throw new Error('No CSV files were exported to the specified bucket. ' +
        'Please check your export bucket configuration.');
    }
    return csvFile;
  }

  /**
   * Returns S3 signed URLs of unloaded scv files.
   */
  private async getSignedS3Urls(
    pathname: string,
  ): Promise<string[]> {
    const client = new S3({
      credentials: {
        accessKeyId: this.config.awsKey as string,
        secretAccessKey: this.config.awsSecret as string,
      },
      region: this.config.awsRegion,
    });
    const url = new URL(pathname);
    const list = await client.listObjectsV2({
      Bucket: url.host,
      Prefix: url.pathname.slice(1),
    });
    if (list.Contents === undefined) {
      throw new Error(`No content in specified path: ${pathname}`);
    }
    const csvFile = await Promise.all(
      list.Contents
        .filter(file => file.Key && /.csv$/i.test(file.Key))
        .map(async (file) => {
          const command = new GetObjectCommand({
            Bucket: url.host,
            Key: file.Key,
          });
          return getSignedUrl(client, command, { expiresIn: 3600 });
        })
    );
    if (csvFile.length === 0) {
      throw new Error('No CSV files were exported to the specified bucket. ' +
        'Please check your export bucket configuration.');
    }
    return csvFile;
  }

  /**
   * Saves specified query to the configured bucket. This requires Databricks
   * cluster to be configured.
   *
   * For Azure blob storage you need to configure account access key in
   * Cluster -> Configuration -> Advanced options
   * (https://docs.databricks.com/data/data-sources/azure/azure-storage.html#access-azure-blob-storage-directly)
   *
   * `fs.azure.account.key.<storage-account-name>.blob.core.windows.net <storage-account-access-key>`
   *
   * For S3 bucket storage you need to configure AWS access key and secret in
   * Cluster -> Configuration -> Advanced options
   * (https://docs.databricks.com/data/data-sources/aws/amazon-s3.html#access-s3-buckets-directly)
   *
   * `fs.s3a.access.key <aws-access-key>`
   * `fs.s3a.secret.key <aws-secret-key>`
   */
  private async createExternalTableFromSql(tableFullName: string, sql: string, params: unknown[]) {
    try {
      await this.query(
        `
        CREATE TABLE ${tableFullName}
        USING CSV LOCATION '${this.config.exportBucketMountDir || this.config.exportBucket}/${tableFullName}.csv'
        OPTIONS (escape = '"')
        AS (${sql});
        `,
        params,
      );
    } finally {
      await this.query(`DROP TABLE IF EXISTS ${tableFullName};`, []);
    }
  }

  /**
   * Saves specified table to the configured bucket. This requires Databricks
   * cluster to be configured.
   *
   * For Azure blob storage you need to configure account access key in
   * Cluster -> Configuration -> Advanced options
   * (https://docs.databricks.com/data/data-sources/azure/azure-storage.html#access-azure-blob-storage-directly)
   *
   * `fs.azure.account.key.<storage-account-name>.blob.core.windows.net <storage-account-access-key>`
   *
   * For S3 bucket storage you need to configure AWS access key and secret in
   * Cluster -> Configuration -> Advanced options
   * (https://docs.databricks.com/data/data-sources/aws/amazon-s3.html#access-s3-buckets-directly)
   *
   * `fs.s3a.access.key <aws-access-key>`
   * `fs.s3a.secret.key <aws-secret-key>`
   */
  private async createExternalTableFromTable(tableFullName: string, columns: string) {
    try {
      await this.query(
        `
        CREATE TABLE _${tableFullName}
        USING CSV LOCATION '${this.config.exportBucketMountDir || this.config.exportBucket}/${tableFullName}.csv'
        OPTIONS (escape = '"')
        AS SELECT ${columns} FROM ${tableFullName}
        `,
        [],
      );
    } finally {
      await this.query(`DROP TABLE IF EXISTS _${tableFullName};`, []);
    }
  }
}
