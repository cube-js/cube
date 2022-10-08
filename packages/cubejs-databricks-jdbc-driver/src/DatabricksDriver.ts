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
import { DriverCapabilities, UnloadOptions, } from '@cubejs-backend/base-driver';
import {
  JDBCDriver,
  JDBCDriverConfiguration,
} from '@cubejs-backend/jdbc-driver';
import { DatabricksQuery } from './DatabricksQuery';
import { downloadJDBCDriver } from './installer';

const { version } = require('../../package.json');

export type DatabricksDriverConfiguration = JDBCDriverConfiguration &
  {
    readOnly?: boolean,
    // common bucket config
    bucketType?: string,
    exportBucket?: string,
    exportBucketMountDir?: string,
    pollInterval?: number,
    // AWS bucket config
    awsKey?: string,
    awsSecret?: string,
    awsRegion?: string,
    // Azure export bucket
    azureKey?: string,
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
    path.join(process.cwd(), 'SparkJDBC42.jar'),
    async () => fileExistsOr(
      path.join(__dirname, '..', 'download', 'SparkJDBC42.jar'),
      async () => {
        const pathOrNull = await downloadJDBCDriver();
        if (pathOrNull) {
          return pathOrNull;
        }
        throw new Error(
          'Please download and place SparkJDBC42.jar inside your ' +
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
      dataSource?: string,
      maxPoolSize?: number,
    } = {},
  ) {
    const dataSource =
      conf.dataSource ||
      assertDataSource('default');

    const config: DatabricksDriverConfiguration = {
      ...conf,
      dbType: 'databricks',
      drivername: 'com.simba.spark.jdbc.Driver',
      customClassPath: undefined,
      properties: {
        // PWD-parameter passed to the connection string has higher priority,
        // so we can set this one to an empty string to avoid a Java error.
        PWD: getEnv('databrickToken', { dataSource }) || '',
        UserAgentEntry: `CubeDev+Cube/${version} (Databricks)`,
      },
      database: getEnv('dbName', { required: false, dataSource }),
      url: getEnv('databrickUrl', { dataSource }),
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
    };
    super(config);
    this.config = config;
  }

  public readOnly() {
    return !!this.config.readOnly;
  }

  public setLogger(logger: any) {
    super.setLogger(logger);
    this.showUrlTokenDeprecation();
  }

  public showUrlTokenDeprecation() {
    if (this.config.url) {
      const result = this.config.url
        .split(';')
        .find(node => /^PWD/i.test(node))
        ?.split('=')[1];

      if (result) {
        this.logger('PWD Parameter Deprecation in connection string', {
          warning: 'PWD parameter is deprecated and will be ignored in future releases. Please migrate to the CUBEJS_DB_DATABRICKS_TOKEN environment variable.'
        });
      }
    }
  }

  /**
   * @override
   */
  protected async getCustomClassPath() {
    return resolveJDBCDriver();
  }

  public async createSchemaIfNotExists(schemaName: string) {
    return this.query(`CREATE SCHEMA IF NOT EXISTS ${schemaName}`, []);
  }

  public quoteIdentifier(identifier: string): string {
    return `\`${identifier}\``;
  }

  public async tableColumnTypes(table: string) {
    const [schema, tableName] = table.split('.');

    const result = [];
    const response: any[] = await this.query(`DESCRIBE ${schema}.${tableName}`, []);

    for (const column of response) {
      // Databricks describe additional info by default after empty line.
      if (column.col_name === '') {
        break;
      }

      result.push({ name: column.col_name, type: this.toGenericType(column.data_type) });
    }

    return result;
  }

  private async queryColumnTypes(sql: string, params: unknown[]) {
    const result = [];
    // eslint-disable-next-line camelcase
    const response = await this.query<{col_name: string; data_type: string}>(`DESCRIBE QUERY ${sql}`, params);

    for (const column of response) {
      // Databricks describe additional info by default after empty line.
      if (column.col_name === '') {
        break;
      }

      result.push({ name: column.col_name, type: this.toGenericType(column.data_type) });
    }

    return result;
  }

  public async getTablesQuery(schemaName: string) {
    const response = await this.query(`SHOW TABLES IN ${this.quoteIdentifier(schemaName)}`, []);

    return response.map((row: any) => ({
      table_name: row.tableName,
    }));
  }

  protected async getTables(): Promise<ShowTableRow[]> {
    if (this.config.database) {
      return <any> this.query<ShowTableRow>(`SHOW TABLES IN ${this.quoteIdentifier(this.config.database)}`, []);
    }

    const databases = await this.query<ShowDatabasesRow>('SHOW DATABASES', []);

    const allTables = await Promise.all(
      databases.map(async ({ databaseName }) => this.query<ShowTableRow>(
        `SHOW TABLES IN ${this.quoteIdentifier(databaseName)}`,
        []
      ))
    );

    return allTables.flat();
  }

  public toGenericType(columnType: string): string {
    return DatabricksToGenericType[columnType.toLowerCase()] || super.toGenericType(columnType);
  }

  public async tablesSchema() {
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
   * Determines whether export bucket feature is configured or no.
   * @returns {boolean}
   */
  public async isUnloadSupported() {
    return this.config.exportBucket !== undefined;
  }

  public async unload(tableName: string, options: UnloadOptions) {
    if (!['azure', 's3'].includes(this.config.bucketType as string)) {
      throw new Error(`Unsupported export bucket type: ${
        this.config.bucketType
      }`);
    }

    const types = options.query ?
      await this.unloadWithSql(tableName, options.query.sql, options.query.params) :
      await this.unloadWithTable(tableName);

    const pathname = `${this.config.exportBucket}/${tableName}.csv`;
    const csvFile = await this.getCsvFiles(
      pathname,
    );

    return {
      csvFile,
      types,
      csvNoHeader: true,
    };
  }

  /**
   * Create table with query and unload it to bucket
   */
  private async unloadWithSql(tableName: string, sql: string, params: unknown[]) {
    const types = await this.queryColumnTypes(sql, params);

    await this.createExternalTableFromSql(tableName, sql, params);
    
    return types;
  }

  /**
   * Create table from preaggregation table with location and unload it to bucket
   */
  private async unloadWithTable(tableName: string) {
    const types = await this.tableColumnTypes(tableName);
    const columns = types.map(t => t.name).join(', ');

    await this.createExternalTableFromTable(tableName, columns);
    
    return types;
  }

  /**
   * return csv files signed URLs array.
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
  private async createExternalTableFromSql(table: string, sql: string, params: unknown[]) {
    await this.query(
      `
      CREATE TABLE ${table}_csv_export
      USING CSV LOCATION '${this.config.exportBucketMountDir || this.config.exportBucket}/${table}.csv'
      OPTIONS (escape = '"')
      AS (${sql})
      `,
      params,
    );
  }

  private async createExternalTableFromTable(table: string, columns: string) {
    await this.query(
      `
      CREATE TABLE ${table}_csv_export
      USING CSV LOCATION '${this.config.exportBucketMountDir || this.config.exportBucket}/${table}.csv'
      OPTIONS (escape = '"')
      AS SELECT ${columns} FROM ${table}
      `,
      [],
    );
  }

  public capabilities(): DriverCapabilities {
    return { unloadWithoutTempTable: true };
  }
}
