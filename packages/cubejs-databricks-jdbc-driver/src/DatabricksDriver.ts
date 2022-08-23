/* eslint-disable no-restricted-syntax */
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
import {
  DownloadTableCSVData,
} from '@cubejs-backend/query-orchestrator';
import {
  JDBCDriver,
  JDBCDriverConfiguration,
} from '@cubejs-backend/jdbc-driver';
import { getEnv } from '@cubejs-backend/shared';
import { DatabricksQuery } from './DatabricksQuery';
import { downloadJDBCDriver } from './installer';

const { version } = require('../../package.json');

export type DatabricksDriverConfiguration = JDBCDriverConfiguration &
  {
    readOnly?: boolean,
    maxPoolSize?: number,
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

const jdbcDriverResolver: Promise<string> | null = null;

async function resolveJDBCDriver(): Promise<string> {
  if (jdbcDriverResolver) {
    return jdbcDriverResolver;
  }
  return fileExistsOr(
    path.join(process.cwd(), 'SparkJDBC42.jar'),
    async () => fileExistsOr(
      path.join(__dirname, '..', '..', 'download', 'SparkJDBC42.jar'),
      async () => {
        const pathOrNull = await downloadJDBCDriver(false);
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

  public constructor(
    conf: Partial<DatabricksDriverConfiguration>,
  ) {
    const config: DatabricksDriverConfiguration = {
      ...conf,
      drivername: 'com.simba.spark.jdbc.Driver',
      customClassPath: undefined,
      properties: {
        // PWD-parameter passed to the connection string has higher priority,
        // so we can set this one to an empty string to avoid a Java error.
        PWD: getEnv('databrickToken') || '',
        // CUBEJS_DB_DATABRICKS_AGENT is a predefined way to override the user
        // agent for the Cloud application.
        UserAgentEntry: getEnv('databrickAgent') || `CubeDev+Cube/${version} (Databricks)`,
      },
      dbType: 'databricks',
      database: getEnv('dbName', { required: false }),
      url: getEnv('databrickUrl'),
      // common export bucket config
      bucketType:
        conf?.bucketType ||
        getEnv('dbExportBucketType', { supported: ['s3', 'azure'] }),
      exportBucket: conf?.exportBucket || getEnv('dbExportBucket'),
      exportBucketMountDir: conf?.exportBucketMountDir || getEnv('dbExportBucketMountDir'),
      pollInterval: (
        conf?.pollInterval || getEnv('dbPollMaxInterval')
      ) * 1000,
      // AWS export bucket config
      awsKey: conf?.awsKey || getEnv('dbExportBucketAwsKey'),
      awsSecret: conf?.awsSecret || getEnv('dbExportBucketAwsSecret'),
      awsRegion: conf?.awsRegion || getEnv('dbExportBucketAwsRegion'),
      // Azure export bucket
      azureKey: conf?.azureKey || getEnv('dbExportBucketAzureKey'),
    };
    super(config);
    this.config = config;
  }

  public readOnly() {
    return !!this.config.readOnly;
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

  public async getTablesQuery(schemaName: string) {
    const response = await this.query(`SHOW TABLES IN ${this.quoteIdentifier(schemaName)}`, []);

    return response.map((row: any) => ({
      table_name: row.tableName,
    }));
  }

  protected async getTables(): Promise<ShowTableRow[]> {
    if (this.config.database) {
      return <any> this.query(`SHOW TABLES IN ${this.quoteIdentifier(this.config.database)}`, []);
    }

    const databases: ShowDatabasesRow[] = await this.query('SHOW DATABASES', []);

    const allTables: (ShowTableRow[])[] = await Promise.all(
      databases.map(async ({ databaseName }) => this.query(
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

  /**
   * Saves pre-aggs table to the bucket and returns links to download
   * results.
   */
  public async unload(
    tableName: string,
  ): Promise<DownloadTableCSVData> {
    const types = await this.tableColumnTypes(tableName);
    const columns = types.map(t => t.name).join(', ');
    const pathname = `${this.config.exportBucket}/${tableName}.csv`;
    const csvFile = await this.getCsvFiles(
      tableName,
      columns,
      pathname,
    );
    return {
      csvFile,
      types,
      csvNoHeader: true,
    };
  }

  /**
   * Unload table to bucket using Databricks JDBC query and returns (async)
   * csv files signed URLs array.
   */
  private async getCsvFiles(
    table: string,
    columns: string,
    pathname: string,
  ): Promise<string[]> {
    let res;
    switch (this.config.bucketType) {
      case 'azure':
        res = await this.getAzureCsvFiles(table, columns, pathname);
        break;
      case 's3':
        res = await this.getS3CsvFiles(table, columns, pathname);
        break;
      default:
        throw new Error(`Unsupported export bucket type: ${
          this.config.bucketType
        }`);
    }
    return res;
  }

  /**
   * Saves specified table to the Azure blob storage and returns (async)
   * csv files signed URLs array.
   */
  private async getAzureCsvFiles(
    table: string,
    columns: string,
    pathname: string,
  ): Promise<string[]> {
    await this.createExternalTable(table, columns);
    return this.getSignedAzureUrls(pathname);
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
   * Saves specified table to the S3 bucket and returns (async) csv files
   * signed URLs array.
   */
  private async getS3CsvFiles(
    table: string,
    columns: string,
    pathname: string,
  ): Promise<string[]> {
    await this.createExternalTable(table, columns);
    return this.getSignedS3Urls(pathname);
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
  private async createExternalTable(table: string, columns: string,) {
    await this.query(
      `
      CREATE TABLE ${table}_csv_export
      USING CSV LOCATION '${this.config.exportBucketMountDir || this.config.exportBucket}/${table}.csv'
      AS SELECT ${columns} FROM ${table}
      `,
      [],
    );
  }
}
