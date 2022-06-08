/* eslint-disable no-restricted-syntax */
import fs from 'fs';
import path from 'path';
import fetch, { Headers, Request, Response } from 'node-fetch';
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
import { getEnv, pausePromise, CancelablePromise } from '@cubejs-backend/shared';
import { v1, v5 } from 'uuid';
import { DatabricksQuery } from './DatabricksQuery';
import { downloadJDBCDriver } from './installer';

export type DatabricksDriverConfiguration = JDBCDriverConfiguration &
  {
    readOnly?: boolean,
    // common bucket config
    bucketType?: string,
    exportBucket?: string,
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

  public constructor(
    conf: Partial<DatabricksDriverConfiguration>,
  ) {
    const config: DatabricksDriverConfiguration = {
      ...conf,
      drivername: 'com.simba.spark.jdbc.Driver',
      customClassPath: undefined,
      properties: {},
      dbType: 'databricks',
      database: getEnv('dbName', { required: false }),
      url: getEnv('databrickUrl'),
      // common export bucket config
      bucketType:
        conf?.bucketType ||
        getEnv('dbExportBucketType', { supported: ['s3', 'azure'] }),
      exportBucket: conf?.exportBucket || getEnv('dbExportBucket'),
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
   * Returns databricks API base URL.
   */
  private getApiUrl(): string {
    let res: string;
    try {
      // eslint-disable-next-line prefer-destructuring
      res = this.config.url
        .split(';')
        .filter(node => /^jdbc/i.test(node))[0]
        .split('/')[2]
        .split(':')[0];
    } catch (e) {
      res = '';
    }
    if (!res.length) {
      throw new Error(
        `Error parsing API URL from the CUBEJS_DB_DATABRICKS_URL = ${
          this.config.url
        }`
      );
    }
    return res;
  }

  /**
   * Returns databricks API token.
   */
  private getApiToken(): string {
    let res: string;
    try {
      // eslint-disable-next-line prefer-destructuring
      res = this.config.url
        .split(';')
        .filter(node => /^PWD/i.test(node))[0]
        .split('=')[1];
    } catch (e) {
      res = '';
    }
    if (!res.length) {
      throw new Error(
        'Error parsing API token from the CUBEJS_DB_DATABRICKS_URL' +
        ` = ${this.config.url}`
      );
    }
    return res;
  }

  /**
   * Sleeper method.
   */
  private wait(ms: number): CancelablePromise<void> {
    return pausePromise(ms);
  }

  /**
   * Assert http response.
   */
  private async assertResponse(response: Response): Promise<void> {
    if (!response.ok) {
      const text = await response.text();
      throw new Error(`Databricks API call error: ${
        response.status
      } - ${
        response.statusText
      } - ${
        text
      }`);
    }
  }

  /**
   * Fetch API wrapper.
   */
  private async fetch(req: Request, count?: number, ms?: number): Promise<Response> {
    count = count || 0;
    ms = ms || 0;
    return new Promise((resolve, reject) => {
      this
        .wait(ms as number)
        .then(() => {
          fetch(req)
            .then((res) => {
              this
                .assertResponse(res)
                .then(() => { resolve(res); })
                .catch((err) => {
                  if (res.status === 429 && (count as number) < 5) {
                    this
                      .fetch(req, (count as number)++, (ms as number) + 1000)
                      .then((_res) => { resolve(_res); })
                      .catch((_err) => { reject(_err); });
                  } else {
                    reject(err);
                  }
                });
            });
        });
    });
  }

  /**
   * Returns IDs of databricks runned clusters.
   */
  private async getClustersIds(): Promise<string[]> {
    const url = `https://${
      this.getApiUrl()
    }/api/2.0/clusters/list`;

    const request = new Request(url, {
      headers: new Headers({
        Accept: '*/*',
        Authorization: `Bearer ${this.getApiToken()}`,
      }),
    });

    const response = await this.fetch(request);
    
    const body: {
      clusters: {
      // eslint-disable-next-line camelcase
        cluster_id: string,
        state: string,
      }[],
    } = await response.json();
    
    return body.clusters
      .filter(item => item.state === 'RUNNING')
      .map(item => item.cluster_id);
  }

  /**
   * Import predefined nodebook to the databricks under specified path.
   */
  private async importNotebook(p: string, content: string): Promise<void> {
    const url = `https://${
      this.getApiUrl()
    }/api/2.0/workspace/import`;
    const request = new Request(url, {
      method: 'POST',
      headers: new Headers({
        Accept: '*/*',
        Authorization: `Bearer ${this.getApiToken()}`,
      }),
      body: JSON.stringify({
        format: 'SOURCE',
        language: 'SCALA',
        overwrite: true,
        content,
        path: p,
      }),
    });
    await this.fetch(request);
  }

  /**
   * Create job and returns job id.
   */
  private async createJob(cluster: string, p: string): Promise<number> {
    const url = `https://${
      this.getApiUrl()
    }/api/2.0/jobs/create`;
    const request = new Request(url, {
      method: 'POST',
      headers: new Headers({
        Accept: '*/*',
        Authorization: `Bearer ${this.getApiToken()}`,
      }),
      body: JSON.stringify({
        existing_cluster_id: cluster,
        notebook_task: {
          notebook_path: p,
        },
      }),
    });
    const response = await this.fetch(request);
    const body: {
      // eslint-disable-next-line camelcase
      job_id: number,
    } = await response.json();
    return body.job_id;
  }

  /**
   * Run job and returns run id.
   */
  private async runJob(job: number): Promise<number> {
    const url = `https://${
      this.getApiUrl()
    }/api/2.0/jobs/run-now`;
    const request = new Request(url, {
      method: 'POST',
      headers: new Headers({
        Accept: '*/*',
        Authorization: `Bearer ${this.getApiToken()}`,
      }),
      body: JSON.stringify({
        job_id: job,
      }),
    });
    const response = await this.fetch(request);
    const body: {
      // eslint-disable-next-line camelcase
      run_id: number,
    } = await response.json();
    return body.run_id;
  }

  /**
   * Pooling databricks until run in progress and resolve when it's done.
   */
  private async waitResult(run: number, ms?: number): Promise<any> {
    ms = ms || 1000;
    ms = ms <= 10000 ? ms + 1000 : ms;
    return new Promise((resolve, reject) => {
      const url = `https://${
        this.getApiUrl()
      }/api/2.0/jobs/runs/get?run_id=${run}`;
      const request = new Request(url, {
        headers: new Headers({
          Accept: '*/*',
          Authorization: `Bearer ${this.getApiToken()}`,
        }),
      });
      this
        .wait(ms as number)
        .then(() => {
          this
            .fetch(request)
            .then((response) => {
              response
                .json()
                .then((body: {
                  state: {
                    // eslint-disable-next-line camelcase
                    life_cycle_state: string,
                    // eslint-disable-next-line camelcase
                    result_state: string,
                  },
                }) => {
                  const { state } = body;
                  if (
                    state.life_cycle_state === 'TERMINATED' &&
                    state.result_state === 'SUCCESS'
                  ) {
                    resolve(state.result_state);
                  } else if (
                    state.life_cycle_state === 'INTERNAL_ERROR' ||
                    state.result_state === 'FAILED' ||
                    state.result_state === 'TIMEDOUT' ||
                    state.result_state === 'CANCELED'
                  ) {
                    reject(state.result_state);
                  } else {
                    this
                      .waitResult(run, ms)
                      .then((res) => { resolve(res); })
                      .catch((err) => { reject(err); });
                  }
                });
            });
        });
    });
  }

  /**
   * Delete job.
   */
  private async deleteJob(job: number): Promise<any> {
    const url = `https://${
      this.getApiUrl()
    }/api/2.0/jobs/delete`;
    const request = new Request(url, {
      method: 'POST',
      headers: new Headers({
        Accept: '*/*',
        Authorization: `Bearer ${this.getApiToken()}`,
      }),
      body: JSON.stringify({
        job_id: job,
      }),
    });
    await this.fetch(request);
  }

  /**
   * Remove nodebook.
   */
  private async deleteNotebook(p: string): Promise<any> {
    const url = `https://${
      this.getApiUrl()
    }/api/2.0/workspace/delete`;
    const request = new Request(url, {
      method: 'POST',
      headers: new Headers({
        Accept: '*/*',
        Authorization: `Bearer ${this.getApiToken()}`,
      }),
      body: JSON.stringify({
        path: p,
        recursive: true,
      }),
    });
    await this.fetch(request);
  }

  /**
   * Returns signed temporary URLs for AWS S3 objects.
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
    return csvFile;
  }

  /**
   * Unload to AWS S3 bucket.
   */
  private async unloadS3Command(
    table: string,
    columns: string,
    pathname: string,
  ): Promise<string[]> {
    let result: string[] = [];
    let notebook = true;
    const filename = `/${v5(pathname, v1()).toString()}.scala`;
    const content = Buffer.from(
      `sc.hadoopConfiguration.set(
        "fs.s3n.awsAccessKeyId", "${this.config.awsKey}"
      )
      sc.hadoopConfiguration.set(
        "fs.s3n.awsSecretAccessKey","${this.config.awsSecret}"
      )
      sqlContext
        .sql("SELECT ${columns} FROM ${table}")
        .write
        .format("com.databricks.spark.csv")
        .option("header", "false")
        .save("${pathname}")`,
      'utf-8',
    ).toString('base64');
    const cluster = (await this.getClustersIds())[0];
    try {
      await this.importNotebook(filename, content);
    } catch (e) {
      notebook = false;
    }
    if (notebook) {
      try {
        const job = await this.createJob(cluster, filename);
        const run = await this.runJob(job);
        await this.waitResult(run);
        await this.deleteJob(job);
        result = await this.getSignedS3Urls(pathname);
      } finally {
        await this.deleteNotebook(filename);
      }
    }
    return result;
  }

  /**
   * Returns signed temporary URLs for Azure container objects.
   */
  private async getSignedWasbsUrls(
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
    const blobsList = containerClient.listBlobsFlat();
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
    return csvFile;
  }

  /**
   * Unload to Azure Blob Container bucket.
   */
  private async unloadWasbsCommand(
    table: string,
    columns: string,
    pathname: string,
  ): Promise<string[]> {
    let result: string[] = [];
    let notebook = true;
    const filename = `/${v5(pathname, v1()).toString()}.scala`;
    const storage = pathname.split('@')[1].split('.')[0];
    const content = Buffer.from(
      `spark.conf.set(
        "fs.azure.account.key.${storage}.blob.core.windows.net",
        "${this.config.azureKey}"
      )
      sqlContext
        .sql("SELECT ${columns} FROM ${table}")
        .write
        .format("com.databricks.spark.csv")
        .option("header", "false")
        .save("${pathname}")`,
      'utf-8',
    ).toString('base64');
    // TODO: if there is no cluster should we create new one?
    const cluster = (await this.getClustersIds())[0];
    try {
      await this.importNotebook(filename, content);
    } catch (e) {
      notebook = false;
    }
    if (notebook) {
      try {
        const job = await this.createJob(cluster, filename);
        const run = await this.runJob(job);
        await this.waitResult(run);
        await this.deleteJob(job);
        result = await this.getSignedWasbsUrls(pathname);
      } finally {
        await this.deleteNotebook(filename);
      }
    }
    return result;
  }

  /**
   * Unload table to bucket.
   */
  private async unloadCommand(
    table: string,
    columns: string,
    pathname: string,
  ): Promise<string[]> {
    let res;
    switch (this.config.bucketType) {
      case 's3':
        res = await this.unloadS3Command(table, columns, pathname);
        break;
      case 'azure':
        res = await this.unloadWasbsCommand(table, columns, pathname);
        break;
      default:
        throw new Error(`Unsupported export bucket type: ${
          this.config.bucketType
        }`);
    }
    return res;
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
    const csvFile = await this.unloadCommand(
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
}
