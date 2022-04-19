/* eslint-disable no-restricted-syntax */
import fs from 'fs';
import path from 'path';
import fetch, { Headers, Request } from 'node-fetch';
import { S3, GetObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import {
  BaseDriver,
  DatabaseStructure,
  DownloadTableCSVData,
  DriverInterface,
  QueryOptions,
  StreamOptions,
  StreamTableData,
  TableName,
} from '@cubejs-backend/query-orchestrator';
import {
  JDBCDriver,
  JDBCDriverConfiguration,
} from '@cubejs-backend/jdbc-driver';
import { getEnv } from '@cubejs-backend/shared';
import { DatabricksQuery } from './DatabricksQuery';
import { downloadJDBCDriver } from './installer';

export type DatabricksDriverConfiguration = JDBCDriverConfiguration &
  {
    readOnly?: boolean,
    accessKeyId?: string,
    secretAccessKey?: string,
    region?: string,
    exportBucket?: string,
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

export class DatabricksDriver extends JDBCDriver {
  protected readonly config: DatabricksDriverConfiguration;

  public static dialectClass() {
    return DatabricksQuery;
  }

  public constructor(
    configuration: Partial<DatabricksDriverConfiguration>,
  ) {
    const accessKeyId =
      configuration?.accessKeyId || process.env.CUBEJS_AWS_KEY;
    const secretAccessKey =
      configuration?.secretAccessKey || process.env.CUBEJS_AWS_SECRET;
    const region =
      configuration?.region || process.env.CUBEJS_AWS_REGION;
    const exportBucket =
      configuration?.exportBucket || getEnv('dbExportBucket');

    const config: DatabricksDriverConfiguration = {
      ...configuration,
      drivername: 'com.simba.spark.jdbc.Driver',
      customClassPath: undefined,
      properties: {},
      dbType: 'databricks',
      database: getEnv('dbName', { required: false }),
      url: getEnv('databrickUrl'),
      // export bucket section
      accessKeyId,
      secretAccessKey,
      region,
      exportBucket,
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
    return this.config.exportBucket !== undefined &&
      this.config.accessKeyId !== undefined &&
      this.config.secretAccessKey !== undefined &&
      this.config.region !== undefined;
  }

  /**
   * Returns databricks base URL.
   */
  private _getBricksUrl(): string {
    return this.config.url.split('/')[2].split(':')[0];
  }

  /**
   * Returns databricks token.
   */
  private _getBricksToken(): string {
    return this.config.url.split('PWD=')[1];
  }

  /**
   * Determines whether S3 bucket mounted to the DBFS or not.
   */
  private async _isBucketMount() {
    const url = `https://${
      this._getBricksUrl()
    }/api/2.0/dbfs/list?path=/mnt/cubejs-bucket`;

    const request = new Request(url, {
      headers: new Headers({
        Accept: '*/*',
        Authorization: `Bearer ${this._getBricksToken()}`,
      }),
    });

    const response = await fetch(request);

    if (!response.ok) {
      if (response.status === 404) {
        return false;
      }
      console.log(response);
      throw new Error(`unexpected response ${response.statusText}`);
    }
    return true;
  }

  /**
   * Returns IDs of bricks clusters with "Running" status.
   */
  private async _getBricksClustersIds(): Promise<string[]> {
    const url = `https://${
      this._getBricksUrl()
    }/api/1.2/clusters/list`;

    const request = new Request(url, {
      headers: new Headers({
        Accept: '*/*',
        Authorization: `Bearer ${this._getBricksToken()}`,
      }),
    });

    const response = await fetch(request);

    if (!response.ok) {
      throw new Error(`unexpected response ${response.statusText}`);
    }

    const body: {
      id: string,
      status: string,
    }[] = await response.json();
    
    return body
      .filter(item => item.status === 'Running')
      .map(item => item.id);
  }

  /**
   * Returns execution context for spesified cluster.
   */
  private async _getBricksContextId(
    clusterId: string,
  ): Promise<string> {
    const url = `https://${
      this._getBricksUrl()
    }/api/1.2/contexts/create`;

    const request = new Request(url, {
      method: 'POST',
      headers: new Headers({
        Accept: '*/*',
        Authorization: `Bearer ${this._getBricksToken()}`,
      }),
      body: JSON.stringify({
        clusterId,
        language: 'python',
      }),
    });
    const response = await fetch(request);
    if (!response.ok) {
      throw new Error(`unexpected response ${response.statusText}`);
    }
    const body = await response.json();
    return body.id;
  }

  /**
   * Starts mounting flow.
   */
  private async _runMountCommand(
    clusterId: string,
    contextId: string,
  ): Promise<string> {
    const url = `https://${
      this._getBricksUrl()
    }/api/1.2/commands/execute`;
    const request = new Request(url, {
      method: 'POST',
      headers: new Headers({
        Accept: '*/*',
        Authorization: `Bearer ${this._getBricksToken()}`,
        'Content-Type': 'application/json',
      }),
      body: JSON.stringify({
        clusterId,
        contextId,
        language: 'python',
        command: `
mount_name = "cubejs-bucket"
access_key = "${this.config.accessKeyId}"
secret_key = "${this.config.secretAccessKey}"
aws_bucket_name = "${this.config.exportBucket}"
encoded_secret_key = secret_key.replace("/", "%2F")
dbutils.fs.mount("s3a://%s:%s@%s" % (access_key, encoded_secret_key, aws_bucket_name), "/mnt/%s" % mount_name)`,
      }),
    });
    const response = await fetch(request);
    if (!response.ok) {
      throw new Error(`unexpected response ${response.statusText}`);
    }
    const body = await response.json();
    return body.id;
  }

  /**
   * Waits until mounting command will be executed.
   */
  private async _waitForMounting(
    clusterId: string,
    contextId: string,
    commandId: string,
  ): Promise<void> {
    return new Promise((resolve, reject) => {
      const url = `https://${
        this._getBricksUrl()
      }/api/1.2/commands/status?clusterId=${
        clusterId
      }&contextId=${
        contextId
      }&commandId=${
        commandId
      }`;
      const request = new Request(url, {
        headers: new Headers({
          Accept: '*/*',
          Authorization: `Bearer ${this._getBricksToken()}`,
        }),
      });
      fetch(request).then((response) => {
        if (!response.ok) {
          reject();
          throw new Error(`unexpected response ${
            response.statusText
          }`);
        }
        response.json().then((body) => {
          if (body.status === 'Finished') {
            resolve();
          } else {
            this._waitForMounting(
              clusterId,
              contextId,
              commandId,
            ).then(() => {
              resolve();
            });
          }
        });
      });
    });
  }

  /**
   * Unload workflow.
   */
  private async _unload(table: string, columns: string) {
    const ids = await this._getBricksClustersIds();
    const promises: Promise<boolean>[] = [];
    ids.forEach((clusterId) => {
      promises.push(new Promise((resolve) => {
        this
          ._getBricksContextId(clusterId)
          .then((contextId) => {
            this
              ._runMountCommand(clusterId, contextId)
              .then((commandId) => {
                this._waitForMounting(
                  clusterId,
                  contextId,
                  commandId,
                ).then(() => {
                  resolve(true);
                });
              });
          });
      }));
    });
    return Promise.all(promises);
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
    const tablePath = `${this.config.exportBucket}/${tableName}`;

    this._unload(tableName, columns);

    return {
      csvFile: [],
      csvNoHeader: true,
    };
  }
}
