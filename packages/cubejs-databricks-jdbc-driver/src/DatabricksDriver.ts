/* eslint-disable no-restricted-syntax */
import fs from 'fs';
import path from 'path';
import fetch, { Headers, Request } from 'node-fetch';
import { S3, GetObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import {
  DownloadTableCSVData,
} from '@cubejs-backend/query-orchestrator';
import {
  JDBCDriver,
  JDBCDriverConfiguration,
} from '@cubejs-backend/jdbc-driver';
import { getEnv, pausePromise } from '@cubejs-backend/shared';
import { DatabricksQuery } from './DatabricksQuery';
import { downloadJDBCDriver } from './installer';

export type DatabricksDriverConfiguration = JDBCDriverConfiguration &
  {
    readOnly?: boolean,
    accessKeyId?: string,
    secretAccessKey?: string,
    region?: string,
    exportBucket?: string,
    pollMaxInterval?: number,
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
    const pollMaxInterval = (
      configuration?.pollMaxInterval || getEnv('dbPollMaxInterval')
    );

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
      pollMaxInterval,
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
   * Split bucket URL to bucket and path.
   */
  private splitPathname(
    url: string,
  ): {bucket: string, prefix: string} {
    const _url = new URL(url);
    return {
      bucket: _url.host,
      prefix: _url.pathname.slice(1),
    };
  }

  /**
   * Returns IDs of databricks runned clusters.
   */
  private async getClustersIds(): Promise<string[]> {
    const url = `https://${
      this.getApiUrl()
    }/api/1.2/clusters/list`;

    const request = new Request(url, {
      headers: new Headers({
        Accept: '*/*',
        Authorization: `Bearer ${this.getApiToken()}`,
      }),
    });

    const response = await fetch(request);

    if (!response.ok) {
      throw new Error(`Databricks API call error: ${
        response.status
      } - ${
        response.statusText
      }`);
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
   * Returns execution context ("scala" by default) for spesified
   * cluster.
   */
  private async getContextId(
    clusterId: string,
    language = 'scala',
  ): Promise<string> {
    const url = `https://${
      this.getApiUrl()
    }/api/1.2/contexts/create`;

    const request = new Request(url, {
      method: 'POST',
      headers: new Headers({
        Accept: '*/*',
        Authorization: `Bearer ${this.getApiToken()}`,
      }),
      body: JSON.stringify({
        clusterId,
        language,
      }),
    });
    const response = await fetch(request);
    if (!response.ok) {
      throw new Error(`Databricks API call error: ${
        response.status
      } - ${
        response.statusText
      }`);
    }
    const body = await response.json();
    return body.id;
  }

  /**
   * Running specified command.
   */
  private async runCommand(
    clusterId: string,
    contextId: string,
    language: string,
    command: string,
  ): Promise<string> {
    const url = `https://${
      this.getApiUrl()
    }/api/1.2/commands/execute`;
    const request = new Request(url, {
      method: 'POST',
      headers: new Headers({
        Accept: '*/*',
        Authorization: `Bearer ${this.getApiToken()}`,
        'Content-Type': 'application/json',
      }),
      body: JSON.stringify({
        clusterId,
        contextId,
        language,
        command,
      }),
    });
    const response = await fetch(request);
    if (!response.ok) {
      throw new Error(`Databricks API call error: ${
        response.status
      } - ${
        response.statusText
      }`);
    }
    const body = await response.json();
    return body.id;
  }

  /**
   * Resolves command result.
   * TODO: timeout to cancel job?
   */
  private async commandResult(
    clusterId: string,
    contextId: string,
    commandId: string,
  ): Promise<{resultType: string, data: string}> {
    return new Promise((resolve, reject) => {
      const url = `https://${
        this.getApiUrl()
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
          Authorization: `Bearer ${this.getApiToken()}`,
        }),
      });
      fetch(request).then((response) => {
        if (!response.ok) {
          reject();
          throw new Error(`Databricks API call error: ${
            response.status
          } - ${
            response.statusText
          }`);
        }
        response.json().then((body) => {
          if (body.status === 'Finished') {
            resolve(body.results);
          } else if (body.status === 'Error') {
            reject(body.results);
          } else if (body.status === 'Cancelled') {
            reject(body.results);
          } else {
            pausePromise(this.config.pollMaxInterval as number)
              .then(() => {
                this.commandResult(
                  clusterId,
                  contextId,
                  commandId,
                ).then((res) => {
                  resolve(res);
                }).catch((err) => {
                  reject(err);
                });
              });
          }
        });
      });
    });
  }

  /**
   * Unload workflow.
   */
  private async unloadCommand(
    table: string,
    columns: string,
    pathname: string,
  ): Promise<{resultType: string, data: string}> {
    const clusterId = (await this.getClustersIds())[0];
    const contextId = await this.getContextId(clusterId);
    const commandId = await this.runCommand(
      clusterId,
      contextId,
      'scala',
      `
        sc.hadoopConfiguration.set(
          "fs.s3n.awsAccessKeyId", "${this.config.accessKeyId}"
        )
        sc.hadoopConfiguration.set(
          "fs.s3n.awsSecretAccessKey","${this.config.secretAccessKey}"
        )
        sqlContext
          .sql("SELECT ${columns} FROM ${table}")
          .write
          .format("com.databricks.spark.csv")
          .option("header", "false")
          .save("${pathname}")
      `,
    );
    const result = await this.commandResult(
      clusterId,
      contextId,
      commandId,
    );
    return result;
  }

  /**
   * Returns signed temporary URLs for table CSV files.
   */
  private async getSignedCsvUrls(
    pathname: string,
  ): Promise<string[]> {
    const client = new S3({
      credentials: {
        accessKeyId: this.config.accessKeyId as string,
        secretAccessKey: this.config.secretAccessKey as string,
      },
      region: this.config.region,
    });
    const { bucket, prefix } = this.splitPathname(pathname);
    const list = await client.listObjectsV2({
      Bucket: bucket,
      Prefix: prefix,
    });
    if (list.Contents === undefined) {
      throw new Error(`No content in specified path: ${pathname}`);
    }
    const csvFile = await Promise.all(
      list.Contents
        .filter(file => file.Key && /.csv$/i.test(file.Key))
        .map(async (file) => {
          const command = new GetObjectCommand({
            Bucket: bucket,
            Key: file.Key,
          });
          return getSignedUrl(client, command, { expiresIn: 3600 });
        })
    );
    return csvFile;
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
    await this.unloadCommand(tableName, columns, pathname);
    const csvFile = await this.getSignedCsvUrls(pathname);
    return {
      csvFile,
      types,
      csvNoHeader: true,
    };
  }
}
