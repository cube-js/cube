/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview The `BigQueryDriver` and related types declaration.
 */

import {
  getEnv,
  assertDataSource,
  pausePromise,
  Required,
} from '@cubejs-backend/shared';
import R from 'ramda';
import {
  BigQuery,
  BigQueryOptions,
  Dataset,
  Job,
  QueryRowsResponse,
} from '@google-cloud/bigquery';
import { Bucket, Storage } from '@google-cloud/storage';
import {
  BaseDriver,
  DatabaseStructure,
  DriverCapabilities,
  DriverInterface,
  QueryColumnsResult,
  QueryOptions,
  QuerySchemasResult,
  QueryTablesResult,
  StreamTableData,
  TableCSVData,
} from '@cubejs-backend/base-driver';
import type { Query } from '@google-cloud/bigquery/build/src/bigquery';

import { HydrationStream, transformRow } from './HydrationStream';

interface BigQueryDriverOptions extends BigQueryOptions {
  readOnly?: boolean
  projectId?: string,
  keyFilename?: string,
  exportBucket?: string,
  location?: string,
  pollTimeout?: number,
  pollMaxInterval?: number,

  /**
   * The export bucket CSV file escape symbol.
   */
  exportBucketCsvEscapeSymbol?: string,
}

type BigQueryDriverOptionsInitialized =
  Required<BigQueryDriverOptions, 'pollTimeout' | 'pollMaxInterval'>;

/**
 * BigQuery driver.
 */
export class BigQueryDriver extends BaseDriver implements DriverInterface {
  /**
   * Returns default concurrency value.
   */
  public static getDefaultConcurrency(): number {
    return 10;
  }

  protected readonly options: BigQueryDriverOptionsInitialized;

  protected readonly bigquery: BigQuery;

  protected readonly storage: Storage | null = null;

  protected readonly bucket: Bucket | null = null;

  /**
   * Class constructor.
   */
  public constructor(
    config: BigQueryDriverOptions & {
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

    this.options = {
      scopes: [
        'https://www.googleapis.com/auth/bigquery',
        'https://www.googleapis.com/auth/drive',
      ],
      projectId: getEnv('bigqueryProjectId', { dataSource }),
      keyFilename: getEnv('bigqueryKeyFile', { dataSource }),
      credentials: getEnv('bigqueryCredentials', { dataSource })
        ? JSON.parse(
          Buffer.from(
            getEnv('bigqueryCredentials', { dataSource }),
            'base64',
          ).toString('utf8')
        )
        : undefined,
      exportBucket:
        getEnv('dbExportBucket', { dataSource }) ||
        getEnv('bigqueryExportBucket', { dataSource }),
      location: getEnv('bigqueryLocation', { dataSource }),
      ...config,
      pollTimeout: (
        config.pollTimeout ||
        getEnv('dbPollTimeout', { dataSource }) ||
        getEnv('dbQueryTimeout', { dataSource })
      ) * 1000,
      pollMaxInterval: (
        config.pollMaxInterval ||
        getEnv('dbPollMaxInterval', { dataSource })
      ) * 1000,
      exportBucketCsvEscapeSymbol: getEnv('dbExportBucketCsvEscapeSymbol', { dataSource }),
    };

    getEnv('dbExportBucketType', {
      dataSource,
      supported: ['gcp'],
    });

    this.bigquery = new BigQuery(this.options);
    if (this.options.exportBucket) {
      this.storage = new Storage(this.options);
      this.bucket = this.storage.bucket(this.options.exportBucket);
    }
  }

  public static driverEnvVariables() {
    // TODO (buntarb): check how this method can/must be used with split
    // names by the data source.
    return [
      'CUBEJS_DB_BQ_PROJECT_ID',
      'CUBEJS_DB_BQ_KEY_FILE',
    ];
  }

  public async testConnection() {
    // From the BigQuery Docs:
    // You are not charged for list, get, patch, update and delete calls.
    // Examples include (but are not limited to): listing datasets, updating
    // a dataset's access control list, updating a table's description, or
    // listing user-defined functions in a dataset.
    // @see https://cloud.google.com/bigquery/pricing#free
    await this.bigquery.getDatasets();
  }

  public readOnly() {
    return !!this.options.readOnly;
  }

  public async query<R = unknown>(query: string, values: unknown[], options?: QueryOptions): Promise<R[]> {
    const data = await this.runQueryJob({
      query,
      params: values,
      parameterMode: 'positional',
      useLegacySql: false
    }, options);

    return <any>(
      data[0] && data[0].map(
        row => transformRow(row)
      )
    );
  }

  protected async loadTablesForDataset(dataset: Dataset) {
    try {
      const result = await dataset.query({
        query: `
        SELECT
          columns.column_name as ${this.quoteIdentifier('column_name')},
          columns.table_name as ${this.quoteIdentifier('table_name')},
          columns.table_schema as ${this.quoteIdentifier('table_schema')},
          columns.data_type as ${this.quoteIdentifier('data_type')}
        FROM INFORMATION_SCHEMA.COLUMNS
      `
      });

      if (result.length) {
        return R.reduce(
          this.informationColumnsSchemaReducer, {}, result[0]
        );
      }

      return {};
    } catch (e) {
      if ((<any>e).message.includes('Permission bigquery.tables.get denied on table')) {
        return {};
      }

      throw e;
    }
  }

  public async tablesSchema(): Promise<DatabaseStructure> {
    const dataSets = await this.bigquery.getDatasets();
    const dataSetsColumns = await Promise.all(
      dataSets[0].map((dataSet) => this.loadTablesForDataset(dataSet))
    );

    return dataSetsColumns.reduce((prev, current) => Object.assign(prev, current), {});
  }

  public override async getSchemas(): Promise<QuerySchemasResult[]> {
    const dataSets = await this.bigquery.getDatasets();
    return dataSets[0].filter((dataSet) => dataSet.id).map((dataSet) => ({
      schema_name: dataSet.id!,
    }));
  }

  public override async getTablesForSpecificSchemas(schemas: QuerySchemasResult[]): Promise<QueryTablesResult[]> {
    try {
      const allTablePromises = schemas.map(async schema => {
        const tables = await this.getTablesQuery(schema.schema_name);
        return tables
          .filter(table => table.table_name)
          .map(table => ({ schema_name: schema.schema_name, table_name: table.table_name! }));
      });

      const allTables = await Promise.all(allTablePromises);

      return allTables.flat();
    } catch (e) {
      console.error('Error fetching tables for schemas:', e);
      throw e;
    }
  }

  public override async getColumnsForSpecificTables(tables: QueryTablesResult[]): Promise<QueryColumnsResult[]> {
    try {
      const allColumnPromises = tables.map(async table => {
        const tableName = `${table.schema_name}.${table.table_name}`;
        const columns = await this.tableColumnTypes(tableName);
        return columns.map((column: any) => ({
          schema_name: table.schema_name,
          table_name: table.table_name,
          data_type: column.type,
          column_name: column.name,
        }));
      });

      const allColumns = await Promise.all(allColumnPromises);

      return allColumns.flat();
    } catch (e) {
      console.error('Error fetching columns for tables:', e);
      throw e;
    }
  }

  public async getTablesQuery(schemaName: string) {
    try {
      const dataSet = await this.bigquery.dataset(schemaName);
      if (!dataSet) {
        return [];
      }
      const [tables] = await this.bigquery.dataset(schemaName).getTables();
      return tables.map(t => ({ table_name: t.id }));
    } catch (e) {
      if ((<any>e).toString().indexOf('Not found')) {
        return [];
      }
      throw e;
    }
  }

  public async tableColumnTypes(table: string) {
    const [schema, name] = table.split('.');
    const [bigQueryTable] = await this.bigquery.dataset(schema).table(name).getMetadata();
    return bigQueryTable.schema.fields.map((c: any) => ({ name: c.name, type: this.toGenericType(c.type) }));
  }

  public async createSchemaIfNotExists(schemaName: string): Promise<void> {
    await this.bigquery.dataset(schemaName).get({ autoCreate: true });
  }

  public async isUnloadSupported() {
    return this.bucket !== null;
  }

  public async stream(
    query: string,
    values: unknown[]
  ): Promise<StreamTableData> {
    const stream = await this.bigquery.createQueryStream({
      query,
      params: values,
      parameterMode: 'positional',
      useLegacySql: false
    });

    const rowStream = new HydrationStream();
    stream.pipe(rowStream);

    return {
      rowStream,
    };
  }

  public async unload(table: string): Promise<TableCSVData> {
    if (!this.bucket) {
      throw new Error('Unload is not configured');
    }

    const destination = this.bucket.file(`${table}-*.csv.gz`);
    const [schema, tableName] = table.split('.');
    const bigQueryTable = this.bigquery.dataset(schema).table(tableName);
    const [job] = await bigQueryTable.createExtractJob(destination, { format: 'CSV', gzip: true });
    await this.waitForJobResult(job, { table }, false);
    // There is an implementation for extracting and signing urls from S3
    // @see BaseDriver->extractUnloadedFilesFromS3()
    // Please use that if you need. Here is a different flow
    // because bigquery requires storage/bucket object for other things,
    // and there is no need to initiate another one (created in extractUnloadedFilesFromS3()).
    const [files] = await this.bucket.getFiles({ prefix: `${table}-` });
    const urls = await Promise.all(files.map(async file => {
      const [url] = await file.getSignedUrl({
        action: 'read',
        expires: new Date(new Date().getTime() + 60 * 60 * 1000),
      });
      return url;
    }));

    return {
      exportBucketCsvEscapeSymbol: this.options.exportBucketCsvEscapeSymbol,
      csvFile: urls,
    };
  }

  public async loadPreAggregationIntoTable(
    preAggregationTableName: string,
    loadSql: string,
    params: any,
    options: any
  ): Promise<any> {
    const [dataSet, tableName] = preAggregationTableName.split('.');

    const bigQueryQuery: Query = {
      query: loadSql,
      params,
      parameterMode: 'positional',
      destination: this.bigquery.dataset(dataSet).table(tableName),
      createDisposition: 'CREATE_IF_NEEDED',
      useLegacySql: false
    };

    return this.runQueryJob(bigQueryQuery, options, false);
  }

  protected async awaitForJobStatus(job: Job, options: any, withResults: boolean) {
    const [result] = await job.getMetadata();
    if (result.status && result.status.state === 'DONE') {
      if (result.status.errorResult) {
        throw new Error(
          result.status.errorResult.message ?
            result.status.errorResult.message :
            JSON.stringify(result.status.errorResult)
        );
      }
      this.reportQueryUsage(result.statistics, options);
    } else {
      return null;
    }

    return withResults ? job.getQueryResults() : true;
  }

  protected async runQueryJob<T = QueryRowsResponse>(
    bigQueryQuery: Query,
    options: any,
    withResults: boolean = true
  ): Promise<T> {
    const [job] = await this.bigquery.createQueryJob(bigQueryQuery);
    return <any> this.waitForJobResult(job, options, withResults);
  }

  protected async waitForJobResult(job: Job, options: any, withResults: boolean) {
    const startedTime = Date.now();

    for (let i = 0; Date.now() - startedTime <= this.options.pollTimeout; i++) {
      const result = await this.awaitForJobStatus(job, options, withResults);
      if (result) {
        return result;
      }

      await pausePromise(
        Math.min(this.options.pollMaxInterval, 200 * i),
      );
    }

    await job.cancel();

    throw new Error(
      `BigQuery job timeout reached ${this.options.pollTimeout}ms`,
    );
  }

  public quoteIdentifier(identifier: string) {
    const nestedFields = identifier.split('.');
    return nestedFields.map(name => {
      if (name.match(/^[a-z0-9_]+$/)) {
        return name;
      }
      return `\`${identifier}\``;
    }).join('.');
  }

  public capabilities(): DriverCapabilities {
    return {
      incrementalSchemaLoading: true,
    };
  }
}
