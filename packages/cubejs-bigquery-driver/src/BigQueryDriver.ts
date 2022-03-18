/* eslint-disable no-underscore-dangle */
import R from 'ramda';
import { BigQuery, BigQueryOptions, Dataset, Job, QueryRowsResponse } from '@google-cloud/bigquery';
import { Bucket, Storage } from '@google-cloud/storage';
import {
  BaseDriver, DownloadTableCSVData,
  DriverInterface, QueryOptions, StreamTableData,
} from '@cubejs-backend/query-orchestrator';
import { getEnv, pausePromise, Required } from '@cubejs-backend/shared';
import { Table } from '@google-cloud/bigquery/build/src/table';
import { Query } from '@google-cloud/bigquery/build/src/bigquery';
import { HydrationStream } from './HydrationStream';

interface BigQueryDriverOptions extends BigQueryOptions {
  readOnly?: boolean
  projectId?: string,
  keyFilename?: string,
  exportBucket?: string,
  location?: string,
  pollTimeout?: number,
  pollMaxInterval?: number,
}

type BigQueryDriverOptionsInitialized = Required<BigQueryDriverOptions, 'pollTimeout' | 'pollMaxInterval'>;

export class BigQueryDriver extends BaseDriver implements DriverInterface {
  protected readonly options: BigQueryDriverOptionsInitialized;

  protected readonly bigquery: BigQuery;

  protected readonly storage: Storage | null = null;

  protected readonly bucket: Bucket | null = null;

  public constructor(config: BigQueryDriverOptions = {}) {
    super();

    this.options = {
      scopes: ['https://www.googleapis.com/auth/bigquery', 'https://www.googleapis.com/auth/drive'],
      projectId: process.env.CUBEJS_DB_BQ_PROJECT_ID,
      keyFilename: process.env.CUBEJS_DB_BQ_KEY_FILE,
      credentials: process.env.CUBEJS_DB_BQ_CREDENTIALS ?
        JSON.parse(Buffer.from(process.env.CUBEJS_DB_BQ_CREDENTIALS, 'base64').toString('utf8')) :
        undefined,
      exportBucket: getEnv('dbExportBucket') || process.env.CUBEJS_DB_BQ_EXPORT_BUCKET,
      location: getEnv('bigQueryLocation'),
      ...config,
      pollTimeout: (config.pollTimeout || getEnv('dbPollTimeout') || getEnv('dbQueryTimeout')) * 1000,
      pollMaxInterval: (config.pollMaxInterval || getEnv('dbPollMaxInterval')) * 1000,
    };

    getEnv('dbExportBucketType', {
      supported: ['gcp'],
    });

    this.bigquery = new BigQuery(this.options);
    if (this.options.exportBucket) {
      this.storage = new Storage(this.options);
      this.bucket = this.storage.bucket(this.options.exportBucket);
    }
  }

  public static driverEnvVariables() {
    return ['CUBEJS_DB_BQ_PROJECT_ID', 'CUBEJS_DB_BQ_KEY_FILE'];
  }

  public async testConnection() {
    await this.bigquery.query({
      query: 'SELECT ? AS number', params: ['1']
    });
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
        row => R.map(value => (value && value.value && typeof value.value === 'string' ? value.value : value), row)
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

      return [];
    } catch (e) {
      if (e.message.includes('Permission bigquery.tables.get denied on table')) {
        return {};
      }

      throw e;
    }
  }

  public async tablesSchema() {
    const dataSets = await this.bigquery.getDatasets();
    const dataSetsColumns = await Promise.all(
      dataSets[0].map((dataSet) => this.loadTablesForDataset(dataSet))
    );

    return dataSetsColumns.reduce((prev, current) => Object.assign(prev, current), {});
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
      if (e.toString().indexOf('Not found')) {
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

  public async createSchemaIfNotExists(schemaName: string) {
    return this.bigquery.dataset(schemaName).get({ autoCreate: true });
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

  public async unload(table: string): Promise<DownloadTableCSVData> {
    if (!this.bucket) {
      throw new Error('Unload is not configured');
    }

    const destination = this.bucket.file(`${table}-*.csv.gz`);
    const [schema, tableName] = table.split('.');
    const bigQueryTable = this.bigquery.dataset(schema).table(tableName);
    const [job] = await bigQueryTable.createExtractJob(destination, { format: 'CSV', gzip: true });
    await this.waitForJobResult(job, { table }, false);
    const [files] = await this.bucket.getFiles({ prefix: `${table}-` });
    const urls = await Promise.all(files.map(async file => {
      const [url] = await file.getSignedUrl({
        action: 'read',
        expires: new Date(new Date().getTime() + 60 * 60 * 1000)
      });
      return url;
    }));

    return { csvFile: urls };
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
}
