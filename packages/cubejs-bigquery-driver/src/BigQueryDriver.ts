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

const suffixTableRegex = /^(.*?)([0-9_]+)$/;

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

  protected readonly storage: Storage|null = null;

  protected readonly bucket: Bucket|null = null;

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
      pollTimeout: (config.pollTimeout || getEnv('dbPollTimeout')) * 1000,
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

    this.mapFieldsRecursive = this.mapFieldsRecursive.bind(this);
    this.tablesSchema = this.tablesSchema.bind(this);
    this.parseDataset = this.parseDataset.bind(this);
    this.parseTableData = this.parseTableData.bind(this);
    this.flatten = this.flatten.bind(this);
    this.toObjectFromId = this.toObjectFromId.bind(this);
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

  protected toObjectFromId(accumulator: any, currentElement: any) {
    accumulator[currentElement.id] = currentElement.data;
    return accumulator;
  }

  protected reduceSuffixTables(accumulator: any, currentElement: any) {
    const suffixMatch = currentElement.id.toString().match(suffixTableRegex);
    if (suffixMatch) {
      accumulator.__suffixMatched = accumulator.__suffixMatched || {};
      accumulator.__suffixMatched[suffixMatch[1]] = accumulator.__suffixMatched[suffixMatch[1]] || [];
      accumulator.__suffixMatched[suffixMatch[1]].push(currentElement);
    } else {
      accumulator[currentElement.id] = currentElement.data;
    }
    return accumulator;
  }

  protected addSuffixTables(accumulator: any) {
    // eslint-disable-next-line no-restricted-syntax,guard-for-in
    for (const prefix in accumulator.__suffixMatched) {
      const suffixMatched = accumulator.__suffixMatched[prefix];
      const sorted = suffixMatched.sort((a: any, b: any) => b.toString().localeCompare(a.toString()));
      for (let i = 0; i < Math.min(10, sorted.length); i++) {
        accumulator[sorted[i].id] = sorted[i].data;
      }
    }
    delete accumulator.__suffixMatched;
    return accumulator;
  }

  protected flatten(list: any) {
    return list.reduce(
      (a: any, b: any) => a.concat(Array.isArray(b) ? this.flatten(b) : b), []
    );
  }

  protected mapFieldsRecursive(field: any) {
    if (field.type === 'RECORD') {
      return this.flatten(field.fields.map(this.mapFieldsRecursive)).map(
        (nestedField: any) => ({ name: `${field.name}.${nestedField.name}`, type: nestedField.type })
      );
    }
    return field;
  }

  protected parseDataset(dataset: Dataset) {
    return dataset.getTables().then(
      (data) => Promise.all(data[0].map(this.parseTableData))
        .then(tables => ({ id: dataset.id, data: this.addSuffixTables(tables.reduce(this.reduceSuffixTables, {})) }))
    );
  }

  protected parseTableData(table: Table) {
    return table.getMetadata().then(
      (data) => ({
        id: table.id,
        data: this.flatten(((data[0].schema && data[0].schema.fields) || []).map(this.mapFieldsRecursive))
      })
    );
  }

  public tablesSchema() {
    return this.bigquery.getDatasets().then((data) => Promise.all(data[0].map(this.parseDataset))
      .then(innerData => innerData.reduce(this.toObjectFromId, {})));
  }

  public async getTablesQuery(schemaName: string) {
    try {
      const tables = await this.query(`SELECT * FROM \`${schemaName}.INFORMATION_SCHEMA.TABLES\``, []);
      return tables.map((t: any) => ({ table_name: t.table_name }));
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
