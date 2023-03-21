/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview The `AthenaDriver` and related types declaration.
 */

import {
  getEnv,
  assertDataSource,
  checkNonNullable,
  pausePromise,
  Required,
} from '@cubejs-backend/shared';
import { Athena, GetQueryResultsCommandOutput, StartQueryExecutionCommandInput } from '@aws-sdk/client-athena';
import { S3, GetObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import * as stream from 'stream';
import {
  BaseDriver, DatabaseStructure,
  DownloadTableCSVData,
  DriverInterface,
  QueryOptions, StreamOptions,
  StreamTableData
} from '@cubejs-backend/base-driver';
import * as SqlString from 'sqlstring';
import { AthenaClientConfig } from '@aws-sdk/client-athena/dist-types/AthenaClient';
import { URL } from 'url';

interface AthenaDriverOptions extends AthenaClientConfig {
  readOnly?: boolean
  accessKeyId?: string
  secretAccessKey?: string
  workGroup?: string
  catalog?: string
  schema?: string
  S3OutputLocation?: string
  exportBucket?: string
  pollTimeout?: number
  pollMaxInterval?: number

  /**
   * The export bucket CSV file escape symbol.
   */
  exportBucketCsvEscapeSymbol?: string
}

type AthenaDriverOptionsInitialized = Required<AthenaDriverOptions, 'pollTimeout' | 'pollMaxInterval'>;

export interface AthenaQueryId {
  QueryExecutionId: string;
}

function applyParams(query: string, params: any[]): string {
  return SqlString.format(query, params);
}

interface AthenaTable {
  schema: string
  name: string
}

export class AthenaDriver extends BaseDriver implements DriverInterface {
  /**
   * Returns default concurrency value.
   */
  public static getDefaultConcurrency(): number {
    return 5;
  }

  private config: AthenaDriverOptionsInitialized;

  private athena: Athena;

  private schema: string;

  /**
   * Class constructor.
   */
  public constructor(
    config: AthenaDriverOptions & {
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
    super({
      testConnectionTimeout: config.testConnectionTimeout,
    });

    const dataSource =
      config.dataSource ||
      assertDataSource('default');

    const accessKeyId =
      config.accessKeyId ||
      getEnv('athenaAwsKey', { dataSource });

    const secretAccessKey =
      config.secretAccessKey ||
      getEnv('athenaAwsSecret', { dataSource });

    const { schema, ...restConfig } = config;

    this.schema = schema ||
      getEnv('dbName', { dataSource }) ||
      getEnv('dbSchema', { dataSource });

    this.config = {
      ...restConfig,
      credentials: accessKeyId && secretAccessKey
        ? { accessKeyId, secretAccessKey }
        : undefined,
      region:
        config.region ||
        getEnv('athenaAwsRegion', { dataSource }),
      S3OutputLocation:
        config.S3OutputLocation ||
        getEnv('athenaAwsS3OutputLocation', { dataSource }),
      workGroup:
        config.workGroup ||
        getEnv('athenaAwsWorkgroup', { dataSource }) ||
        'primary',
      catalog:
        config.catalog ||
        getEnv('athenaAwsCatalog', { dataSource }),
      exportBucket:
        config.exportBucket ||
        getEnv('dbExportBucket', { dataSource }),
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
    if (this.config.exportBucket) {
      this.config.exportBucket = AthenaDriver.normalizeS3Path(this.config.exportBucket);
    }

    this.athena = new Athena(this.config);
  }

  public readOnly(): boolean {
    return !!this.config.readOnly;
  }

  public async isUnloadSupported() {
    return this.config.exportBucket !== undefined;
  }

  public async testConnection() {
    await this.athena.getWorkGroup({
      WorkGroup: this.config.workGroup
    });
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  public async query<R = unknown>(query: string, values: unknown[], options?: QueryOptions): Promise<R[]> {
    const qid = await this.startQuery(query, values);
    await this.waitForSuccess(qid);
    const rows: R[] = [];
    for await (const row of this.lazyRowIterator<R>(qid, query)) {
      rows.push(row);
    }
    return rows;
  }

  public async stream(query: string, values: unknown[], options: StreamOptions): Promise<StreamTableData> {
    const qid = await this.startQuery(query, values);
    await this.waitForSuccess(qid);
    const rowStream = stream.Readable.from(this.lazyRowIterator(qid, query), { highWaterMark: options.highWaterMark });
    return {
      rowStream
    };
  }

  public async loadPreAggregationIntoTable(
    preAggregationTableName: string,
    loadSql: string,
    params: any,
  ): Promise<any> {
    if (this.config.S3OutputLocation === undefined) {
      throw new Error('Unload is not configured. Please define CUBEJS_AWS_S3_OUTPUT_LOCATION env var ');
    }

    const qid = await this.startQuery(loadSql, params);
    await this.waitForSuccess(qid);
  }

  public async unload(tableName: string): Promise<DownloadTableCSVData> {
    if (this.config.exportBucket === undefined) {
      throw new Error('Unload is not configured');
    }

    const types = await this.tableColumnTypes(tableName);
    const columns = types.map(t => t.name).join(', ');
    const path = `${this.config.exportBucket}/${tableName}`;

    const unloadSql = `
      UNLOAD (SELECT ${columns} FROM ${tableName})
      TO '${path}'
      WITH (format = 'TEXTFILE', field_delimiter = ',', compression='GZIP')
    `;
    const qid = await this.startQuery(unloadSql, []);
    await this.waitForSuccess(qid);

    const client = new S3({
      credentials: this.config.credentials,
      region: this.config.region,
    });
    const { bucket, prefix } = AthenaDriver.splitS3Path(path);
    const list = await client.listObjectsV2({
      Bucket: bucket,
      // skip leading /
      Prefix: prefix.slice(1),
    });
    if (list.Contents === undefined) {
      return {
        exportBucketCsvEscapeSymbol: this.config.exportBucketCsvEscapeSymbol,
        csvFile: [],
        types,
      };
    }
    const csvFile = await Promise.all(
      list.Contents.map(async (file) => {
        const command = new GetObjectCommand({
          Bucket: bucket,
          Key: file.Key,
        });
        return getSignedUrl(client, command, { expiresIn: 3600 });
      })
    );

    return {
      exportBucketCsvEscapeSymbol: this.config.exportBucketCsvEscapeSymbol,
      csvFile,
      types,
      csvNoHeader: true
    };
  }

  public informationSchemaQuery() {
    if (this.schema) {
      return `${super.informationSchemaQuery()} AND columns.table_schema = '${this.schema}'`;
    }
    return super.informationSchemaQuery();
  }

  public async tablesSchema(): Promise<DatabaseStructure> {
    const tablesSchema = await super.tablesSchema();
    const viewsSchema = await this.viewsSchema(tablesSchema);

    return this.mergeSchemas([tablesSchema, viewsSchema]);
  }

  protected async startQuery(query: string, values: unknown[]): Promise<AthenaQueryId> {
    const queryString = applyParams(
      query,
      (values || []).map(s => (typeof s === 'string' ? {
        toSqlString: () => SqlString.escape(s).replace(/\\\\([_%])/g, '\\$1').replace(/\\'/g, '\'\'')
      } : s))
    );
    const request: StartQueryExecutionCommandInput = {
      QueryString: queryString,
      WorkGroup: this.config.workGroup,
      ResultConfiguration: {
        OutputLocation: this.config.S3OutputLocation
      },
      ...(this.config.catalog != null ? { QueryExecutionContext: { Catalog: this.config.catalog } } : {})
    };
    const { QueryExecutionId } = await this.athena.startQueryExecution(request);
    return { QueryExecutionId: checkNonNullable('StartQueryExecution', QueryExecutionId) };
  }

  protected async checkStatus(qid: AthenaQueryId): Promise<boolean> {
    const queryExecution = await this.athena.getQueryExecution(qid);

    const status = queryExecution.QueryExecution?.Status?.State;
    if (status === 'FAILED') {
      throw new Error(queryExecution.QueryExecution?.Status?.StateChangeReason);
    }

    if (status === 'CANCELLED') {
      throw new Error('Query has been cancelled');
    }

    return status === 'SUCCEEDED';
  }

  protected async waitForSuccess(qid: AthenaQueryId): Promise<void> {
    const startedTime = Date.now();
    for (let i = 0; Date.now() - startedTime <= this.config.pollTimeout; i++) {
      if (await this.checkStatus(qid)) {
        return;
      }
      await pausePromise(
        Math.min(this.config.pollMaxInterval, 500 * i)
      );
    }
    throw new Error(
      `Athena job timeout reached ${this.config.pollTimeout}ms`
    );
  }

  protected async* lazyRowIterator<R extends unknown>(qid: AthenaQueryId, query: string): AsyncGenerator<R> {
    let isFirstBatch = true;
    let columnInfo: { Name: string }[] = [];
    for (
      let results: GetQueryResultsCommandOutput | undefined = await this.athena.getQueryResults(qid);
      results;
      results = results.NextToken
        ? (await this.athena.getQueryResults({ ...qid, NextToken: results.NextToken }))
        : undefined
    ) {
      let rows = results.ResultSet?.Rows ?? [];
      if (isFirstBatch) {
        isFirstBatch = false;
        // Athena returns the columns names in first row, skip it.
        rows = rows.slice(1);
        columnInfo = /SHOW COLUMNS/.test(query) // Fix for getColumns method
          ? [{ Name: 'column' }]
          : checkNonNullable('ColumnInfo', results.ResultSet?.ResultSetMetadata?.ColumnInfo)
            .map(info => ({ Name: checkNonNullable('Name', info.Name) }));
      }

      for (const row of rows) {
        const fields: Record<string, any> = {};
        columnInfo
          .forEach((c, j) => {
            fields[c.Name] = row.Data?.[j].VarCharValue;
          });
        yield fields as R;
      }
    }
  }

  protected async viewsSchema(tablesSchema: DatabaseStructure): Promise<DatabaseStructure> {
    const isView = (table: AthenaTable) => !tablesSchema[table.schema]
      || !tablesSchema[table.schema][table.name];

    const allTables = await this.getAllTables();
    const arrViewsSchema = await Promise.all(
      allTables
        .filter(isView)
        .map(table => this.getColumns(table))
    );

    return this.mergeSchemas(arrViewsSchema);
  }

  protected async getAllTables(): Promise<AthenaTable[]> {
    let allTablesQuery = `
      SELECT table_schema AS schema, table_name AS name
      FROM information_schema.tables
      WHERE tables.table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
    `;
    if (this.schema) {
      allTablesQuery = `${allTablesQuery} AND tables.table_schema = '${this.schema}'`;
    }
    const rows = await this.query(
      allTablesQuery,
      []
    );

    return rows as AthenaTable[];
  }

  protected async getColumns(table: AthenaTable): Promise<DatabaseStructure> {
    const data: { column: string }[] = await this.query(`SHOW COLUMNS IN \`${table.schema}\`.\`${table.name}\``, []);

    return {
      [table.schema]: {
        [table.name]: data.map(({ column }) => {
          const [name, type] = column.split('\t');
          return { name, type, attributes: [] };
        })
      }
    };
  }

  protected mergeSchemas(arrSchemas: DatabaseStructure[]): DatabaseStructure {
    const result: DatabaseStructure = {};

    arrSchemas.forEach(schemas => {
      Object.keys(schemas).forEach(schema => {
        Object.keys(schemas[schema]).forEach((name) => {
          if (!result[schema]) result[schema] = {};
          if (!result[schema][name]) result[schema][name] = schemas[schema][name];
        });
      });
    });

    return result;
  }

  public static normalizeS3Path(path: string): string {
    // Remove trailing /
    path = path.replace(/\/+$/, '');
    // Prepend s3:// prefix to plain bucket
    if (!path.startsWith('s3://')) {
      return `s3://${path}`;
    }
    return path;
  }

  public static splitS3Path(path: string) {
    const url = new URL(path);
    return {
      bucket: url.host,
      prefix: url.pathname
    };
  }
}
