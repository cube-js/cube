import { Athena, GetQueryResultsCommandOutput } from '@aws-sdk/client-athena';
import { S3, GetObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import * as stream from 'stream';
import {
  BaseDriver,
  DownloadTableCSVData,
  DriverInterface,
  QueryOptions, StreamOptions,
  StreamTableData
} from '@cubejs-backend/query-orchestrator';
import { checkNonNullable, getEnv, pausePromise, Required } from '@cubejs-backend/shared';
import * as SqlString from 'sqlstring';
import { AthenaClientConfig } from '@aws-sdk/client-athena/dist-types/AthenaClient';
import { URL } from 'url';

interface AthenaDriverOptions extends AthenaClientConfig {
  readOnly?: boolean
  accessKeyId?: string
  secretAccessKey?: string
  workGroup?: string
  S3OutputLocation?: string
  pollTimeout?: number
  pollMaxInterval?: number
}

type AthenaDriverOptionsInitialized = Required<AthenaDriverOptions, 'pollTimeout' | 'pollMaxInterval'>;

export interface AthenaQueryId {
  QueryExecutionId: string;
}

interface AthenaTable {
  schema: string
  name: string
}

interface AthenaColumn {
  name: string
  type: string
  attributes: string[]
}

type AthenaSchema = Record<string, Record<string, AthenaColumn[]>>;

function applyParams(query: string, params: any[]): string {
  return SqlString.format(query, params);
}

export class AthenaDriver extends BaseDriver implements DriverInterface {
  private config: AthenaDriverOptionsInitialized;

  private athena: Athena;

  public constructor(config: AthenaDriverOptions = {}) {
    super();

    const accessKeyId = config.accessKeyId || process.env.CUBEJS_AWS_KEY;
    const secretAccessKey = config.secretAccessKey || process.env.CUBEJS_AWS_SECRET;

    this.config = {
      credentials: accessKeyId && secretAccessKey ? { accessKeyId, secretAccessKey } : undefined,
      region: config.region || process.env.CUBEJS_AWS_REGION,
      S3OutputLocation: config.S3OutputLocation || process.env.CUBEJS_AWS_S3_OUTPUT_LOCATION,
      workGroup: config.workGroup || process.env.CUBEJS_AWS_ATHENA_WORKGROUP || 'primary',
      ...config,
      pollTimeout: (config.pollTimeout || getEnv('dbPollTimeout') || getEnv('dbQueryTimeout')) * 1000,
      pollMaxInterval: (config.pollMaxInterval || getEnv('dbPollMaxInterval')) * 1000,
    };

    this.athena = new Athena(this.config);
  }

  public readOnly(): boolean {
    return !!this.config.readOnly;
  }

  public async isUnloadSupported() {
    return this.config.S3OutputLocation !== undefined;
  }

  public async testConnection() {
    await this.athena.getWorkGroup({
      WorkGroup: this.config.workGroup
    });
  }

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
      throw new Error('Unload is not configured');
    }

    const qid = await this.startQuery(loadSql, params);
    await this.waitForSuccess(qid);
  }

  public async unload(tableName: string): Promise<DownloadTableCSVData> {
    if (this.config.S3OutputLocation === undefined) {
      throw new Error('Unload is not configured');
    }

    const path = `${this.config.S3OutputLocation}/${tableName}`;

    const unloadSql = `
      UNLOAD (SELECT * FROM ${tableName})
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
      throw new Error(`Unable to UNLOAD table ${path}`);
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
      csvFile,
      csvNoHeader: true
    };
  }

  public async tablesSchema(): Promise<AthenaSchema> {
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

    const { QueryExecutionId } = await this.athena.startQueryExecution({
      QueryString: queryString,
      WorkGroup: this.config.workGroup,
      ResultConfiguration: {
        OutputLocation: this.config.S3OutputLocation
      }
    });

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

  protected async viewsSchema(tablesSchema: AthenaSchema): Promise<AthenaSchema> {
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
    const data = await this.query(
      `
        SELECT table_schema AS schema, table_name AS name
        FROM information_schema.tables
        WHERE tables.table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
      `,
      []
    );

    return data as AthenaTable[];
  }

  protected async getColumns(table: AthenaTable): Promise<AthenaSchema> {
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

  protected mergeSchemas(arrSchemas: AthenaSchema[]): AthenaSchema {
    const result: AthenaSchema = {};

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

  public static splitS3Path(path: string) {
    const url = new URL(path);
    return {
      bucket: url.host,
      prefix: url.pathname
    };
  }
}
