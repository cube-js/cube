import * as AWS from '@aws-sdk/client-athena';
import * as stream from 'stream';
import { BaseDriver, DriverInterface, QueryOptions, StreamTableData } from '@cubejs-backend/query-orchestrator';
import { checkNonNullable, getEnv, pausePromise, Required } from '@cubejs-backend/shared';
import * as SqlString from 'sqlstring';
import { AthenaClientConfig } from '@aws-sdk/client-athena/dist-types/AthenaClient';
import { hydrationStream } from "./HydrationStream";

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

  private athena: AWS.Athena;

  public constructor(config: AthenaDriverOptions = {}) {
    super();

    const accessKeyId = config.accessKeyId || process.env.CUBEJS_AWS_KEY;
    const secretAccessKey = config.secretAccessKey || process.env.CUBEJS_AWS_SECRET;

    this.config = {
      credentials: accessKeyId && secretAccessKey ? { accessKeyId, secretAccessKey } : undefined,
      region: process.env.CUBEJS_AWS_REGION,
      S3OutputLocation: config.S3OutputLocation || process.env.CUBEJS_AWS_S3_OUTPUT_LOCATION,
      workGroup: config.workGroup || process.env.CUBEJS_AWS_ATHENA_WORKGROUP || 'primary',
      ...config,
      pollTimeout: (config.pollTimeout || getEnv('dbPollTimeout') || getEnv('dbQueryTimeout')) * 1000,
      pollMaxInterval: (config.pollMaxInterval || getEnv('dbPollMaxInterval')) * 1000,
    };

    this.athena = new AWS.Athena(this.config);
  }

  public readOnly(): boolean {
    return !!this.config.readOnly;
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
    for await (const row of this.rowIterator(qid, query)) {
      rows.push(row);
    }
    return rows;
  }

  public async stream(query: string, values: unknown[]): Promise<StreamTableData> {
    const qid = await this.startQuery(query, values);
    await this.waitForSuccess(qid);
    const rowStream = stream.Readable.from(this.rowIterator(qid, query));
    return {
      rowStream
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

  protected async* rowIterator<R extends unknown>(qid: AthenaQueryId, query: string): AsyncGenerator<R> {
    let columnInfo: { Name: string }[] | undefined;
    for (
      let results: AWS.GetQueryResultsCommandOutput | undefined = await this.athena.getQueryResults(qid);
      results;
      results = results.NextToken
        ? (await this.athena.getQueryResults({ ...qid, NextToken: results.NextToken }))
        : undefined
    ) {
      if (!columnInfo) {
        columnInfo = /SHOW COLUMNS/.test(query) // Fix for getColumns method
          ? [{ Name: 'column' }]
          : results.ResultSet?.ResultSetMetadata?.ColumnInfo?.map(info => ({ Name: checkNonNullable('Name', info.Name) }));
      }

      const rows = results.ResultSet?.Rows ?? [];
      for (let i = 0; i < rows.length; i++) {
        const row = rows[i];
        const fields: Record<string, any> = {};
        checkNonNullable('ColumnInfo', columnInfo)
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
}
