import * as AWS from '@aws-sdk/client-athena';
import { BaseDriver, QueryOptions } from '@cubejs-backend/query-orchestrator';
import { getEnv, pausePromise, Required } from '@cubejs-backend/shared';
import * as SqlString from 'sqlstring';
import { AthenaClientConfig } from '@aws-sdk/client-athena/dist-types/AthenaClient';

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

interface AthenaQueryId {
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

function checkNonNullable<T>(name: string, x: T): NonNullable<T> {
  if (x === undefined || x === null) {
    throw new Error(`${name} is not defined.`);
  }
  return x!;
}

function checkQueryId<T extends {QueryExecutionId?: string}>(name: string, t: T): AthenaQueryId {
  return {
    QueryExecutionId: checkNonNullable(name, t.QueryExecutionId)
  };
}

function applyParams(query: string, params: any[]): string {
  return SqlString.format(query, params);
}

class AthenaDriver extends BaseDriver {
  private config: AthenaDriverOptionsInitialized;

  private athena: AWS.Athena;

  public constructor(config: AthenaDriverOptions = {}) {
    super();

    this.config = {
      credentials: {
        accessKeyId: checkNonNullable('CUBEJS_AWS_KEY', config.accessKeyId || process.env.CUBEJS_AWS_KEY),
        secretAccessKey: checkNonNullable('CUBEJS_AWS_SECRET', config.secretAccessKey || process.env.CUBEJS_AWS_SECRET),
      },
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

  protected async awaitForJobStatus<R = unknown>(qid: AthenaQueryId, query: string, options: any): Promise<R[] | null> {
    const queryExecution = await this.athena.getQueryExecution(qid);

    const status = queryExecution.QueryExecution?.Status?.State;
    if (status === 'FAILED') {
      throw new Error(queryExecution.QueryExecution?.Status?.StateChangeReason);
    }

    if (status === 'CANCELLED') {
      throw new Error('Query has been cancelled');
    }

    if (
      status === 'SUCCEEDED'
    ) {
      const allRows: AWS.Row[] = [];
      let columnInfo: { Name: string }[] | undefined;

      this.reportQueryUsage({
        dataScannedInBytes: queryExecution.QueryExecution?.Statistics?.DataScannedInBytes
      }, options);

      for (
        let results: AWS.GetQueryResultsCommandOutput | undefined = await this.athena.getQueryResults(qid);
        results;
        results = results.NextToken
          ? (await this.athena.getQueryResults({ ...qid, NextToken: results.NextToken }))
          : undefined
      ) {
        const rows = results.ResultSet?.Rows ?? [];
        if (rows.length > 0) {
          const [header, ...tableRows] = rows;
          allRows.push(...(allRows.length ? rows : tableRows));
          if (!columnInfo) {
            columnInfo = /SHOW COLUMNS/.test(query) // Fix for getColumns method
              ? [{ Name: 'column' }]
              : results.ResultSet?.ResultSetMetadata?.ColumnInfo?.map(info => ({ Name: checkNonNullable('Name', info.Name) }));
          }
        }
      }

      return allRows.map(r => {
        const fields: Record<string, any> = {};
        checkNonNullable('ColumnInfo', columnInfo)
          .forEach((c, i) => {
            fields[c.Name] = r.Data?.[i].VarCharValue;
          });
        return fields;
      }) as unknown as R[];
    }

    return null;
  }

  public async query<R = unknown>(query: string, values: unknown[], options?: QueryOptions): Promise<R[]> {
    const queryString = applyParams(
      query,
      (values || []).map(s => (typeof s === 'string' ? {
        toSqlString: () => SqlString.escape(s).replace(/\\\\([_%])/g, '\\$1').replace(/\\'/g, '\'\'')
      } : s))
    );

    const qid = checkQueryId(
      'Start query id',
      await this.athena.startQueryExecution({
        QueryString: queryString,
        WorkGroup: this.config.workGroup,
        ResultConfiguration: {
          OutputLocation: this.config.S3OutputLocation
        }
      })
    );

    const startedTime = Date.now();

    for (let i = 0; Date.now() - startedTime <= this.config.pollTimeout; i++) {
      const result: R[] | null = await this.awaitForJobStatus(qid, query, options);
      if (result) {
        return result;
      }

      await pausePromise(
        Math.min(this.config.pollMaxInterval, 500 * i)
      );
    }

    throw new Error(
      `Athena job timeout reached ${this.config.pollTimeout}ms`
    );
  }

  public async tablesSchema(): Promise<AthenaSchema> {
    const tablesSchema = await super.tablesSchema();
    const viewsSchema = await this.viewsSchema(tablesSchema);

    return this.mergeSchemas([tablesSchema, viewsSchema]);
  }

  private async viewsSchema(tablesSchema: AthenaSchema): Promise<AthenaSchema> {
    // eslint-disable-next-line camelcase
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

  // eslint-disable-next-line camelcase
  protected async getColumns(table: AthenaTable): Promise<AthenaSchema> {
    // eslint-disable-next-line camelcase
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

module.exports = AthenaDriver;
