import SqlString from 'sqlstring';
import { Athena, AthenaClientConfig, QueryExecutionState, QueryExecutionStatistics, paginateGetQueryResults } from '@aws-sdk/client-athena';

import { BaseDriver, DriverInterface, QueryOptions } from '@cubejs-backend/query-orchestrator';
import { getEnv, pausePromise } from '@cubejs-backend/shared';

export interface AthenaDriverOptions extends AthenaClientConfig {
  // Compatibility
  accessKeyId?: string;
  secretAccessKey?: string;
  //
  readOnly?: boolean,
  pollTimeout?: number,
  pollMaxInterval?: number,
}

export class AthenaDriver extends BaseDriver implements DriverInterface {
  protected readonly config: any;

  protected readonly athena: Athena;

  public constructor(config: AthenaDriverOptions = {}) {
    super();

    this.config = {
      credentials: {
        accessKeyId: config.accessKeyId || process.env.CUBEJS_AWS_KEY,
        secretAccessKey: config.secretAccessKey || process.env.CUBEJS_AWS_SECRET,
      },
      region: process.env.CUBEJS_AWS_REGION,
      S3OutputLocation: process.env.CUBEJS_AWS_S3_OUTPUT_LOCATION,
      ...config,
      pollTimeout: (config.pollTimeout || getEnv('dbPollTimeout')) * 1000,
      pollMaxInterval: (config.pollMaxInterval || getEnv('dbPollMaxInterval')) * 1000,
    };

    this.athena = new Athena(this.config);
  }

  public readOnly() {
    return !!this.config.readOnly;
  }

  public async testConnection() {
    await this.query('SELECT 1', []);
  }

  protected async awaitForJobStatus(QueryExecutionId: string, query: string, options: any) {
    const queryExecution = await this.athena.getQueryExecution({
      QueryExecutionId
    });

    const { QueryExecution } = queryExecution;
    if (!QueryExecution) {
      throw new Error('Unable to detect QueryExecution result for query');
    }

    const { Status } = QueryExecution;
    if (!Status) {
      throw new Error('Unable to detect QueryExecution result for query');
    }

    const status = Status.State;

    if (status === QueryExecutionState.FAILED) {
      throw new Error(Status.StateChangeReason);
    }

    if (status === QueryExecutionState.CANCELLED) {
      throw new Error('Query has been cancelled');
    }

    if (status === QueryExecutionState.SUCCEEDED) {
      let columnInfo: any;

      this.reportQueryUsage({
        dataScannedInBytes: (<QueryExecutionStatistics> QueryExecution.Statistics).DataScannedInBytes
      }, options);

      const paginator = paginateGetQueryResults({
        client: this.athena,
        // It's max limit
        pageSize: 1000,
      }, {
        QueryExecutionId,
      });

      const allRows: any[] = [];

      // eslint-disable-next-line no-restricted-syntax
      for await (const page of paginator) {
        const tableRows = page?.ResultSet?.Rows;

        // @ts-ignore
        allRows.push(...(allRows.length ? tableRows : tableRows));
        if (!columnInfo) {
          columnInfo = /SHOW COLUMNS/.test(query) // Fix for getColumns method
            ? [{ Name: 'column' }]
            : page?.ResultSet?.ResultSetMetadata?.ColumnInfo;
        }
      }

      return allRows.map(r => columnInfo
        .map((c: any, i: any) => ({ [c.Name]: r.Data[i].VarCharValue }))
        .reduce((a: any, b: any) => ({ ...a, ...b }), {}));
    }

    return null;
  }

  public async query(query: string, values: unknown[], options?: QueryOptions) {
    const queryString = SqlString.format(
      query,
      (values || []).map(s => (typeof s === 'string' ? {
        toSqlString: () => SqlString.escape(s).replace(/\\\\([_%])/g, '\\$1').replace(/\\'/g, '\'\'')
      } : s))
    );

    const { QueryExecutionId } = await this.athena.startQueryExecution({
      QueryString: queryString,
      ResultConfiguration: {
        OutputLocation: this.config.S3OutputLocation
      }
    });
    if (!QueryExecutionId) {
      throw new Error(
        'Unable to detect QueryExecutionId for the query'
      );
    }

    const startedTime = Date.now();

    for (let i = 0; Date.now() - startedTime <= this.config.pollTimeout; i++) {
      const result = await this.awaitForJobStatus(QueryExecutionId, query, options);
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

  public async tablesSchema() {
    const tablesSchema = await super.tablesSchema();
    const viewsSchema = await this.viewsSchema(tablesSchema);

    return this.mergeSchemas([tablesSchema, viewsSchema]);
  }

  public async viewsSchema(tablesSchema: any) {
    // eslint-disable-next-line camelcase
    const isView = ({ table_schema, table_name }: any) => !tablesSchema[table_schema]
      || !tablesSchema[table_schema][table_name];

    const allTables = await this.getAllTables();
    const arrViewsSchema = await Promise.all(
      allTables
        .filter(isView)
        .map(table => this.getColumns(table))
    );

    return this.mergeSchemas(arrViewsSchema);
  }

  public async getAllTables() {
    const data = await this.query(`
      SELECT table_schema, table_name
      FROM information_schema.tables
      WHERE tables.table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
    `, []);

    return data;
  }

  // eslint-disable-next-line camelcase
  public async getColumns({ table_schema, table_name }: any = {}) {
    // eslint-disable-next-line camelcase
    const data = await this.query(`SHOW COLUMNS IN \`${table_schema}\`.\`${table_name}\``, []);

    return {
      [table_schema]: {
        [table_name]: data.map(({ column }) => {
          const [name, type] = column.split('\t');
          return { name, type, attributes: [] };
        })
      }
    };
  }

  public mergeSchemas(arrSchemas: any) {
    const result: any = {};

    arrSchemas.forEach((schemas: any) => {
      Object.keys(schemas).forEach(schema => {
        Object.keys(schemas[schema]).forEach((name) => {
          if (!result[schema]) {
            result[schema] = {};
          }

          if (!result[schema][name]) {
            result[schema][name] = schemas[schema][name];
          }
        });
      });
    });

    return result;
  }
}
