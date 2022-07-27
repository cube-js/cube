import {
  BaseDriver,
  DriverInterface,
  StreamTableData,
  DownloadTableCSVData,
} from '@cubejs-backend/query-orchestrator';
import {
  Firebolt,
  ConnectionOptions,
  Connection,
  OutputFormat,
  Meta,
  Row,
  isNumberType
} from 'firebolt-sdk';
import { FireboltQuery } from './FireboltQuery';

export type FireboltDriverConfiguration = {
  readOnly?: boolean;
  apiEndpoint?: string;
  connection: ConnectionOptions;
};

const FireboltTypeToGeneric: Record<string, string> = {
  long: 'bigint',
};

const COMPLEX_TYPE = /(nullable|array)\((.+)\)/;

export class FireboltDriver extends BaseDriver implements DriverInterface {
  /**
   * Returns default concurrency value.
   */
  public static getDefaultConcurrency(): number {
    return 5;
  }

  private config: FireboltDriverConfiguration;

  private firebolt;

  private connection: Promise<Connection> | null = null;

  public constructor(config: Partial<FireboltDriverConfiguration> = {}) {
    super(config);

    this.config = {
      readOnly: true,
      apiEndpoint: process.env.CUBEJS_FIREBOLT_API_ENDPOINT,
      ...config,
      connection: {
        username: <string>process.env.CUBEJS_DB_USER,
        password: <string>process.env.CUBEJS_DB_PASS,
        database: <string>process.env.CUBEJS_DB_NAME,
        account: <string>process.env.CUBEJS_FIREBOLT_ACCOUNT,
        engineName: <string>process.env.CUBEJS_FIREBOLT_ENGINE_NAME,
        // engineEndpoint was deprecated in favor of engineName + account
        engineEndpoint: <string>process.env.CUBEJS_FIREBOLT_ENGINE_ENDPOINT,
        ...(config.connection || {}),
      },
    };

    this.firebolt = Firebolt({
      apiEndpoint: this.config.apiEndpoint,
    });
  }

  public quoteIdentifier(identifier: string): string {
    return `"${identifier}"`;
  }

  private async initConnection() {
    try {
      const connection = await this.firebolt.connect(this.config.connection);
      return connection;
    } catch (e) {
      this.connection = null;
      throw e;
    }
  }

  public createTableSql(
    quotedTableName: string,
    columns: { name: string; type: string }[]
  ) {
    const cols = columns
      .map(
        (c) => `${this.quoteIdentifier(c.name)} ${this.fromGenericType(c.type)}`
      )
      .join(', ');

    return `CREATE DIMENSION TABLE ${quotedTableName} (${cols})`;
  }

  public dropTable(tableName: string) {
    if (tableName.match(/\./)) {
      const [_, name] = tableName.split('.');
      tableName = name;
    }
    return this.query(`DROP TABLE ${tableName}`, []);
  }

  private async getConnection(): Promise<Connection> {
    if (this.connection) {
      const connection = await this.connection;
      return connection;
    }

    this.connection = this.initConnection();
    return this.connection;
  }

  public static dialectClass() {
    return FireboltQuery;
  }

  public async createSchemaIfNotExists(_schemaName: string): Promise<any> {
    // no-op
  }

  public async testConnection(): Promise<void> {
    try {
      await this.query('select 1');
    } catch (error) {
      console.log(error);
      throw new Error('Unable to connect');
    }
  }

  private getHydratedValue(value: unknown, meta: Meta) {
    const { type } = meta;
    if (isNumberType(type)) {
      return `${value}`;
    }
    return value;
  }

  private hydrateRow = (row: Row, meta: Meta[]) => {
    const hydratedRow: Record<string, unknown> = {};
    for (let index = 0; index < meta.length; index++) {
      const column = meta[index];
      const key = column.name;
      hydratedRow[key] = this.getHydratedValue(
        (row as Record<string, unknown>)[key],
        column,
      );
    }
    return hydratedRow;
  };

  public async query<R = Record<string, unknown>>(
    query: string,
    parameters?: unknown[]
  ): Promise<R[]> {
    const response = await this.queryResponse(query, parameters);
    return response.data as R[];
  }

  public async stream(
    query: string,
    parameters: unknown[]
  ): Promise<StreamTableData> {
    return this.streamResponse(query, parameters);
  }

  private async streamResponse(
    query: string,
    parameters: unknown[],
    retry = true
  ): Promise<StreamTableData> {
    try {
      const connection = await this.getConnection();

      const statement = await connection.execute(query, {
        settings: { output_format: OutputFormat.JSON },
        parameters,
        response: { hydrateRow: this.hydrateRow }
      });

      const { data: rowStream, meta: metaPromise } =
        await statement.streamResult();
      const meta = await metaPromise;

      const types = meta.map(({ type, name }) => ({
        name,
        type: this.toGenericType(type),
      }));

      return {
        rowStream,
        types,
      };
    } catch (error) {
      if (error.status === 404 && retry) {
        await this.ensureEngineRunning();
        return this.streamResponse(query, parameters, false);
      }
      throw error;
    }
  }

  public async unload(): Promise<DownloadTableCSVData> {
    throw new Error('Unload is not supported');
  }

  private async ensureEngineRunning() {
    if (this.config.connection.engineName) {
      const engine = await this.firebolt.resourceManager.engine.getByName(this.config.connection.engineName);
      await engine.startAndWait();
    }
  }

  private async queryResponse(query: string, parameters?: unknown[], retry = true): Promise<{
    data: Row[];
    meta: Meta[];
  }> {
    try {
      const connection = await this.getConnection();

      const statement = await connection.execute(query, {
        settings: { output_format: OutputFormat.JSON },
        parameters,
        response: { hydrateRow: this.hydrateRow }
      });
      const response = await statement.fetchResult();
      return response;
    } catch (error) {
      if (error.status === 404 && retry) {
        await this.ensureEngineRunning();
        return this.queryResponse(query, parameters, false);
      }
      throw error;
    }
  }

  /* eslint-disable camelcase */
  public async getTablesQuery(): Promise<
    { table_name?: string; TABLE_NAME?: string }[]
    > {
    const data = await this.query<{ table_name: string }>('SHOW TABLES', []);
    return data.map(({ table_name }) => ({ table_name }));
  }
  /* eslint-enable camelcase */

  public async downloadQueryResults(query: string, values: unknown[]) {
    const response = await this.queryResponse(query, values);
    const { data, meta } = response;
    const types = meta.map(({ type, name }) => ({
      name,
      type: this.toGenericType(type),
    }));
    return {
      rows: data as Record<string, unknown>[],
      types,
    };
  }

  /* eslint-disable camelcase */
  public async tableColumnTypes(table: string) {
    const response = await this.query<{
      column_name: string;
      data_type: string;
    }>(`DESCRIBE ${table}`, []);
    return response.map((row) => ({
      name: row.column_name,
      type: this.toGenericType(row.data_type),
    }));
  }
  /* eslint-enable camelcase */

  public toGenericType(columnType: string) {
    if (columnType in FireboltTypeToGeneric) {
      return FireboltTypeToGeneric[columnType];
    }

    const match = columnType.match(COMPLEX_TYPE);
    if (match) {
      const [_, _outerType, innerType] = match;
      if (columnType in FireboltTypeToGeneric) {
        return FireboltTypeToGeneric[innerType];
      }
    }
    return super.toGenericType(columnType);
  }

  public readOnly() {
    return !!this.config.readOnly;
  }

  public async isUnloadSupported() {
    return false;
  }

  public async release() {
    if (this.connection) {
      const connection = await this.connection;
      connection.destroy();
      this.connection = null;
    }
  }
}
