import axios, { AxiosInstance } from "axios";
import {
  BaseDriver,
  DriverInterface,
  StreamOptions,
  QueryOptions,
  StreamTableData,
  GenericDataBaseType,
} from '@cubejs-backend/base-driver';
import {
  getEnv,
  assertDataSource,
} from '@cubejs-backend/shared';
import { Table, tableFromIPC } from 'apache-arrow';

import { DuckDBServerQuery } from './DuckDBServerQuery';
import { transformValue } from './transform';

export type DuckDBServerDriverConfiguration = {
  initSql?: string,
  database?: string,
  schema?: string,
  url?: string,
};

export class DuckDBServerDriver extends BaseDriver implements DriverInterface {
  protected readonly client: AxiosInstance;

  private database: string;
  private schema: string;

  public constructor(
    protected readonly config: DuckDBServerDriverConfiguration & {
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

    let url = config.url || getEnv('dbUrl', { dataSource });

    if (!url) {
      const host = getEnv('dbHost', { dataSource });
      const port = getEnv('dbPort', { dataSource });
      if (host && port) {
        const protocol = getEnv('dbSsl', { dataSource })
          ? 'https'
          : 'http';
        url = `${protocol}://${host}:${port}`;
      } else {
        throw new Error('Please specify CUBEJS_DB_URL');
      }
    }

    this.database = config.database || getEnv('dbName', { dataSource });
    this.schema = config.schema || getEnv('duckdbSchema', { dataSource });

    this.config = {
      url,
      ...config,
    };

    this.client = axios.create({ baseURL: url });
  }

  public readOnly() {
    return false;
  }

  public async testConnection() {
    await this.query('SELECT 1');
  }

  public override informationSchemaQuery(): string {
    if (this.schema) {
      return `${super.informationSchemaQuery()} AND table_catalog = '${this.schema}'`;
    }

    return super.informationSchemaQuery();
  }

  public override getSchemasQuery(): string {
    if (this.schema) {
      return `
        SELECT table_schema as ${super.quoteIdentifier('schema_name')}
        FROM information_schema.tables
        WHERE table_catalog = '${this.schema}'
        GROUP BY table_schema
      `;
    }
    return super.getSchemasQuery();
  }

  public static dialectClass() {
    return DuckDBServerQuery;
  }

  protected async fetchAsync(sql: string, args: unknown[], persist: boolean = false): Promise<Table> {
    const data = {
      sql,
      args,
      database: this.database,
      type: 'arrow',
      persist
    };

    const headers = {
      'Content-Type': 'application/json'
    };

    const response = await this.client.post('/', data, {
      headers,
      responseType: 'arraybuffer'
    });

    return tableFromIPC(new Uint8Array(response.data));
  }

  public async query<R = unknown>(query: string, args: unknown[] = [], _options?: QueryOptions): Promise<R[]> {
    const result = await this.fetchAsync(query, args, false);
    const jsonResult = [];
    for (const row of result) {
      const jsonRow: Record<string, any> = {};
      result.schema.fields.forEach(field => {
        jsonRow[field.name] = transformValue(row[field.name]);
      });
      jsonResult.push(jsonRow);
    }

    return jsonResult as R[];
  }
}
