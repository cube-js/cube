import { BaseDriver } from '@cubejs-backend/base-driver';
import { getEnv } from '@cubejs-backend/shared';

import { DruidClient, DruidClientBaseConfiguration } from './DruidClient';
import { DruidQuery } from './DruidQuery';

export type DruidDriverConfiguration = DruidClientBaseConfiguration & {
  url: string,
};

export class DruidDriver extends BaseDriver {
  protected readonly config: DruidDriverConfiguration;

  protected readonly client: DruidClient;

  public static dialectClass() {
    return DruidQuery;
  }

  /**
   * Returns default concurrency value.
   */
  public static getDefaultConcurrency(): number {
    return 2;
  }

  public constructor(config?: DruidDriverConfiguration) {
    super();

    let url = config?.url || process.env.CUBEJS_DB_URL;
    if (!url) {
      const host = process.env.CUBEJS_DB_HOST;
      const port = process.env.CUBEJS_DB_PORT;

      if (host && port) {
        const protocol = getEnv('dbSsl') ? 'https' : 'http';
        url = `${protocol}://${host}:${port}`;
      } else {
        throw new Error('Please specify CUBEJS_DB_URL');
      }
    }

    this.config = {
      url,
      user: config?.user || process.env.CUBEJS_DB_USER,
      password: config?.password || process.env.CUBEJS_DB_PASS,
      database: config?.database || process.env.CUBEJS_DB_NAME || config?.database || 'default',
      ...config,
    };

    this.client = new DruidClient(this.config);
  }

  public readOnly() {
    return true;
  }

  public async testConnection() {
    //
  }

  public async query(query: string, values: unknown[] = []): Promise<Array<unknown>> {
    return this.client.query(query, this.normalizeQueryValues(values));
  }

  public informationSchemaQuery() {
    return `
        SELECT
            COLUMN_NAME as ${this.quoteIdentifier('column_name')},
            TABLE_NAME as ${this.quoteIdentifier('table_name')},
            TABLE_SCHEMA as ${this.quoteIdentifier('table_schema')},
            DATA_TYPE as ${this.quoteIdentifier('data_type')}
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'sys')
    `;
  }

  public async createSchemaIfNotExists(schemaName: string): Promise<unknown[]> {
    throw new Error('Unable to create schema, Druid does not support it');
  }

  public async getTablesQuery(schemaName: string) {
    return this.query('SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ?', [
      schemaName
    ]);
  }

  protected normalizeQueryValues(values: unknown[]) {
    return values.map((value) => ({
      value,
      type: 'VARCHAR',
    }));
  }

  protected normaliseResponse(res: any) {
    return res;
  }
}
