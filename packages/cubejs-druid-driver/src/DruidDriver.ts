/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview The `DruidDriver` and related types declaration.
 */

import {
  getEnv,
  assertDataSource,
} from '@cubejs-backend/shared';
import { BaseDriver, TableQueryResult } from '@cubejs-backend/base-driver';
import { DruidClient, DruidClientBaseConfiguration, DruidClientConfiguration } from './DruidClient';
import { DruidQuery } from './DruidQuery';

export type DruidDriverConfiguration = DruidClientBaseConfiguration & {
  url?: string,
};

/**
 * Druid driver class.
 */
export class DruidDriver extends BaseDriver {
  protected readonly config: DruidClientConfiguration;

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

  /**
   * Class constructor.
   */
  public constructor(
    config: DruidDriverConfiguration & {
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
    } = {}
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
    this.config = {
      url,
      user:
        config.user ||
        getEnv('dbUser', { dataSource }),
      password:
        config.password ||
        getEnv('dbPass', { dataSource }),
      database:
        config.database ||
        getEnv('dbName', { dataSource }) ||
        'default',
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

  public async query<R = unknown>(query: string, values: unknown[] = []): Promise<Array<R>> {
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

  public async createSchemaIfNotExists(schemaName: string): Promise<void> {
    throw new Error('Unable to create schema, Druid does not support it');
  }

  public async getTablesQuery(schemaName: string) {
    return this.query<TableQueryResult>('SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ?', [
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
