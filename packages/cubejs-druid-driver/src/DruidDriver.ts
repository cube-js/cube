const { uuid } = require('uuidv4');
const sqlstring = require('sqlstring');

import { DruidClient, DruidClientConfiguration } from './DruidClient';
import { createPool, Pool, Options as PoolOptions } from 'generic-pool';
import { BaseDriver } from '@cubejs-backend/query-orchestrator';
import { DruidQuery } from './DruidQuery';

type DruidBaseConfiguration = DruidClientConfiguration & Pick<PoolOptions, 'max' & 'min'>;
type DruidDriverConfiguration = DruidBaseConfiguration & unknown;

export class DruidDriver extends BaseDriver {
  protected readonly config: DruidDriverConfiguration;
  protected readonly pool: Pool<DruidClient>;

  static dialectClass() {
    return DruidQuery;
  }

  constructor(config: DruidClientConfiguration) {
    super();

    this.config = {
      host: process.env.CUBEJS_DB_HOST,
      port: process.env.CUBEJS_DB_PORT,
      user: process.env.CUBEJS_DB_USER,
      password: process.env.CUBEJS_DB_PASS,
      database: process.env.CUBEJS_DB_NAME || config && config.database || 'default',
      ...config
    };

    this.pool = createPool({
      create: async () => new DruidClient(this.config),
      destroy: () => Promise.resolve()
    }, {
      min: 0,
      max: 8,
      evictionRunIntervalMillis: 10000,
      softIdleTimeoutMillis: 30000,
      idleTimeoutMillis: 30000,
      acquireTimeoutMillis: 20000
    });
  }

  withConnection(fn: (conn: DruidClient) => Promise<unknown>) {
    let cancelled = false;
    const cancelObj: any = {};

    const promise: Promise<unknown> & { cancel?: () => void } = (async () => {
      const connection = await this.pool.acquire();

      cancelObj.cancel = async () => {
        cancelled = true;
      };

      try {
        const result = await fn(connection);

        if (cancelled) {
          throw new Error('Query cancelled');
        }

        return result;
      } catch (e) {
        if (cancelled) {
          throw new Error('Query cancelled');
        }

        throw e;
      } finally {
        await this.pool.release(connection);
      }
    })();

    promise.cancel = () => cancelObj.cancel();
    return promise;
  }

  public async testConnection() {
    return true;
  }

  public async query(query: string, values: unknown[] = []) {
    return this.withConnection(
      (client) => client.query(query, this.normalizeQueryValues(values))
    );
  }

  public async release() {
    await this.pool.drain();
    await this.pool.clear();
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

  public async createSchemaIfNotExists(schemaName: string) {
    throw new Error('Unable to create schema, Druid doesnot support it');
  }

  public async getTablesQuery(schemaName: string) {
    return this.query(`SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ?`, [
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
