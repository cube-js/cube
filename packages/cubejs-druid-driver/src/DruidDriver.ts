import BaseDriver from '@cubejs-backend/query-orchestrator/driver/BaseDriver';
import { DruidClient, DruidClientConfiguration } from './DruidClient';
import { DruidQuery } from './DruidQuery';

type DruidBaseConfiguration = DruidClientConfiguration;
type DruidDriverConfiguration = DruidBaseConfiguration & unknown;

export class DruidDriver extends BaseDriver {
  protected readonly config: DruidDriverConfiguration;

  protected readonly client: DruidClient;

  static dialectClass() {
    return DruidQuery;
  }

  constructor(config: DruidClientConfiguration) {
    super();

    this.config = {
      host: config.host || process.env.CUBEJS_DB_HOST,
      port: config.port || process.env.CUBEJS_DB_PORT,
      user: config.user || process.env.CUBEJS_DB_USER,
      password: config.password || process.env.CUBEJS_DB_PASS,
      database: config.database || process.env.CUBEJS_DB_NAME || config && config.database || 'default',
      ...config
    };

    this.client = new DruidClient(this.config);
  }

  withConnection(fn: (conn: DruidClient) => Promise<unknown>) {
    let cancelled = false;
    const cancelObj: any = {};

    const promise: Promise<unknown> & { cancel?: () => void } = (async () => {
      cancelObj.cancel = async () => {
        cancelled = true;
      };

      try {
        const result = await fn(this.client);

        if (cancelled) {
          throw new Error('Query cancelled');
        }

        return result;
      } catch (e) {
        if (cancelled) {
          throw new Error('Query cancelled');
        }

        throw e;
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
