import {
  BaseDriver,
  DriverInterface,
  StreamOptions,
  QueryOptions, StreamTableData,
} from '@cubejs-backend/base-driver';
import { getEnv } from '@cubejs-backend/shared';
import { promisify } from 'util';
import * as stream from 'stream';
// eslint-disable-next-line import/no-extraneous-dependencies
import { Connection, Database } from 'duckdb';

import { DuckDBQuery } from './DuckDBQuery';
import { HydrationStream, transformRow } from './HydrationStream';

export type DuckDBDriverConfiguration = {
  dataSource?: string,
  initSql?: string,
};

export class DuckDBDriver extends BaseDriver implements DriverInterface {
  protected initPromise: Promise<Database> | null = null;

  public constructor(
    protected readonly config: DuckDBDriverConfiguration = {},
  ) {
    super();
  }

  protected async initDatabase(): Promise<Database> {
    const token = getEnv('duckdbMotherDuckToken', this.config);
    
    const db = new Database(token ? `md:?motherduck_token=${token}` : ':memory:');
    const conn = db.connect();

    const s3InitQuries = [
      {
        key: 's3_region',
        value: getEnv('duckdbS3Region', this.config),
      },
      {
        key: 's3_endpoint',
        value: getEnv('duckdbS3Endpoint', this.config),
      },
      {
        key: 's3_access_key_id',
        value: getEnv('duckdbS3AccessKeyId', this.config),
      },
      {
        key: 's3_secret_access_key',
        value: getEnv('duckdbS3SecretAccessKeyId', this.config),
      },
    ];
    
    try {
      await this.handleQuery(conn, 'INSTALL httpfs', []);
    } catch (e) {
      if (this.logger) {
        console.error('DuckDB - error on httpfs installation', {
          e
        });
      }
    }
    
    try {
      for (const { key, value } of s3InitQuries) {
        if (value) {
          await this.handleQuery(conn, `SET ${key}='${value}'`, []);
        }
      }
    } catch (e) {
      if (this.logger) {
        console.error('DuckDB - error on s3 configuration', {
          e
        });
      }
    }

    if (this.config.initSql) {
      try {
        await this.handleQuery(conn, this.config.initSql, []);
      } catch (e) {
        if (this.logger) {
          console.error('DuckDB - error on init sql (skipping)', {
            e
          });
        }
      }
    }

    return db;
  }

  protected async getConnection() {
    if (!this.initPromise) {
      this.initPromise = this.initDatabase();
    }

    try {
      const db = (await this.initPromise);
      return db.connect();
    } catch (e) {
      this.initPromise = null;

      throw e;
    }
  }

  public static dialectClass() {
    return DuckDBQuery;
  }

  protected handleQuery<R>(connection: Connection, query: string, values: unknown[] = [], _options?: QueryOptions): Promise<R[]> {
    const executeQuery: (sql: string, ...args: any[]) => Promise<R[]> = promisify(connection.all).bind(connection) as any;

    return executeQuery(query, ...values);
  }

  public async query<R = unknown>(query: string, values: unknown[] = [], _options?: QueryOptions): Promise<R[]> {
    const result = await this.handleQuery<R>(await this.getConnection(), query, values, _options);

    return result.map((item) => {
      transformRow(item);

      return item;
    });
  }

  public async stream(
    query: string,
    values: unknown[],
    { highWaterMark }: StreamOptions
  ): Promise<StreamTableData> {
    const connection = await this.getConnection();

    const asyncIterator = connection.stream(query, ...(values || []));
    const rowStream = stream.Readable.from(asyncIterator, { highWaterMark }).pipe(new HydrationStream());

    return {
      rowStream,
    };
  }

  public async testConnection(): Promise<void> {
    await this.query('SELECT 1', []);
  }

  public readOnly() {
    return false;
  }

  public async release(): Promise<void> {
    if (this.initPromise) {
      const db = await this.initPromise;
      const close = promisify(db.close).bind(db);
      this.initPromise = null;

      await close();
    }
  }
}
