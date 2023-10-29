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

type InitPromise = {
  connection: Connection,
  db: Database;
};

export class DuckDBDriver extends BaseDriver implements DriverInterface {
  protected initPromise: Promise<InitPromise> | null = null;

  private schema: string;

  public constructor(
    protected readonly config: DuckDBDriverConfiguration = {},
  ) {
    super();

    this.schema = getEnv('duckdbSchema', this.config);
  }

  protected async init(): Promise<InitPromise> {
    const token = getEnv('duckdbMotherDuckToken', this.config);
    
    const db = new Database(token ? `md:?motherduck_token=${token}` : ':memory:');
    const connection = db.connect();
    
    try {
      await this.handleQuery(connection, 'INSTALL httpfs', []);
    } catch (e) {
      if (this.logger) {
        console.error('DuckDB - error on httpfs installation', {
          e
        });
      }

      // DuckDB will lose connection_ref on connection on error, this will lead to broken connection object
      throw e;
    }

    try {
      await this.handleQuery(connection, 'LOAD httpfs', []);
    } catch (e) {
      if (this.logger) {
        console.error('DuckDB - error on loading httpfs', {
          e
        });
      }

      // DuckDB will lose connection_ref on connection on error, this will lead to broken connection object
      throw e;
    }

    const configuration = [
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
      {
        key: 'memory_limit',
        value: getEnv('duckdbMemoryLimit', this.config),
      },
      {
        key: 'schema',
        value: getEnv('duckdbSchema', this.config),
      },
    ];
    
    for (const { key, value } of configuration) {
      if (value) {
        try {
          await this.handleQuery(connection, `SET ${key}='${value}'`, []);
        } catch (e) {
          if (this.logger) {
            console.error(`DuckDB - error on configuration, key: ${key}`, {
              e
            });
          }
        }
      }
    }

    if (this.config.initSql) {
      try {
        await this.handleQuery(connection, this.config.initSql, []);
      } catch (e) {
        if (this.logger) {
          console.error('DuckDB - error on init sql (skipping)', {
            e
          });
        }
      }
    }
    
    return {
      connection,
      db
    };
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

  protected async getConnection(): Promise<Connection> {
    if (!this.initPromise) {
      this.initPromise = this.init();
    }

    try {
      const { connection } = await this.initPromise;
      return connection;
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
      const { db } = await this.initPromise;
      const close = promisify(db.close).bind(db);
      this.initPromise = null;

      await close();
    }
  }
}
