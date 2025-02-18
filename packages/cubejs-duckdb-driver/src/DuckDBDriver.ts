import {
  BaseDriver,
  DriverInterface,
  StreamOptions,
  QueryOptions,
  StreamTableData,
  GenericDataBaseType,
} from '@cubejs-backend/base-driver';
import { getEnv } from '@cubejs-backend/shared';
import { promisify } from 'util';
import * as stream from 'stream';
import { Connection, Database } from 'duckdb';

import { DuckDBQuery } from './DuckDBQuery';
import { HydrationStream, transformRow } from './HydrationStream';

const { version } = require('../../package.json');

export type DuckDBDriverConfiguration = {
  dataSource?: string,
  initSql?: string,
  schema?: string,
};

type InitPromise = {
  defaultConnection: Connection,
  db: Database;
};

const DuckDBToGenericType: Record<string, GenericDataBaseType> = {
  // DATE_TRUNC returns DATE, but Cube Store still doesn't support DATE type
  // DuckDB's driver transform date/timestamp to Date object, but HydrationStream converts any Date object to ISO timestamp
  // That's why It's safe to use timestamp here
  date: 'timestamp',
};

export class DuckDBDriver extends BaseDriver implements DriverInterface {
  protected initPromise: Promise<InitPromise> | null = null;

  private schema: string;

  public constructor(
    protected readonly config: DuckDBDriverConfiguration = {},
  ) {
    super();

    this.schema = this.config.schema || getEnv('duckdbSchema', this.config);
  }

  public toGenericType(columnType: string): GenericDataBaseType {
    if (columnType.toLowerCase() in DuckDBToGenericType) {
      return DuckDBToGenericType[columnType.toLowerCase()];
    }

    return super.toGenericType(columnType.toLowerCase());
  }

  protected async init(): Promise<InitPromise> {
    const token = getEnv('duckdbMotherDuckToken', this.config);
    const dbPath = getEnv('duckdbDatabasePath', this.config);
    
    // Determine the database URL based on the provided db_path or token
    let dbUrl: string;
    if (dbPath) {
      dbUrl = dbPath;
    } else if (token) {
      dbUrl = `md:?motherduck_token=${token}&custom_user_agent=Cube/${version}`;
    } else {
      dbUrl = ':memory:';
    }

    let dbOptions;
    if (token) {
      dbOptions = { custom_user_agent: `Cube/${version}` };
    }

    // Create a new Database instance with the determined URL and custom user agent
    const db = new Database(dbUrl, dbOptions);

    // Under the hood all methods of Database uses internal default connection, but there is no way to expose it
    const defaultConnection = db.connect();
    const execAsync: (sql: string, ...params: any[]) => Promise<void> = promisify(defaultConnection.exec).bind(defaultConnection) as any;

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
      {
        key: 's3_use_ssl',
        value: getEnv('duckdbS3UseSsl', this.config),
      },
      {
        key: 's3_url_style',
        value: getEnv('duckdbS3UrlStyle', this.config),
      },
      {
        key: 's3_session_token',
        value: getEnv('duckdbS3SessionToken', this.config),
      }
    ];
    
    for (const { key, value } of configuration) {
      if (value) {
        try {
          await execAsync(`SET ${key}='${value}'`);
        } catch (e) {
          if (this.logger) {
            console.error(`DuckDB - error on configuration, key: ${key}`, {
              e
            });
          }
        }
      }
    }

    // Install & load extensions if configured in env variable.
    const extensions = getEnv('duckdbExtensions', this.config);
    for (const extension of extensions) {
      try {
        await execAsync(`INSTALL ${extension}`);
      } catch (e) {
        if (this.logger) {
          console.error(`DuckDB - error on installing ${extension}`, {
            e
          });
        }

        // DuckDB will lose connection_ref on connection on error, this will lead to broken connection object
        throw e;
      }

      try {
        await execAsync(`LOAD ${extension}`);
      } catch (e) {
        if (this.logger) {
          console.error(`DuckDB - error on loading ${extension}`, {
            e
          });
        }

        // DuckDB will lose connection_ref on connection on error, this will lead to broken connection object
        throw e;
      }
    }

    if (this.config.initSql) {
      try {
        await execAsync(this.config.initSql);
      } catch (e) {
        if (this.logger) {
          console.error('DuckDB - error on init sql (skipping)', {
            e
          });
        }
      }
    }
    
    return {
      defaultConnection,
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

  protected async getInitiatedState(): Promise<InitPromise> {
    if (!this.initPromise) {
      this.initPromise = this.init();
    }

    try {
      return await this.initPromise;
    } catch (e) {
      this.initPromise = null;

      throw e;
    }
  }

  public static dialectClass() {
    return DuckDBQuery;
  }

  public async query<R = unknown>(query: string, values: unknown[] = [], _options?: QueryOptions): Promise<R[]> {
    const { defaultConnection } = await this.getInitiatedState();
    const fetchAsync: (sql: string, ...params: any[]) => Promise<R[]> = promisify(defaultConnection.all).bind(defaultConnection) as any;

    const result = await fetchAsync(query, ...values);
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
    const { db } = await this.getInitiatedState();

    // new connection, because stream can break with
    // Attempting to execute an unsuccessful or closed pending query result
    // PreAggregation queue has a concurrency limit, it's why pool is not needed here
    const connection = db.connect();
    const closeAsync = promisify(connection.close).bind(connection);

    try {
      const asyncIterator = connection.stream(query, ...(values || []));
      const rowStream = stream.Readable.from(asyncIterator, { highWaterMark }).pipe(new HydrationStream());

      return {
        rowStream,
        release: async () => {
          await closeAsync();
        }
      };
    } catch (e) {
      await closeAsync();

      throw e;
    }
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
