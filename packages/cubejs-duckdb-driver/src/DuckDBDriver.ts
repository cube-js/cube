import {
  BaseDriver,
  DriverInterface,
  StreamOptions,
  QueryOptions,
  StreamTableData,
  GenericDataBaseType,
} from '@cubejs-backend/base-driver';
import { getEnv } from '@cubejs-backend/shared';
import * as stream from 'stream';
import { DuckDBInstance, DuckDBConnection, DuckDBValue } from '@duckdb/node-api';

import { DuckDBQuery } from './DuckDBQuery';
import { cubeValueConverter } from './HydrationStream';

const { version } = require('../../package.json');

export type DuckDBDriverConfiguration = {
  databasePath?: string,
  dataSource?: string,
  initSql?: string,
  motherDuckToken?: string,
  schema?: string,
  duckdbS3UseCredentialChain?: boolean,
  preAggregations?: boolean,
};

type InitPromise = {
  defaultConnection: DuckDBConnection,
  db: DuckDBInstance;
};

const DuckDBToGenericType: Record<string, GenericDataBaseType> = {
  // DATE_TRUNC returns DATE, but Cube Store doesn't support DATE type
  // The cubeValueConverter transforms date/timestamp to ISO timestamp strings
  date: 'timestamp',
};

export class DuckDBDriver extends BaseDriver implements DriverInterface {
  protected initPromise: Promise<InitPromise> | null = null;

  private readonly schema: string;

  public constructor(
    protected readonly config: DuckDBDriverConfiguration = {},
  ) {
    super();

    this.schema = this.config.schema || getEnv('duckdbSchema', this.config);
  }

  protected override toGenericType(columnType: string, precision?: number | null, scale?: number | null): GenericDataBaseType {
    const match = columnType.trim().toLowerCase().match(/^numeric\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)$/i);

    if (match) {
      precision = Number(match[1]);
      scale = Number(match[2]);
    }

    return DuckDBToGenericType[columnType.toLowerCase()] || super.toGenericType(columnType.toLowerCase(), precision, scale);
  }

  private async installExtensions(extensions: string[], connection: DuckDBConnection, repository: string = ''): Promise<void> {
    repository = repository ? ` FROM ${repository}` : '';
    for (const extension of extensions) {
      try {
        await connection.run(`INSTALL ${extension}${repository}`);
      } catch (e) {
        if (this.logger) {
          console.error(`DuckDB - error on installing ${extension}`, { e });
        }
        // DuckDB will lose connection_ref on connection on error, this will lead to broken connection object
        throw e;
      }
    }
  }

  private async loadExtensions(extensions: string[], connection: DuckDBConnection): Promise<void> {
    for (const extension of extensions) {
      try {
        await connection.run(`LOAD ${extension}`);
      } catch (e) {
        if (this.logger) {
          console.error(`DuckDB - error on loading ${extension}`, { e });
        }
        // DuckDB will lose connection_ref on connection on error, this will lead to broken connection object
        throw e;
      }
    }
  }

  protected async init(): Promise<InitPromise> {
    const token = this.config.motherDuckToken || getEnv('duckdbMotherDuckToken', this.config);
    const dbPath = this.config.databasePath || getEnv('duckdbDatabasePath', this.config);
    // Determine the database URL based on the provided db_path or token
    let dbUrl: string;
    if (dbPath) {
      dbUrl = dbPath;
    } else if (token) {
      dbUrl = `md:?motherduck_token=${token}&custom_user_agent=Cube/${version}`;
    } else {
      dbUrl = ':memory:';
    }

    const dbOptions: any = {};
    if (token) {
      dbOptions.custom_user_agent = `Cube/${version}`;
    }

    // Create a new Database instance with the determined URL and custom user agent
    const db = await DuckDBInstance.create(dbUrl, dbOptions);

    // Create a default connection for configuration and queries
    const defaultConnection = await db.connect();

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
          await defaultConnection.run(`SET ${key}='${value}'`);
        } catch (e) {
          if (this.logger) {
            console.error(`DuckDB - error on configuration, key: ${key}`, {
              e
            });
          }
        }
      }
    }

    const useCredentialChain = this.config.duckdbS3UseCredentialChain || getEnv('duckdbS3UseCredentialChain', this.config);
    if (useCredentialChain) {
      try {
        await defaultConnection.run('CREATE SECRET (TYPE S3, PROVIDER \'CREDENTIAL_CHAIN\')');
      } catch (e) {
        if (this.logger) {
          console.error('DuckDB - error on creating S3 credential chain secret', { e });
        }
        throw e;
      }
    }

    // Install & load extensions if configured in env variable.
    const officialExtensions = getEnv('duckdbExtensions', this.config);
    await this.installExtensions(officialExtensions, defaultConnection);
    await this.loadExtensions(officialExtensions, defaultConnection);
    const communityExtensions = getEnv('duckdbCommunityExtensions', this.config);
    // @see https://duckdb.org/community_extensions/
    await this.installExtensions(communityExtensions, defaultConnection, 'community');
    await this.loadExtensions(communityExtensions, defaultConnection);

    if (this.config.initSql) {
      try {
        await defaultConnection.run(this.config.initSql);
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

    const reader = await defaultConnection.runAndReadAll(query, values as DuckDBValue[]);
    // Use custom converter to get Cube-friendly string values
    return reader.convertRowObjects(cubeValueConverter) as R[];
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
    const connection = await db.connect();

    try {
      const result = await connection.stream(query, values as DuckDBValue[]);

      // yieldConvertedRowObjects yields chunks of rows, so we need to flatten them
      const flattenRows = async function* flattenRows() {
        for await (const chunk of result.yieldConvertedRowObjects(cubeValueConverter)) {
          for (const row of chunk) {
            yield row;
          }
        }
      };

      const rowStream = stream.Readable.from(flattenRows(), { highWaterMark, objectMode: true });

      return {
        rowStream,
        release: async () => {
          connection.closeSync();
        }
      };
    } catch (e) {
      connection.closeSync();

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
      const { defaultConnection } = await this.initPromise;
      this.initPromise = null;

      // Close the default connection
      defaultConnection.closeSync();

      // Note: DuckDBInstance is auto-managed and doesn't have an explicit close method
      // The instance will be cleaned up automatically when all connections are closed
    }
  }
}
