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
import { finished } from 'stream/promises';
import { DuckDBConnection, DuckDBInstance } from '@duckdb/node-api';

import { DuckDBQuery } from './DuckDBQuery';
import { transformRow } from './HydrationStream';
import { convertDuckDBParams, convertDuckDBValue } from './DuckDBValueConverters';

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
  instance: DuckDBInstance;
};

type ExecFn = (sql: string) => Promise<unknown>;

const DuckDBToGenericType: Record<string, GenericDataBaseType> = {
  // DATE_TRUNC returns DATE, but Cube Store still doesn't support DATE type
  // DuckDB's driver transform date/timestamp to Date object, but HydrationStream converts any Date object to ISO timestamp
  // That's why It's safe to use timestamp here
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

  private async installExtensions(extensions: string[], execAsync: ExecFn, repository: string = ''): Promise<void> {
    repository = repository ? ` FROM ${repository}` : '';

    for (const extension of extensions) {
      try {
        await execAsync(`INSTALL ${extension}${repository}`);
      } catch (e) {
        if (this.logger) {
          console.error(`DuckDB - error on installing ${extension}`, { e });
        }
        throw e;
      }
    }
  }

  private async loadExtensions(extensions: string[], execAsync: ExecFn): Promise<void> {
    for (const extension of extensions) {
      try {
        await execAsync(`LOAD ${extension}`);
      } catch (e) {
        if (this.logger) {
          console.error(`DuckDB - error on loading ${extension}`, { e });
        }
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

    let dbOptions: Record<string, string> | undefined;
    if (token) {
      dbOptions = { custom_user_agent: `Cube/${version}` };
    }

    const instance = await DuckDBInstance.create(dbUrl, dbOptions);
    let defaultConnection: DuckDBConnection | undefined;

    try {
      defaultConnection = await instance.connect();
      await this.configureConnection(defaultConnection);

      return { defaultConnection, instance };
    } catch (e) {
      try {
        defaultConnection?.closeSync();
      } finally {
        instance.closeSync();
      }

      throw e;
    }
  }

  private async configureConnection(connection: DuckDBConnection): Promise<void> {
    const execAsync: ExecFn = (sql: string) => connection.run(sql);

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

    const useCredentialChain = this.config.duckdbS3UseCredentialChain || getEnv('duckdbS3UseCredentialChain', this.config);
    if (useCredentialChain) {
      try {
        await execAsync('CREATE SECRET (TYPE S3, PROVIDER \'CREDENTIAL_CHAIN\')');
      } catch (e) {
        if (this.logger) {
          console.error('DuckDB - error on creating S3 credential chain secret', { e });
        }
        throw e;
      }
    }

    // Install & load extensions if configured in env variable.
    const officialExtensions = getEnv('duckdbExtensions', this.config);
    await this.installExtensions(officialExtensions, execAsync);
    await this.loadExtensions(officialExtensions, execAsync);
    const communityExtensions = getEnv('duckdbCommunityExtensions', this.config);
    // @see https://duckdb.org/community_extensions/
    await this.installExtensions(communityExtensions, execAsync, 'community');
    await this.loadExtensions(communityExtensions, execAsync);

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

    const reader = await defaultConnection.runAndReadAll(query, convertDuckDBParams(values));
    const rows = reader.convertRowObjects(convertDuckDBValue);

    return rows.map((item) => {
      transformRow(item);

      return item as R;
    });
  }

  public async stream(
    query: string,
    values: unknown[],
    { highWaterMark }: StreamOptions
  ): Promise<StreamTableData> {
    const { instance } = await this.getInitiatedState();

    // new connection, because stream can break with
    // Attempting to execute an unsuccessful or closed pending query result
    // PreAggregation queue has a concurrency limit, it's why pool is not needed here
    const connection = await instance.connect();
    let closed = false;
    const close = () => {
      if (!closed) {
        closed = true;
        connection.closeSync();
      }
    };

    try {
      const result = await connection.stream(query, convertDuckDBParams(values));

      // Chunks are fetched lazily, so the connection must outlive the iteration; closing it
      // in `finally` covers completion, destroy() and early `break` alike.
      const rows = async function* rows(): AsyncGenerator<Record<string, unknown>> {
        try {
          for await (const chunk of result.yieldConvertedRowObjects(convertDuckDBValue)) {
            for (const row of chunk) {
              transformRow(row);
              yield row;
            }
          }
        } finally {
          close();
        }
      };

      const rowStream = stream.Readable.from(rows(), { highWaterMark });
      // destroy() before the first read never enters the generator
      rowStream.once('close', close);

      return {
        rowStream,
        release: async () => {
          rowStream.destroy();
          await finished(rowStream, { cleanup: true }).catch(() => {
            // the consumer already received the destroy error
          });
        }
      };
    } catch (e) {
      close();

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
      const { initPromise } = this;
      this.initPromise = null;
      const { defaultConnection, instance } = await initPromise;

      try {
        defaultConnection.closeSync();
      } finally {
        instance.closeSync();
      }
    }
  }
}
