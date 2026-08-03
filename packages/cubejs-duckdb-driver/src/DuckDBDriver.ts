import {
  BaseDriver,
  DriverInterface,
  StreamOptions,
  QueryOptions,
  StreamTableData,
  GenericDataBaseType,
  TableStructure,
  TableColumnQueryResult,
  normalizeSqlPreamble,
  splitSqlPreamble,
} from '@cubejs-backend/base-driver';
import { getEnv } from '@cubejs-backend/shared';
import { promisify } from 'util';
import * as stream from 'stream';
import { Connection, Database } from 'duckdb';

import { DuckDBQuery } from './DuckDBQuery';
import { HydrationStream, transformRow } from './HydrationStream';

const { version } = require('../../package.json');

export type DuckDBDriverConfiguration = {
  databasePath?: string,
  dataSource?: string,
  /**
   * @deprecated Use `sqlPreamble`. Unlike `sqlPreamble`, failures here are
   * swallowed — kept for existing deployments that rely on that.
   */
  initSql?: string,
  sqlPreamble?: string,
  motherDuckToken?: string,
  schema?: string,
  duckdbS3UseCredentialChain?: boolean,
  preAggregations?: boolean,
  preAggregationsSqlPreamble?: boolean,
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

  private async installExtensions(extensions: string[], execAsync: (sql: string, ...params: any[]) => Promise<void>, repository: string = ''): Promise<void> {
    repository = repository ? ` FROM ${repository}` : '';
    for (const extension of extensions) {
      try {
        await execAsync(`INSTALL ${extension}${repository}`);
      } catch (e) {
        if (this.logger) {
          console.error(`DuckDB - error on installing ${extension}`, { e });
        }
        // DuckDB will lose connection_ref on connection on error, this will lead to broken connection object
        throw e;
      }
    }
  }

  private async loadExtensions(extensions: string[], execAsync: (sql: string, ...params: any[]) => Promise<void>): Promise<void> {
    for (const extension of extensions) {
      try {
        await execAsync(`LOAD ${extension}`);
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

    await this.applySqlPreamble(execAsync);

    return {
      defaultConnection,
      db
    };
  }

  // Resolved here rather than in BaseDriver, so the base accessor would
  // otherwise report no preamble to the pre-aggregation version key.
  public override effectiveSqlPreamble(): string | undefined {
    return this.configuredSqlPreamble();
  }

  private configuredSqlPreamble(): string | undefined {
    return normalizeSqlPreamble(this.config.sqlPreamble) ?? getEnv('dbSqlPreamble', {
      dataSource: this.config.dataSource ?? 'default',
      // Not `this.config.preAggregations`: a build resolves the preamble from
      // the pre-aggregation namespace even when its credentials do not.
      preAggregations: this.config.preAggregationsSqlPreamble ?? this.config.preAggregations,
    });
  }

  /**
   * Runs the preamble on the connection init() prepared.
   *
   * The two option names differ in failure posture on purpose: `initSql` has
   * always swallowed errors, and existing deployments may depend on that, while
   * a silently skipped `sqlPreamble` meant to define a UDF surfaces later as a
   * baffling "function does not exist". So the new name fails loudly and the
   * deprecated one keeps its old behaviour until it is removed.
   */
  private async applySqlPreamble(execAsync: (sql: string, ...params: any[]) => Promise<void>): Promise<void> {
    const preamble = this.configuredSqlPreamble();

    if (preamble) {
      await execAsync(preamble);
      return;
    }

    const legacy = normalizeSqlPreamble(this.config.initSql);

    if (legacy) {
      try {
        await execAsync(legacy);
      } catch (e) {
        if (this.logger) {
          console.error('DuckDB - error on init sql (skipping)', {
            e
          });
        }
      }
    }
  }

  /**
   * Replays the preamble on a connection this driver opened outside init().
   *
   * Everything a preamble creates in the catalog — macros, tables, secrets — is
   * shared across connections of the same Database, so those statements have
   * already taken effect and re-running them raises "already exists". Only
   * session-scoped settings need replaying, and there is no way to tell the two
   * apart without parsing SQL. So statements are replayed individually and an
   * already-applied one is skipped: the session settings land, and the catalog
   * objects that are already present stay usable.
   */
  private async replaySqlPreamble(execAsync: (sql: string, ...params: any[]) => Promise<void>): Promise<void> {
    const preamble = this.configuredSqlPreamble();
    // The legacy name swallows failures wherever it runs, so a streamed query
    // cannot start failing on an `initSql` statement that init() would have
    // skipped. Only `sqlPreamble` surfaces errors.
    const legacy = preamble ? undefined : normalizeSqlPreamble(this.config.initSql);

    for (const statement of splitSqlPreamble(preamble ?? legacy)) {
      try {
        await execAsync(statement);
      } catch (e) {
        if (legacy) {
          if (this.logger) {
            console.error('DuckDB - error on init sql (skipping)', { e });
          }
        } else if (!/already exists/i.test((e as Error)?.message ?? '')) {
          throw e;
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
      // This connection is not the one init() set up, so session-scoped parts of
      // the preamble have to be replayed or a streamed query would run without
      // them.
      await this.replaySqlPreamble(promisify(connection.exec).bind(connection) as any);

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
