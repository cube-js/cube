import { Database } from 'duckdb';
import {
  BaseDriver,
  DriverInterface,
  GenericDataBaseType, StreamOptions,
  QueryOptions, StreamTableData,
} from '@cubejs-backend/base-driver';
import { DucksDBQuery } from './DucksDBQuery';
import { promisify } from 'util';
import * as stream from 'stream';
import { HydrationStream } from './HydrationStream';

const GenericTypeToPostgres: Record<GenericDataBaseType, string> = {
};

const PostgresToGenericType: Record<string, GenericDataBaseType> = {
};

export type DucksDBDriverConfiguration = {
};

export class DucksDBDriver extends BaseDriver implements DriverInterface {
  protected readonly config: DucksDBDriverConfiguration;

  protected readonly db: Database;

  public constructor(
    config: DucksDBDriverConfiguration,
  ) {
    super();

    this.config = config;
    this.db = new Database(':memory:');
  }

  public static dialectClass() {
    return DucksDBQuery;
  }

  public async query<R = unknown>(query: string, values: unknown[], _options?: QueryOptions): Promise<R[]> {
    const connection = this.db.connect();
    const executeQuery: (sql: string, ...args: any[]) => Promise<R[]> = promisify(connection.all).bind(connection) as any;

    return executeQuery(query, ...(values || []));
  }

  public async stream(
    query: string,
    values: unknown[],
    { highWaterMark }: StreamOptions
  ): Promise<StreamTableData> {
    const connection = this.db.connect();

    const asyncIterator = connection.stream(query, ...(values || []));
    const rowStream = stream.Readable.from(asyncIterator, { highWaterMark }).pipe(new HydrationStream());

    return {
      rowStream,
    };
  }

  public async testConnection(): Promise<void> {
    // nothing to do
  }

  public toGenericType(columnType: string): GenericDataBaseType {
    if (columnType in PostgresToGenericType) {
      return PostgresToGenericType[columnType];
    }

    return super.toGenericType(columnType);
  }

  public readOnly() {
    return false;
  }

  public async release(): Promise<void> {
    await promisify(this.db.close).bind(this);
  }

  public fromGenericType(columnType: string) {
    return GenericTypeToPostgres[columnType] || super.fromGenericType(columnType);
  }
}
