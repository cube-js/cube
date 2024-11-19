import {
  DriverInterface,
  StreamOptions,
  StreamTableData,
  BaseDriver,
  DownloadQueryResultsOptions,
  DownloadQueryResultsResult,
} from '@cubejs-backend/base-driver';
import {
  getEnv,
  assertDataSource,
} from '@cubejs-backend/shared';

import SqlString from 'sqlstring';
import { Trino, BasicAuth } from 'trino-client';
import { PrestodbQuery } from '@cubejs-backend/schema-compiler/dist/src/adapter/PrestodbQuery';
import {
  map, zipObj, prop, concat
} from 'ramda';


export type TrinoDriverConfiguration = {
  server?: string;
  catalog?: string;
  schema?: string;
  auth?: BasicAuth;
  dataSource?: string;
};

export class TrinoDriver extends BaseDriver implements DriverInterface {
  public static getDefaultConcurrency() {
    return 2;
  }

  protected readonly config: TrinoDriverConfiguration;

  protected readonly client: Trino;

  public constructor(config: TrinoDriverConfiguration = {}) {
    super();

    const dataSource = config.dataSource || assertDataSource('default');

    this.config = {
      server: getEnv('dbHost', { dataSource }),
      catalog:
        getEnv('trinoCatalog', { dataSource }) ||
        getEnv('dbCatalog', { dataSource }),
      schema:
        getEnv('dbName', { dataSource }) ||
        getEnv('dbSchema', { dataSource }),
      auth: getEnv('dbPass', { dataSource }) ?
        new BasicAuth(getEnv('dbUser', { dataSource }), getEnv('dbPass', { dataSource })) :
        undefined,
      ...config,
    };

    this.client = Trino.create(this.config);
  }

  public async testConnection(): Promise<void> {
    const query = SqlString.format('SHOW SCHEMAS FROM ?', [this.config.catalog]);

    const schemas = await this.query(query, []);
    if (schemas.length === 0) {
      throw new Error(`Catalog not found: '${this.config.catalog}'`);
    }
  }

  public prepareQuery(query: string, values: unknown[]): string {
    return SqlString.format(
      query,
      (values || []).map((value) => (typeof value === 'string'
        ? {
          toSqlString: () => SqlString.escape(value).replace(/\\\\([_%])/g, '\\$1'),
        }
        : value))
    );
  }

  public async query(query: string, values: unknown[]): Promise<any[]> {
    const preparedQuery = this.prepareQuery(query, values);
    const iterator = await this.client.query(preparedQuery);

    const data: any[] = [];
    const columns: any[] = [];

    for await (const result of iterator) {
      if (columns.length === 0 && result.columns) {
        columns.push(...result.columns);
      }

      if (result.data) {
        data.push(...result.data);
      }
    }

    return this.normalizeResultOverColumns(data, columns);
  }

  public normalizeResultOverColumns(data: any[], columns: TableStructure) {
    const columnNames = map(prop('name'), columns || []);
    const arrayToObject = zipObj(columnNames);
    return map(arrayToObject, data || []);
  }

  public downloadQueryResults(
    query: string,
    values: unknown[],
    options: DownloadQueryResultsOptions
  ): Promise<DownloadQueryResultsResult> {
    if (options.streamImport) {
      return this.stream(query, values, options) as Promise<DownloadQueryResultsResult>;
    }
    return super.downloadQueryResults(query, values, options);
  }

  public static dialectClass() {
    return PrestodbQuery;
  }
}
