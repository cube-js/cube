/* eslint-disable no-restricted-syntax */
import {
  BaseDriver,
  DriverInterface,
} from '@cubejs-backend/base-driver';
import { format as formatSql } from 'sqlstring';
import axios, { AxiosResponse } from 'axios';
import { Mutex } from 'async-mutex';
import { KsqlQuery } from './KsqlQuery';

type KsqlDriverOptions = {
  url: string,
  username: string,
  password: string,
  streamingSourceName?: string,
};

type KsqlTable = {
  name: string,
  topic: string,
  format: string,
  type: string,
  isWindowed?: string,
};

type KsqlShowTablesResponse = {
  tables: KsqlTable[]
};

type KsqlShowStreamsResponse = {
  streams: KsqlTable[]
};

type KsqlField = {
  name: string;
  type?: 'KEY';
  schema: {
    type: string;
  };
};

type KsqlDescribeResponse = {
  sourceDescription: {
    name: string;
    fields: KsqlField[];
    type: 'STREAM' | 'TABLE';
    windowType: 'SESSION' | 'HOPPING' | 'TUMBLING'
  }
};

export class KsqlDriver extends BaseDriver implements DriverInterface {
  /**
   * Returns default concurrency value.
   */
  public static getDefaultConcurrency(): number {
    return 2;
  }

  protected readonly config: KsqlDriverOptions;

  protected readonly dropTableMutex: Mutex = new Mutex();

  public constructor(config: Partial<KsqlDriverOptions> = {}) {
    super();

    this.config = {
      url: <string>process.env.CUBEJS_DB_URL,
      username: <string>process.env.CUBEJS_DB_USER,
      password: <string>process.env.CUBEJS_DB_PASS,
      ...config,
    };
  }

  private async apiQuery(path: string, body: any): Promise<AxiosResponse> {
    const url = `${this.config.url}${path}`;
    try {
      return await axios.post(url, body, {
        auth: {
          username: this.config.username,
          password: this.config.password,
        },
      });
    } catch (e) {
      throw new Error(`ksql API error for '${body.ksql}': ${e.response?.data?.message || e.response?.statusCode}`);
    }
  }

  public async query<R = unknown>(query: string, values?: unknown[]): Promise<R> {
    if (query.toLowerCase().startsWith('select')) {
      throw new Error('Select queries for ksql allowed only from Cube Store. In order to query ksql create pre-aggregation first.');
    }
    const { data } = await this.apiQuery('/ksql', {
      ksql: `${formatSql(query, values)};`,
    });
    return data[0];
  }

  public async testConnection() {
    await this.query('SHOW VARIABLES');
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  public async createSchemaIfNotExists(schemaName: string): Promise<any> {
    // do nothing as there are no schemas in ksql
  }

  // eslint-disable-next-line camelcase
  private async fetchTables(schemaName?: string): Promise<({ tableName: string, tableSchema: string, fullTableName: string })[]> {
    const [tables, streams] = await Promise.all([
      this.query<KsqlShowTablesResponse>('SHOW TABLES'),
      this.query<KsqlShowStreamsResponse>('SHOW STREAMS'),
    ]);

    return ((tables.tables).concat(streams.streams))
      .map(t => t.name.split('-'))
      .filter(([schema, tableName]) => !schemaName || tableName && schema.toLowerCase() === schemaName.toLowerCase())
      .map((parts) => ({
        tableName: parts[1] || parts[0],
        tableSchema: parts[1] ? parts[0] : '',
        fullTableName: parts.join('-'),
      }));
  }

  // eslint-disable-next-line camelcase
  public async getTablesQuery(schemaName: string): Promise<({ table_name?: string, schema?: string })[]> {
    return (await this.fetchTables(schemaName)).filter(t => !!t.tableSchema).map(table => ({ table_name: table.tableName }));
  }

  public async tablesSchema(): Promise<any> {
    const tables = await this.fetchTables();

    const tablesAndDescribes = await Promise.all(tables.map(async table => ({
      table,
      describe: await this.query<KsqlDescribeResponse>(`DESCRIBE ${this.quoteIdentifier(table.fullTableName)}`),
    })));

    const schema: {
      [schemaName: string]: {
        [tableName: string]: { name: string, type: string }[]
      }
    } = {};

    tablesAndDescribes.forEach(({ table, describe }) => {
      schema[table.tableSchema] = schema[table.tableSchema] || {};
      schema[table.tableSchema][table.tableName] = describe.sourceDescription.fields.map(
        f => ({ name: f.name, type: f.schema.type })
      );
    });

    return schema;
  }

  public tableDashName(table: string) {
    return table.replace('.', '-');
  }

  public async tableColumnTypes(table: string) {
    const describe = await this.query<KsqlDescribeResponse>(`DESCRIBE ${this.quoteIdentifier(this.tableDashName(table))}`);

    let { fields } = describe.sourceDescription;
    if (describe.sourceDescription.windowType) {
      const fieldsUnderGroupBy = describe.sourceDescription.fields.filter(c => c.type === 'KEY');
      const fieldsRest = describe.sourceDescription.fields.filter(c => c.type !== 'KEY');
      fields = [
        ...fieldsUnderGroupBy,
        ...[
          { name: 'WINDOWSTART', schema: { type: 'INTEGER' } },
          { name: 'WINDOWEND', schema: { type: 'INTEGER' } },
        ] as KsqlField[],
        ...fieldsRest
      ];
    }

    return fields.map(c => ({ name: c.name, type: this.toGenericType(c.schema.type) }));
  }

  public loadPreAggregationIntoTable(preAggregationTableName: string, loadSql: string, params: any[], options: any): Promise<any> {
    return this.query(loadSql.replace(preAggregationTableName, this.tableDashName(preAggregationTableName)), params);
  }

  public async downloadTable(table: string, options: any): Promise<any> {
    return this.getStreamingTableData(this.tableDashName(table));
  }

  public async downloadQueryResults(query: string, params: any, options: any) {
    const table = KsqlQuery.extractTableFromSimpleSelectAsteriskQuery(query);
    if (!table) {
      throw new Error('Unable to detect a source table for ksql download query. In order to query ksql use "SELECT * FROM <TABLE>"');
    }

    return this.getStreamingTableData(table);
  }

  private async getStreamingTableData(streamingTable: string) {
    return {
      types: await this.tableColumnTypes(streamingTable),
      streamingTable,
      streamingSource: {
        name: this.config.streamingSourceName || 'default',
        type: 'ksql',
        credentials: {
          user: this.config.username,
          password: this.config.password,
          url: this.config.url
        }
      }
    };
  }

  public dropTable(tableName: string, options: any): Promise<any> {
    return this.dropTableMutex.runExclusive(
      async () => super.dropTable(this.quoteIdentifier(this.tableDashName(tableName)), options)
    );
  }

  public quoteIdentifier(identifier: string): string {
    return `\`${identifier}\``;
  }

  public static driverEnvVariables() {
    return [
      'CUBEJS_DB_URL',
      'CUBEJS_DB_USER',
      'CUBEJS_DB_PASS',
    ];
  }

  public static dialectClass() {
    return KsqlQuery;
  }
}
