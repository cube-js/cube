/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview The `KsqlDriver` and related types declaration.
 */

import {
  getEnv,
  assertDataSource,
} from '@cubejs-backend/shared';
import {
  BaseDriver, DriverCapabilities,
  DriverInterface, TableColumn,
} from '@cubejs-backend/base-driver';
import { Kafka } from 'kafkajs';
import sqlstring, { format as formatSql } from 'sqlstring';
import axios, { AxiosResponse } from 'axios';
import { Mutex } from 'async-mutex';
import { KsqlQuery } from './KsqlQuery';

type KsqlDriverOptions = {
  url: string,
  username: string,
  password: string,
  kafkaHost?: string,
  kafkaUser?: string,
  kafkaPassword?: string,
  kafkaUseSsl?: boolean,
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
    windowType: 'SESSION' | 'HOPPING' | 'TUMBLING',
    partitions: number;
    topic: string;
  }
};

type KsqlQueryOptions = {
  outputColumnTypes?: TableColumn[],
  streamOffset?: string,
  selectStatement?: string,
};

/**
 * KSQL driver class.
 */
export class KsqlDriver extends BaseDriver implements DriverInterface {
  /**
   * Returns default concurrency value.
   */
  public static getDefaultConcurrency(): number {
    return 1;
  }

  protected readonly config: KsqlDriverOptions;

  protected readonly dropTableMutex: Mutex = new Mutex();

  private readonly kafkaClient?: Kafka;

  /**
   * Class constructor.
   */
  public constructor(
    config: Partial<KsqlDriverOptions> & {
      /**
       * Data source name.
       */
      dataSource?: string,

      /**
       * Max pool size value for the [cube]<-->[db] pool.
       */
      maxPoolSize?: number,

      /**
       * Time to wait for a response from a connection after validation
       * request before determining it as not valid. Default - 10000 ms.
       */
      testConnectionTimeout?: number,
    } = {}
  ) {
    super({
      testConnectionTimeout: config.testConnectionTimeout,
    });

    const dataSource =
      config.dataSource ||
      assertDataSource('default');

    this.config = {
      url: getEnv('dbUrl', { dataSource }),
      username: getEnv('dbUser', { dataSource }),
      password: getEnv('dbPass', { dataSource }),
      kafkaHost: getEnv('dbKafkaHost', { dataSource }),
      kafkaUser: getEnv('dbKafkaUser', { dataSource }),
      kafkaPassword: getEnv('dbKafkaPass', { dataSource }),
      kafkaUseSsl: getEnv('dbKafkaUseSsl', { dataSource }),
      ...config,
    };

    if (this.config.kafkaHost) {
      this.kafkaClient = new Kafka({
        clientId: 'Cube',
        brokers: this.config.kafkaHost
          .split(',')
          .map(h => h.trim()),
        // authenticationTimeout: 10000,
        // reauthenticationThreshold: 10000,
        ssl: this.config.kafkaUseSsl,
        sasl: this.config.kafkaUser ? {
          mechanism: 'plain',
          username: this.config.kafkaUser,
          password: this.config.kafkaPassword || ''
        } : undefined,
      });
    }
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
      throw new Error(
        `ksql API error for '${
          body.ksql
        }': ${
          (<any>e).response?.data?.message ||
          (<any>e).response?.statusCode ||
          (<any>e).message ||
          (<any>e).toString()
        }`
      );
    }
  }

  public async query<R = unknown>(query: string, values?: unknown[], options: KsqlQueryOptions = {}): Promise<R> {
    if (query.toLowerCase().startsWith('select')) {
      throw new Error('Select queries for ksql allowed only from Cube Store. In order to query ksql create pre-aggregation first.');
    }
    const { data } = await this.apiQuery('/ksql', {
      ksql: `${formatSql(query, values)};`,
      ...(options.streamOffset ? {
        streamsProperties: {
          'ksql.streams.auto.offset.reset': options.streamOffset
        }
      } : {})
    });

    return data[0];
  }

  public async testConnection() {
    await this.query('SHOW VARIABLES');
    if (this.kafkaClient) {
      await this.kafkaClient.admin().connect();
      await this.kafkaClient.admin().disconnect();
    }
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

  public async tableColumnTypes(table: string, describe?: KsqlDescribeResponse) {
    describe = describe || await this.describeTable(table);

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

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  public loadPreAggregationIntoTable(preAggregationTableName: string, loadSql: string, params: any[], options: KsqlQueryOptions): Promise<any> {
    const { streamOffset } = options || {};
    return this.query(loadSql.replace(preAggregationTableName, this.tableDashName(preAggregationTableName)), params, { streamOffset });
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  public async downloadTable(table: string, options: any): Promise<any> {
    const { streamOffset } = options || {};
    return this.getStreamingTableData(this.tableDashName(table), { streamOffset });
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  public async downloadQueryResults(query: string, params: any, options: any) {
    const table = KsqlQuery.extractTableFromSimpleSelectAsteriskQuery(query);
    if (!table) {
      throw new Error('Unable to detect a source table for ksql download query. In order to query ksql use "SELECT * FROM <TABLE>"');
    }

    const selectStatement = sqlstring.format(query, params);
    const { streamOffset, outputColumnTypes } = options || {};
    return this.getStreamingTableData(table, { selectStatement, streamOffset, outputColumnTypes });
  }

  private async getStreamingTableData(streamingTable: string, options: KsqlQueryOptions = {}) {
    const { selectStatement, streamOffset, outputColumnTypes } = options;
    const describe = await this.describeTable(streamingTable);
    const name = this.config.streamingSourceName || 'default';
    const kafkaDirectDownload = !!this.config.kafkaHost;
    const streamingSource = kafkaDirectDownload ? {
      name: `${name}-kafka`,
      type: 'kafka',
      credentials: {
        user: this.config.kafkaUser,
        password: this.config.kafkaPassword,
        host: this.config.kafkaHost,
        use_ssl: this.config.kafkaUseSsl,
      }
    } : {
      name,
      type: 'ksql',
      credentials: {
        user: this.config.username,
        password: this.config.password,
        url: this.config.url
      }
    };
    const sourceTableTypes = await this.tableColumnTypes(streamingTable, describe);
    streamingTable = kafkaDirectDownload ? describe.sourceDescription?.topic : streamingTable;

    return {
      types: outputColumnTypes || sourceTableTypes,
      partitions: describe.sourceDescription?.partitions,
      streamingTable,
      streamOffset,
      selectStatement,
      streamingSource,
      sourceTable: outputColumnTypes ? {
        types: sourceTableTypes,
        tableName: streamingTable
      } : null
    };
  }

  private describeTable(streamingTable: string): Promise<KsqlDescribeResponse> {
    return this.query<KsqlDescribeResponse>(`DESCRIBE ${this.quoteIdentifier(this.tableDashName(streamingTable))}`);
  }

  public dropTable(tableName: string, options: any): Promise<any> {
    return this.dropTableMutex.runExclusive(
      async () => this.query(`DROP TABLE ${this.quoteIdentifier(this.tableDashName(tableName))} DELETE TOPIC`, [], options)
    );
  }

  public quoteIdentifier(identifier: string): string {
    return `\`${identifier}\``;
  }

  public static driverEnvVariables() {
    // TODO (buntarb): check how this method can/must be used with split
    // names by the data source.
    return [
      'CUBEJS_DB_URL',
      'CUBEJS_DB_USER',
      'CUBEJS_DB_PASS',
    ];
  }

  public static dialectClass() {
    return KsqlQuery;
  }

  public capabilities(): DriverCapabilities {
    return {
      streamingSource: true,
    };
  }
}
