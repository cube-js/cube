import { BaseDriver } from '@cubejs-backend/query-orchestrator';
import { format as formatSql } from 'sqlstring';

import { CubeStoreQuery } from './CubeStoreQuery';
import { ConnectionConfig } from './types';
import { WebSocketConnection } from './WebSocketConnection';

const GenericTypeToCubeStore: Record<string, string> = {
  string: 'varchar(255)',
  text: 'varchar(255)'
};

export class CubeStoreDriver extends BaseDriver {
  protected readonly config: any;

  protected readonly connection: WebSocketConnection;

  public constructor(config?: Partial<ConnectionConfig>) {
    super();

    this.config = {
      host: process.env.CUBEJS_DB_HOST,
      port: process.env.CUBEJS_DB_PORT,
      user: process.env.CUBEJS_DB_USER,
      password: process.env.CUBEJS_DB_PASS,
      ...config,
    };
    this.connection = new WebSocketConnection(this.config);
  }

  public async testConnection() {
    await this.query('SELECT 1', []);
  }

  public async query(query, values) {
    return this.connection.query(formatSql(query, values || []));
  }

  public async release() {
    return this.connection.close();
  }

  public informationSchemaQuery() {
    return `${super.informationSchemaQuery()} AND columns.table_schema = '${this.config.database}'`;
  }

  public quoteIdentifier(identifier: string): string {
    return `\`${identifier}\``;
  }

  public fromGenericType(columnType: string): string {
    return GenericTypeToCubeStore[columnType] || super.fromGenericType(columnType);
  }

  public toColumnValue(value: any, genericType: any) {
    if (genericType === 'timestamp' && typeof value === 'string') {
      return value && value.replace('Z', '');
    }
    if (genericType === 'boolean' && typeof value === 'string') {
      if (value.toLowerCase() === 'true') {
        return true;
      }
      if (value.toLowerCase() === 'false') {
        return false;
      }
    }
    return super.toColumnValue(value, genericType);
  }

  public async uploadTableWithIndexes(table: any, columns: any, tableData: any, indexesSql: any) {
    if (tableData.csvFile) {
      const files = Array.isArray(tableData.csvFile) ? tableData.csvFile : [tableData.csvFile];
      const createTableSql = this.createTableSql(table, columns);
      const indexes =
        indexesSql.map((s: any) => s.sql[0].replace(/^CREATE INDEX (.*?) ON (.*?) \((.*)$/, 'INDEX $1 ($3')).join(' ');
      // eslint-disable-next-line no-unused-vars
      const createTableSqlWithLocation = `${createTableSql} ${indexes} LOCATION ${files.map(() => '?').join(', ')}`;
      await this.query(createTableSqlWithLocation, files).catch(e => {
        e.message = `Error during create table: ${createTableSqlWithLocation}: ${e.message}`;
        throw e;
      });
      return;
    }
    if (!tableData.rows) {
      throw new Error(`${this.constructor} driver supports only rows and CSV upload`);
    }
    await this.createTable(table, columns);
    try {
      for (let i = 0; i < indexesSql.length; i++) {
        const [query, params] = indexesSql[i].sql;
        await this.query(query, params);
      }
      const batchSize = 2000; // TODO make dynamic?
      for (let j = 0; j < Math.ceil(tableData.rows.length / batchSize); j++) {
        const currentBatchSize = Math.min(tableData.rows.length - j * batchSize, batchSize);
        const indexArray = Array.from({ length: currentBatchSize }, (v, i) => i);
        const valueParamPlaceholders =
          indexArray.map(i => `(${columns.map((c, paramIndex) => this.param(paramIndex + i * columns.length)).join(', ')})`).join(', ');
        const params = indexArray.map(i => columns
          .map(c => this.toColumnValue(tableData.rows[i + j * batchSize][c.name], c.type)))
          .reduce((a, b) => a.concat(b), []);

        await this.query(
          `INSERT INTO ${table}
        (${columns.map(c => this.quoteIdentifier(c.name)).join(', ')})
        VALUES ${valueParamPlaceholders}`,
          params
        );
      }
    } catch (e) {
      await this.dropTable(table);
      throw e;
    }
  }

  public static dialectClass() {
    return CubeStoreQuery;
  }

  public capabilities() {
    return {
      csvImport: true
    };
  }
}
