import { pipeline } from 'stream';
import { createGzip } from 'zlib';
import csvWriter from 'csv-write-stream';
import { BaseDriver } from '@cubejs-backend/query-orchestrator';
import { format as formatSql } from 'sqlstring';
import fetch from 'node-fetch';

import { CubeStoreQuery } from './CubeStoreQuery';
import { ConnectionConfig } from './types';
import { WebSocketConnection } from './WebSocketConnection';

const GenericTypeToCubeStore: Record<string, string> = {
  string: 'varchar(255)',
  text: 'varchar(255)'
};

type Column = {
  type: string;
  name: string;
};

export class CubeStoreDriver extends BaseDriver {
  protected readonly config: any;

  protected readonly connection: WebSocketConnection;

  protected readonly baseUrl: string;

  public constructor(config?: Partial<ConnectionConfig>) {
    super();

    this.config = {
      host: process.env.CUBEJS_DB_HOST,
      port: process.env.CUBEJS_DB_PORT,
      user: process.env.CUBEJS_DB_USER,
      password: process.env.CUBEJS_DB_PASS,
      ...config,
    };
    this.baseUrl = (this.config.url || `ws://${this.config.host || 'localhost'}:${this.config.port || '3030'}/`).replace(/\/ws$/, '/').replace(/\/$/, '');
    this.connection = new WebSocketConnection(`${this.baseUrl}/ws`);
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

  public async uploadTableWithIndexes(table: string, columns: Column[], tableData: any, indexesSql: any) {
    const indexes =
      indexesSql.map((s: any) => s.sql[0].replace(/^CREATE INDEX (.*?) ON (.*?) \((.*)$/, 'INDEX $1 ($3')).join(' ');

    if (tableData.rowStream) {
      await this.importStream(columns, tableData, table, indexes);
    } else if (tableData.csvFile) {
      await this.importCsvFile(tableData, table, columns, indexes);
    } else if (tableData.rows) {
      await this.importRows(table, columns, indexesSql, tableData);
    } else {
      throw new Error(`Unsupported table data passed to ${this.constructor}`);
    }
  }

  private async importRows(table: string, columns: Column[], indexesSql: any, tableData: any) {
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
          params,
        );
      }
    } catch (e) {
      await this.dropTable(table);
      throw e;
    }
  }

  private async importCsvFile(tableData: any, table: string, columns: Column[], indexes) {
    const files = Array.isArray(tableData.csvFile) ? tableData.csvFile : [tableData.csvFile];
    const createTableSql = this.createTableSql(table, columns);
    // eslint-disable-next-line no-unused-vars
    const createTableSqlWithLocation = `${createTableSql} ${indexes} LOCATION ${files.map(() => '?').join(', ')}`;
    await this.query(createTableSqlWithLocation, files).catch(e => {
      e.message = `Error during create table: ${createTableSqlWithLocation}: ${e.message}`;
      throw e;
    });
  }

  private async importStream(columns: Column[], tableData: any, table: string, indexes) {
    try {
      const writer = csvWriter({ headers: columns.map(c => c.name) });
      const gzipStream = createGzip();

      await new Promise(
        (resolve, reject) => pipeline(
          tableData.rowStream, writer, gzipStream, (err) => (err ? reject(err) : resolve(null))
        )
      );
      const fileName = `${table}.csv.gz`;
      const res = await fetch(`${this.baseUrl.replace(/^ws/, 'http')}/upload-temp-file?name=${fileName}`, {
        method: 'POST',
        body: gzipStream,
      });

      const createTableSql = this.createTableSql(table, columns);
      // eslint-disable-next-line no-unused-vars
      const createTableSqlWithLocation = `${createTableSql} ${indexes} LOCATION ?`;

      if (res.status !== 200) {
        const err = await res.json();
        throw new Error(`Error during create table: ${createTableSqlWithLocation}: ${err.error}`);
      }
      await this.query(createTableSqlWithLocation, [`temp://${fileName}`]).catch(e => {
        e.message = `Error during create table: ${createTableSqlWithLocation}: ${e.message}`;
        throw e;
      });
    } finally {
      await tableData.release();
    }
  }

  public static dialectClass() {
    return CubeStoreQuery;
  }

  public capabilities() {
    return {
      csvImport: true,
      streamImport: true,
    };
  }
}
