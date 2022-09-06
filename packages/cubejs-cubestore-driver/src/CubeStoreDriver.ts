import { pipeline, Writable } from 'stream';
import { createGzip } from 'zlib';
import { createWriteStream, createReadStream } from 'fs';
import { unlink } from 'fs-extra';
import tempy from 'tempy';
import csvWriter from 'csv-write-stream';
import {
  BaseDriver,
  DownloadTableCSVData,
  DownloadTableMemoryData, DriverInterface, IndexesSQL, CreateTableIndex,
  StreamTableData,
  StreamingSourceTableData, QueryOptions,
} from '@cubejs-backend/query-orchestrator';
import { getEnv } from '@cubejs-backend/shared';
import { format as formatSql } from 'sqlstring';
import fetch from 'node-fetch';

import { CubeStoreQuery } from './CubeStoreQuery';
import { ConnectionConfig } from './types';
import { WebSocketConnection } from './WebSocketConnection';

const GenericTypeToCubeStore: Record<string, string> = {
  string: 'varchar(255)',
  text: 'varchar(255)',
  uuid: 'varchar(64)'
};

type Column = {
  type: string;
  name: string;
};

type CreateTableOptions = {
  inputFormat?: string
  buildRangeEnd?: string
  uniqueKey?: string
  indexes?: string
  files?: string[]
  aggregations?: string
};

export class CubeStoreDriver extends BaseDriver implements DriverInterface {
  protected readonly config: any;

  protected readonly connection: WebSocketConnection;

  protected readonly baseUrl: string;

  public constructor(config?: Partial<ConnectionConfig>) {
    super();

    this.config = {
      batchingRowSplitCount: getEnv('batchingRowSplitCount'),
      ...config,
      // TODO Can arrive as null somehow?
      host: config?.host || getEnv('cubeStoreHost'),
      port: config?.port || getEnv('cubeStorePort'),
      user: config?.user || getEnv('cubeStoreUser'),
      password: config?.password || getEnv('cubeStorePass'),
    };
    this.baseUrl = (this.config.url || `ws://${this.config.host || 'localhost'}:${this.config.port || '3030'}/`).replace(/\/ws$/, '/').replace(/\/$/, '');
    this.connection = new WebSocketConnection(`${this.baseUrl}/ws`);
  }

  public async testConnection() {
    await this.query('SELECT 1', []);
  }

  public async query(query: string, values: any[], options?: QueryOptions) {
    const { inlineTables, ...queryTracingObj } = options ?? {};
    return this.connection.query(formatSql(query, values || []), inlineTables ?? [], { ...queryTracingObj, instance: getEnv('instanceId') });
  }

  public async release() {
    return this.connection.close();
  }

  public informationSchemaQuery() {
    return `${super.informationSchemaQuery()} AND columns.table_schema = '${this.config.database}'`;
  }

  public createTableSqlWithOptions(tableName, columns, options: CreateTableOptions) {
    let sql = this.createTableSql(tableName, columns);
    const params: string[] = [];
    const withEntries: string[] = [];

    if (options.inputFormat) {
      withEntries.push(`input_format = '${options.inputFormat}'`);
    }
    if (options.buildRangeEnd) {
      withEntries.push(`build_range_end = '${options.buildRangeEnd}'`);
    }
    if (withEntries.length > 0) {
      sql = `${sql} WITH (${withEntries.join(', ')})`;
    }
    if (options.uniqueKey) {
      sql = `${sql} UNIQUE KEY (${options.uniqueKey})`;
    }
    if (options.aggregations) {
      sql = `${sql} ${options.aggregations}`;
    }
    if (options.indexes) {
      sql = `${sql} ${options.indexes}`;
    }
    if (options.files) {
      sql = `${sql} LOCATION ${options.files.map(() => '?').join(', ')}`;
      params.push(...options.files);
    }
    return sql;
  }

  public createTableWithOptions(tableName, columns, options: CreateTableOptions, queryTracingObj: any) {
    const sql = this.createTableSqlWithOptions(tableName, columns, options);
    const params: string[] = [];

    if (options.files) {
      params.push(...options.files);
    }

    return this.query(sql, params, queryTracingObj).catch(e => {
      e.message = `Error during create table: ${sql}: ${e.message}`;
      throw e;
    });
  }

  public async getTablesQuery(schemaName) {
    return this.query(
      `SELECT table_name, build_range_end FROM information_schema.tables WHERE table_schema = ${this.param(0)}`,
      [schemaName]
    );
  }

  public async getPrefixTablesQuery(schemaName, tablePrefixes) {
    const prefixWhere = tablePrefixes.map(_ => 'table_name LIKE CONCAT(?, \'%\')').join(' OR ');
    return this.query(
      `SELECT table_name, build_range_end FROM information_schema.tables WHERE table_schema = ${this.param(0)} AND (${prefixWhere})`,
      [schemaName].concat(tablePrefixes)
    );
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

  public async uploadTableWithIndexes(table: string, columns: Column[], tableData: any, indexesSql: IndexesSQL, uniqueKeyColumns?: string[], queryTracingObj?: any, aggregationsColumns?: string[], createTableIndexes?: CreateTableIndex[]) {
    const indexes = createTableIndexes && createTableIndexes.length ? createTableIndexes.map(this.createIndexString).join(' ') : '';

    let hasAggregatingIndexes = false;
    if (createTableIndexes && createTableIndexes.length) {
      hasAggregatingIndexes = createTableIndexes.some((index) => index.type === 'aggregate');
    }

    const aggregations = hasAggregatingIndexes && aggregationsColumns && aggregationsColumns.length ? ` AGGREGATIONS (${aggregationsColumns.join(', ')})` : '';

    if (tableData.rowStream) {
      await this.importStream(columns, tableData, table, indexes, aggregations, queryTracingObj);
    } else if (tableData.csvFile) {
      await this.importCsvFile(tableData, table, columns, indexes, aggregations, queryTracingObj);
    } else if (tableData.streamingSource) {
      await this.importStreamingSource(columns, tableData, table, indexes, uniqueKeyColumns, queryTracingObj);
    } else if (tableData.rows) {
      await this.importRows(table, columns, indexes, aggregations, tableData, queryTracingObj);
    } else {
      throw new Error(`Unsupported table data passed to ${this.constructor}`);
    }
  }

  private createIndexString(index: CreateTableIndex) {
    const prefix = {
      regular: '',
      aggregate: 'AGGREGATE '
    }[index.type] || '';
    return `${prefix}INDEX ${index.indexName} (${index.columns.join(',')})`;
  }

  private async importRows(table: string, columns: Column[], indexesSql: any, aggregations: any, tableData: DownloadTableMemoryData, queryTracingObj?: any) {
    await this.createTableWithOptions(table, columns, { indexes: indexesSql, aggregations, buildRangeEnd: queryTracingObj?.buildRangeEnd }, queryTracingObj);
    try {
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
          queryTracingObj
        );
      }
    } catch (e) {
      await this.dropTable(table);
      throw e;
    }
  }

  private async importCsvFile(tableData: DownloadTableCSVData, table: string, columns: Column[], indexes: any, aggregations: any, queryTracingObj?: any) {
    const files = Array.isArray(tableData.csvFile) ? tableData.csvFile : [tableData.csvFile];
    const options: CreateTableOptions = {
      buildRangeEnd: queryTracingObj?.buildRangeEnd,
      indexes,
      aggregations
    };
    if (files.length > 0) {
      options.inputFormat = tableData.csvNoHeader ? 'csv_no_header' : 'csv';
      options.files = files;
    }

    return this.createTableWithOptions(table, columns, options, queryTracingObj);
  }

  private async importStream(columns: Column[], tableData: StreamTableData, table: string, indexes: string, aggregations: string, queryTracingObj?: any) {
    const tempFiles: string[] = [];
    try {
      const pipelinePromises: Promise<any>[] = [];
      const filePromises: Promise<string>[] = [];
      let currentFileStream: { stream: NodeJS.WritableStream, tempFile: string } | null = null;

      const options: CreateTableOptions = {
        buildRangeEnd: queryTracingObj?.buildRangeEnd,
        indexes,
        aggregations
      };

      const { baseUrl } = this;
      let fileCounter = 0;

      const createTableSql = this.createTableSql(table, columns);
      // eslint-disable-next-line no-unused-vars
      const createTableSqlWithoutLocation = this.createTableSqlWithOptions(table, columns, options);

      const getFileStream = () => {
        if (!currentFileStream) {
          const writer = csvWriter({ headers: columns.map(c => c.name) });
          const tempFile = tempy.file();
          tempFiles.push(tempFile);
          const gzipStream = createGzip();
          pipelinePromises.push(new Promise((resolve, reject) => {
            pipeline(writer, gzipStream, createWriteStream(tempFile), (err) => {
              if (err) {
                reject(err);
              }

              const fileName = `${table}-${fileCounter++}.csv.gz`;
              filePromises.push(fetch(`${baseUrl.replace(/^ws/, 'http')}/upload-temp-file?name=${fileName}`, {
                method: 'POST',
                body: createReadStream(tempFile),
              }).then(async res => {
                if (res.status !== 200) {
                  const error = await res.json();
                  throw new Error(`Error during upload of ${fileName} create table: ${createTableSqlWithoutLocation}: ${error.error}`);
                }
                return fileName;
              }));

              resolve(null);
            });
            currentFileStream = { stream: writer, tempFile };
          }));
        }
        if (!currentFileStream) {
          throw new Error('Stream init error');
        }
        return currentFileStream;
      };

      let rowCount = 0;

      const endStream = (chunk, encoding, callback) => {
        const { stream } = getFileStream();
        currentFileStream = null;
        rowCount = 0;
        if (chunk) {
          stream.end(chunk, encoding, callback);
        } else {
          stream.end(callback);
        }
      };

      const { batchingRowSplitCount } = this.config;

      const outputStream = new Writable({
        write(chunk, encoding, callback) {
          rowCount++;
          if (rowCount >= batchingRowSplitCount) {
            endStream(chunk, encoding, callback);
          } else {
            getFileStream().stream.write(chunk, encoding, callback);
          }
        },
        final(callback: (error?: (Error | null)) => void) {
          endStream(null, null, callback);
        },
        objectMode: true
      });

      await new Promise(
        (resolve, reject) => pipeline(
          tableData.rowStream, outputStream, (err) => (err ? reject(err) : resolve(null))
        )
      );

      await Promise.all(pipelinePromises);

      const files = await Promise.all(filePromises);
      if (files.length > 0) {
        options.files = files.map(fileName => `temp://${fileName}`);
      }

      return this.createTableWithOptions(table, columns, options, queryTracingObj);
    } finally {
      await Promise.all(tempFiles.map(tempFile => unlink(tempFile)));
    }
  }

  private async importStreamingSource(columns: Column[], tableData: StreamingSourceTableData, table: string, indexes: string, uniqueKeyColumns?: string[], queryTracingObj?: any) {
    if (!uniqueKeyColumns) {
      throw new Error('Older version of orchestrator is being used with newer version of Cube Store driver. Please upgrade cube.js.');
    }
    await this.query(
      `CREATE SOURCE OR UPDATE ${this.quoteIdentifier(tableData.streamingSource.name)} as ? VALUES (${Object.keys(tableData.streamingSource.credentials).map(k => `${k} = ?`)})`,
      [tableData.streamingSource.type]
        .concat(
          Object.keys(tableData.streamingSource.credentials).map(k => tableData.streamingSource.credentials[k])
        ),
      queryTracingObj
    );

    const options: CreateTableOptions = {
      buildRangeEnd: queryTracingObj?.buildRangeEnd,
      uniqueKey: uniqueKeyColumns.join(','),
      indexes,
      files: [`stream://${tableData.streamingSource.name}/${tableData.streamingTable}`],
    };
    return this.createTableWithOptions(table, columns, options, queryTracingObj);
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
