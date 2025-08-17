import { pipeline, Writable } from 'stream';
import { createGzip } from 'zlib';
import { createWriteStream, createReadStream } from 'fs';
import { unlink } from 'fs-extra';
import tempy from 'tempy';
import csvWriter from 'csv-write-stream';
import {
  BaseDriver,
  DownloadTableCSVData,
  ExternalCreateTableOptions,
  DownloadTableMemoryData, DriverInterface, IndexesSQL, CreateTableIndex,
  StreamTableData,
  StreamingSourceTableData,
  QueryOptions,
  ExternalDriverCompatibilities, TableStructure, TableColumnQueryResult,
} from '@cubejs-backend/base-driver';
import { AsyncDebounce, getEnv } from '@cubejs-backend/shared';
import { format as formatSql, escape } from 'sqlstring';
import fetch from 'node-fetch';

import { ConnectionConfig } from './types';
import { WebSocketConnection } from './WebSocketConnection';

const GenericTypeToCubeStore: Record<string, string> = {
  string: 'varchar(255)',
  text: 'varchar(255)',
  uuid: 'varchar(64)',
  // Cube Store uses an old version of sql parser which doesn't support timestamp with custom precision, but
  // athena driver (I believe old version) allowed to use it
  'timestamp(3)': 'timestamp',
  // TODO comes from JDBC. We might consider decimal96 here
  bigdecimal: 'decimal'
};

type Column = {
  type: string;
  name: string;
};

type CreateTableOptions = {
  streamOffset?: string;
  inputFormat?: string
  buildRangeEnd?: string
  uniqueKey?: string
  indexes?: string
  files?: string[]
  aggregations?: string
  selectStatement?: string
  sourceTable?: any
  sealAt?: string
  delimiter?: string
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
      // We use ip here instead of localhost, because Node.js 18 resolve localhost to IPV6 by default
      // https://github.com/node-fetch/node-fetch/issues/1624
      host: config?.host || getEnv('cubeStoreHost') || '127.0.0.1',
      port: config?.port || getEnv('cubeStorePort') || '3030',
      user: config?.user || getEnv('cubeStoreUser'),
      password: config?.password || getEnv('cubeStorePass'),
    };
    this.baseUrl = (this.config.url || `ws://${this.config.host}:${this.config.port}/`).replace(/\/ws$/, '/').replace(/\/$/, '');
    this.connection = new WebSocketConnection(`${this.baseUrl}/ws`);
  }

  public async testConnection() {
    await this.query('SELECT 1', []);
  }

  public async query<R = any>(query: string, values: any[], options?: QueryOptions): Promise<R[]> {
    const { inlineTables, ...queryTracingObj } = options ?? {};
    const sql = formatSql(query, values || []);
    return this.connection.query(sql, inlineTables ?? [], { ...queryTracingObj, instance: getEnv('instanceId') });
  }

  public async release() {
    return this.connection.close();
  }

  public informationSchemaQuery() {
    return `
      SELECT columns.column_name as ${this.quoteIdentifier('column_name')},
             columns.table_name as ${this.quoteIdentifier('table_name')},
             columns.table_schema as ${this.quoteIdentifier('table_schema')},
             columns.data_type as ${this.quoteIdentifier('data_type')}
      FROM information_schema.columns as columns
      WHERE columns.table_schema NOT IN ('information_schema', 'system')`;
  }

  public createTableSqlWithOptions(tableName, columns, options: CreateTableOptions) {
    let sql = this.createTableSql(tableName, columns);
    const params: string[] = [];
    const withEntries: string[] = [];

    if (options.inputFormat) {
      withEntries.push(`input_format = '${options.inputFormat}'`);
    }
    if (options.delimiter) {
      withEntries.push(`delimiter = '${options.delimiter}'`);
    }
    if (options.buildRangeEnd) {
      withEntries.push(`build_range_end = '${options.buildRangeEnd}'`);
    }
    if (options.sealAt) {
      withEntries.push(`seal_at = '${options.sealAt}'`);
    }
    if (options.selectStatement) {
      withEntries.push(`select_statement = ${escape(options.selectStatement)}`);
    }
    if (options.sourceTable) {
      withEntries.push(`source_table = ${escape(`CREATE TABLE ${options.sourceTable.tableName} (${options.sourceTable.types.map(t => `${t.name} ${this.fromGenericType(t.type)}`).join(', ')})`)}`);
    }
    if (options.streamOffset) {
      withEntries.push(`stream_offset = '${options.streamOffset}'`);
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

  public createTableWithOptions(tableName: string, columns: Column[], options: CreateTableOptions, queryTracingObj: any) {
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

  @AsyncDebounce()
  public async getTablesQuery(schemaName) {
    return this.query(
      `SELECT table_name, build_range_end FROM information_schema.tables WHERE table_schema = ${this.param(0)}`,
      [schemaName]
    );
  }

  @AsyncDebounce()
  public async getPrefixTablesQuery(schemaName, tablePrefixes) {
    const prefixWhere = tablePrefixes.map(_ => 'table_name LIKE CONCAT(?, \'%\')').join(' OR ');
    return this.query(
      `SELECT table_name, build_range_end FROM information_schema.tables WHERE table_schema = ${this.param(0)} AND (${prefixWhere})`,
      [schemaName].concat(tablePrefixes)
    );
  }

  public async tableColumnTypes(table: string): Promise<TableStructure> {
    const [schema, name] = table.split('.');

    const columns = await this.query<TableColumnQueryResult>(
      `SELECT column_name as ${this.quoteIdentifier('column_name')},
             table_name as ${this.quoteIdentifier('table_name')},
             table_schema as ${this.quoteIdentifier('table_schema')},
             data_type as ${this.quoteIdentifier('data_type')}
      FROM information_schema.columns
      WHERE table_name = ${this.param(0)} AND table_schema = ${this.param(1)}`,
      [name, schema]
    );

    return columns.map(c => ({ name: c.column_name, type: this.toGenericType(c.data_type) }));
  }

  public quoteIdentifier(identifier: string): string {
    return `\`${identifier}\``;
  }

  public fromGenericType(columnType: string): string {
    return GenericTypeToCubeStore[columnType] || super.fromGenericType(columnType);
  }

  public toColumnValue(value: any, genericType: any) {
    if (genericType === 'timestamp' && typeof value === 'string') {
      return value?.replace('Z', '');
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

  public async uploadTableWithIndexes(table: string, columns: Column[], tableData: any, indexesSql: IndexesSQL, uniqueKeyColumns: string[] | null, queryTracingObj?: any, externalOptions?: ExternalCreateTableOptions) {
    const createTableIndexes = externalOptions?.createTableIndexes;
    const aggregationsColumns = externalOptions?.aggregationsColumns;

    const indexes = createTableIndexes?.length ? createTableIndexes.map(this.createIndexString).join(' ') : '';

    let hasAggregatingIndexes = false;
    if (createTableIndexes?.length) {
      hasAggregatingIndexes = createTableIndexes.some((index) => index.type === 'aggregate');
    }

    const aggregations = hasAggregatingIndexes && aggregationsColumns?.length ? ` AGGREGATIONS (${aggregationsColumns.join(', ')})` : '';

    if (tableData.rowStream) {
      await this.importStream(columns, tableData, table, indexes, aggregations, queryTracingObj);
    } else if (tableData.csvFile) {
      await this.importCsvFile(tableData, table, columns, indexes, aggregations, queryTracingObj);
    } else if (tableData.streamingSource) {
      await this.importStreamingSource(columns, tableData, table, indexes, uniqueKeyColumns, queryTracingObj, externalOptions?.sealAt);
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
    if (!columns || columns.length === 0) {
      throw new Error('Unable to import (as rows) in Cube Store: empty columns. Most probably, introspection has failed.');
    }

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
    if (!columns || columns.length === 0) {
      throw new Error('Unable to import (as csv) in Cube Store: empty columns. Most probably, introspection has failed.');
    }

    const files = Array.isArray(tableData.csvFile) ? tableData.csvFile : [tableData.csvFile];
    const options: CreateTableOptions = {
      buildRangeEnd: queryTracingObj?.buildRangeEnd,
      indexes,
      aggregations
    };
    if (files.length > 0) {
      options.inputFormat = tableData.csvNoHeader ? 'csv_no_header' : 'csv';
      if (tableData.csvDelimiter) {
        options.delimiter = tableData.csvDelimiter;
      }
      options.files = files;
    }

    return this.createTableWithOptions(table, columns, options, queryTracingObj);
  }

  private async importStream(columns: Column[], tableData: StreamTableData, table: string, indexes: string, aggregations: string, queryTracingObj?: any) {
    if (!columns || columns.length === 0) {
      throw new Error('Unable to import (as stream) in Cube Store: empty columns. Most probably, introspection has failed.');
    }

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

      this.createTableSql(table, columns);
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

  private async importStreamingSource(columns: Column[], tableData: StreamingSourceTableData, table: string, indexes: string, uniqueKeyColumns: string[] | null, queryTracingObj?: any, sealAt?: string) {
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

    let locations = [`stream://${tableData.streamingSource.name}/${tableData.streamingTable}`];

    if (tableData.partitions) {
      locations = [];
      for (let i = 0; i < tableData.partitions; i++) {
        locations.push(`stream://${tableData.streamingSource.name}/${tableData.streamingTable}/${i}`);
      }
    }

    const options: CreateTableOptions = {
      buildRangeEnd: queryTracingObj?.buildRangeEnd,
      uniqueKey: uniqueKeyColumns.join(','),
      indexes,
      files: locations,
      selectStatement: tableData.selectStatement,
      sourceTable: tableData.sourceTable,
      streamOffset: tableData.streamOffset,
      sealAt
    };
    return this.createTableWithOptions(table, columns, options, queryTracingObj);
  }

  public capabilities(): ExternalDriverCompatibilities {
    return {
      csvImport: true,
      streamImport: true,
    };
  }
}
