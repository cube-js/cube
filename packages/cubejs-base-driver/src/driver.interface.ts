/* eslint-disable max-len */
export type GenericDataBaseType = string;

export interface TableColumn {
  name: string;
  type: GenericDataBaseType;
  attributes?: string[]
}

export type ForeignKey = {
  // eslint-disable-next-line camelcase
  target_table: string;
  // eslint-disable-next-line camelcase
  target_column: string;
};

export interface TableColumnQueryResult {
  // eslint-disable-next-line camelcase
  column_name: string;
  // eslint-disable-next-line camelcase
  data_type: GenericDataBaseType;
  attributes?: string[]
  // eslint-disable-next-line camelcase
  foreign_keys?: ForeignKey[]
}

export type TableStructure = TableColumn[];
export type SchemaStructure = Record<string, TableStructure>;
export type DatabaseStructure = Record<string, SchemaStructure>;

export type Row = Record<string, unknown>;
export type Rows = Row[];
export interface InlineTable {
  name: string
  columns: TableStructure
  csvRows: string // in csv format
}
export type InlineTables = InlineTable[];

// It's easier to use this interface with optional method release as a base interface instead of type assertion
export interface DownloadTableBase {
  /**
   * Optional function to release stream/cursor/connection
   */
  release?: () => Promise<void>;
}

export interface TableMemoryData extends DownloadTableBase {
  rows: Rows;
  /**
   * Some drivers know types of response
   */
  types?: TableStructure;
}

export interface TableCSVData extends DownloadTableBase {
  /**
   * An array of unloaded CSV data temporary URLs.
   */
  csvFile: string[];

  /**
   * Unloaded data fields types.
   */
  types?: TableStructure;

  /**
   * Determine whether CSV file contains header or not.
   */
  csvNoHeader?: boolean;

  csvDelimiter?: string;

  /**
   * The CSV file escape symbol.
   */
  exportBucketCsvEscapeSymbol?: string;
}

export interface StreamTableData extends DownloadTableBase {
  rowStream: NodeJS.ReadableStream;
  /**
   * Some drivers know types of response
   */
  types?: TableStructure;
}

export interface StreamingSourceTableData extends DownloadTableBase {
  streamingTable: string;
  selectStatement?: string;
  sourceTable?: any,
  partitions?: number;
  streamOffset?: string;
  streamingSource: {
    name: string;
    type: string;
    credentials: any;
  };
  /**
   * Some drivers know types of response
   */
  types?: TableStructure;
}

export function isDownloadTableMemoryData(tableData: any): tableData is TableMemoryData {
  return Boolean(tableData.rows);
}

export function isDownloadTableCSVData(tableData: any): tableData is TableCSVData {
  return Boolean(tableData.csvFile);
}

export type DownloadTableData = TableMemoryData | TableCSVData | StreamTableData | StreamingSourceTableData;

export interface ExternalDriverCompatibilities {
  csvImport?: boolean,
  streamImport?: boolean,
}

export interface DriverCapabilities extends ExternalDriverCompatibilities {
  unloadWithoutTempTable?: boolean,
  streamingSource?: boolean,
  incrementalSchemaLoading?: boolean,
}

export type StreamOptions = {
  highWaterMark: number;
};

export type StreamingSourceOptions = {
  streamOffset?: boolean;
  outputColumnTypes?: TableColumn[]
};

export interface DownloadQueryResultsBase {
  types: TableStructure
}

export type DownloadQueryResultsOptions = StreamOptions & ExternalDriverCompatibilities & StreamingSourceOptions;

export type IndexesSQL = {
  sql: [string, unknown[]];
}[];

export type CreateTableIndex = {
  indexName: string,
  type: string,
  columns: string[]
};

type UnloadQuery = {
  sql: string,
  params: unknown[]
};

export type UnloadOptions = {
  maxFileSize: number,
  query?: UnloadQuery;
};

export type QueryOptions = {
  inlineTables?: InlineTables,
  [key: string]: any
};

export type ExternalCreateTableOptions = {
  aggregationsColumns?: string[],
  createTableIndexes?: CreateTableIndex[],
  sealAt?: string
};

export type DownloadTableMemoryData = TableMemoryData & DownloadQueryResultsBase;
export type DownloadTableCSVData = TableCSVData & DownloadQueryResultsBase;
export type DownloadStreamTableData = StreamTableData & DownloadQueryResultsBase;
export type DownloadStreamingSourceTableData = StreamingSourceTableData & DownloadQueryResultsBase;
export type DownloadQueryResultsResult = DownloadTableMemoryData | DownloadTableCSVData | DownloadStreamTableData | DownloadStreamingSourceTableData;

// OLD alias for DownloadStreamTableData
export type StreamTableDataWithTypes = DownloadStreamTableData;

// eslint-disable-next-line camelcase
export type TableQueryResult = { table_name?: string, TABLE_NAME?: string };

// eslint-disable-next-line camelcase
export type QuerySchemasResult = { schema_name: string };

// eslint-disable-next-line camelcase
export type QueryTablesResult = { schema_name: string, table_name: string };

// eslint-disable-next-line camelcase
export type QueryColumnsResult = { schema_name: string, table_name: string } & TableColumnQueryResult;

export type PrimaryKeysQueryResult = {
  // eslint-disable-next-line camelcase
  table_schema: string
  // eslint-disable-next-line camelcase
  table_name: string
  // eslint-disable-next-line camelcase
  column_name: string
};

export type ForeignKeysQueryResult = {
  // eslint-disable-next-line camelcase
  table_schema: string
  // eslint-disable-next-line camelcase
  table_name: string
  // eslint-disable-next-line camelcase
  column_name: string
  // eslint-disable-next-line camelcase
  target_table: string
  // eslint-disable-next-line camelcase
  target_column: string
};

export type TableKeysFilter = {
  tableSchema: string,
  tableName: string[]
};

export interface DriverInterface {
  createSchemaIfNotExists(schemaName: string): Promise<void>;
  uploadTableWithIndexes(
    table: string, columns: TableStructure, tableData: DownloadTableData, indexesSql: IndexesSQL, uniqueKeyColumns: string[], queryTracingObj: any, externalOptions: ExternalCreateTableOptions
  ): Promise<void>;
  loadPreAggregationIntoTable: (preAggregationTableName: string, loadSql: string, params: any, options: any) => Promise<any>;
  //
  query<R = unknown>(query: string, params: unknown[], options?: QueryOptions): Promise<R[]>;
  //
  tableColumnTypes: (table: string) => Promise<TableStructure>;
  queryColumnTypes: (sql: string, params: unknown[]) => Promise<{ name: any; type: string; }[]>;
  //
  getSchemas: () => Promise<QuerySchemasResult[]>;
  tablesSchema: () => Promise<any>;
  getTablesForSpecificSchemas: (schemas: QuerySchemasResult[]) => Promise<QueryTablesResult[]>;
  getColumnsForSpecificTables: (tables: QueryTablesResult[]) => Promise<QueryColumnsResult[]>;
  // eslint-disable-next-line camelcase
  getTablesQuery: (schemaName: string) => Promise<TableQueryResult[]>;
  // Remove table from database
  dropTable: (tableName: string, options?: QueryOptions) => Promise<unknown>;
  // Download data from Query (for readOnly)
  downloadQueryResults: (query: string, values: unknown[], options: DownloadQueryResultsOptions) => Promise<DownloadQueryResultsResult>;
  // Download table
  downloadTable: (table: string, options: ExternalDriverCompatibilities & StreamingSourceOptions) => Promise<TableMemoryData>;

  /**
   * Returns stream table object that includes query result stream and
   * queried fields types.
   */
  stream?: (table: string, values: unknown[], options: StreamOptions) => Promise<StreamTableData>;

  /**
   * Returns to the Cubestore an object with links to unloaded to an
   * export bucket data.
   */
  unload?: (table: string, options: UnloadOptions) => Promise<TableCSVData>;
  unloadFromQuery?: (sql: string, params: unknown[], options: UnloadOptions) => Promise<DownloadTableCSVData>;

  /**
   * Determines whether export bucket feature is configured or not.
   */
  isUnloadSupported?: (options: UnloadOptions) => Promise<boolean>;

  // Current timestamp, defaults to new Date().getTime()
  nowTimestamp(): number;
  // Shutdown the driver
  release(): Promise<void>

  capabilities(): DriverCapabilities;
}
