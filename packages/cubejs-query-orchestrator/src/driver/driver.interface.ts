/* eslint-disable max-len */
export type GenericDataBaseType = string;

export interface TableColumn {
  name: string;
  type: GenericDataBaseType;
  attributes?: string[]
}
export type TableStructure = TableColumn[];
export type SchemaStructure = Record<string, TableStructure>;
export type DatabaseStructure = Record<string, SchemaStructure>;

// It's more easy to use this interface with optional method release as a base interface instead of type assertion
export interface DownloadTableBase {
  /**
   * Optional function to release stream/cursor/connection
   */
  release?: () => Promise<void>;
}

export interface DownloadTableMemoryData extends DownloadTableBase {
  rows: Record<string, unknown>[];
  /**
   * Some drivers know types of response
   */
  types?: TableStructure;
}

export interface DownloadTableCSVData extends DownloadTableBase {
  csvFile: string[];
  /**
   * Some drivers know types of response
   */
  types?: TableStructure;

  /**
   * Some drivers export csv files with no header row.
   */
  csvNoHeader?: boolean;
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

export type StreamTableDataWithTypes = StreamTableData & {
  /**
   * Some drivers know types of response
   */
  types: TableStructure;
};

export type DownloadTableData = DownloadTableMemoryData | DownloadTableCSVData | StreamTableData;

export interface ExternalDriverCompatibilities {
  csvImport?: true,
  streamImport?: true,
}
export type StreamOptions = {
  highWaterMark: number
};

export interface DownloadQueryResultsBase {
  types: TableStructure
}

export type DownloadQueryResultsOptions = StreamOptions & ExternalDriverCompatibilities;

export type IndexesSQL = {
  sql: [string, unknown[]];
}[];

export type UnloadOptions = {
  maxFileSize: number,
};

export type QueryOptions = {};
export type DownloadQueryResultsResult = DownloadQueryResultsBase & (DownloadTableMemoryData | DownloadTableCSVData | StreamTableData);

export interface DriverInterface {
  createSchemaIfNotExists(schemaName: string): Promise<any>;
  uploadTableWithIndexes(
    table: string, columns: TableStructure, tableData: DownloadTableData, indexesSql: IndexesSQL, uniqueKeyColumns: string[], queryTracingObj: any
  ): Promise<void>;
  loadPreAggregationIntoTable: (preAggregationTableName: string, loadSql: string, params: any, options: any) => Promise<any>;
  //
  query<R = unknown>(query: string, params: unknown[], options?: QueryOptions): Promise<R[]>;
  //
  tableColumnTypes: (table: string) => Promise<TableStructure>;
  // eslint-disable-next-line camelcase
  getTablesQuery: (schemaName: string) => Promise<({ table_name?: string, TABLE_NAME?: string })[]>;
  // Remove table from database
  dropTable: (tableName: string, options?: QueryOptions) => Promise<unknown>;
  // Download data from Query (for readOnly)
  downloadQueryResults: (query: string, values: unknown[], options: DownloadQueryResultsOptions) => Promise<DownloadQueryResultsResult>;
  // Download table
  downloadTable: (table: string, options: ExternalDriverCompatibilities) => Promise<DownloadTableMemoryData | DownloadTableCSVData>;
  // Some drivers can implement streaming from SQL
  stream?: (table: string, values: unknown[], options: StreamOptions) => Promise<StreamTableData>;
  // Some drivers can implement UNLOAD data to external storage
  unload?: (table: string, options: UnloadOptions) => Promise<DownloadTableCSVData>;
  // Some drivers can implement UNLOAD data to external storage
  isUnloadSupported?: (options: UnloadOptions) => Promise<boolean>;
  // Current timestamp, defaults to new Date().getTime()
  nowTimestamp(): number;
  // Shutdown the driver
  release(): Promise<void>
}
