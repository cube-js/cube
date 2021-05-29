/* eslint-disable max-len */

export type GenericDataBaseType = string;

export interface TableColumn {
  name: string;
  type: GenericDataBaseType;
}
export type TableStructure = TableColumn[];

export interface DownloadTableMemoryData {
  rows: Record<string, unknown>[];
}

export interface DownloadTableCSVData {
  csvFile: string[];
}

export interface StreamTableData {
  rowStream: NodeJS.ReadableStream;
  /**
   * Some drivers know types of response
   */
  types?: TableStructure;
  /**
   * Optional function to release stream/cursor/connection
   */
  release?: () => Promise<void>;
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

export interface DriverInterface {
  createSchemaIfNotExists(schemaName: string): Promise<any>;
  uploadTableWithIndexes(table: string, columns: TableStructure, tableData: DownloadTableData, indexesSql: IndexesSQL): Promise<void>;
  loadPreAggregationIntoTable: (preAggregationTableName: string, loadSql: string, params: any, options: any) => Promise<any>;
  //
  tableColumnTypes: (table: string) => Promise<TableStructure>;
  // Download data from Query (for readOnly)
  downloadQueryResults: (query: string, values: unknown[], options: DownloadQueryResultsOptions) => Promise<DownloadQueryResultsBase & (DownloadTableMemoryData | DownloadTableCSVData | StreamTableData)>;
  // Download table
  downloadTable: (table: string, options: ExternalDriverCompatibilities) => Promise<DownloadTableMemoryData | DownloadTableCSVData>;
  // Some drivers can implement streaming from SQL
  stream?: (table: string, values: unknown[], options: StreamOptions) => Promise<StreamTableData>;
}
