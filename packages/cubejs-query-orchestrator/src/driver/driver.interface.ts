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
   * Optional function
   */
  release?: () => Promise<void>;
}

export type DownloadTableData = DownloadTableMemoryData | DownloadTableCSVData | StreamTableData;

export interface ExternalDriverCompatibilities {
  csvImport?: true,
  streamImport?: true,
}
export type DownloadQueryResults = ExternalDriverCompatibilities;

export type IndexesSQL = {
  sql: [string, unknown[]];
}[];

export interface DriverInterface {
  createSchemaIfNotExists(schemaName: string): Promise<any>;
  uploadTableWithIndexes(table: string, columns: TableStructure, tableData: DownloadTableData, indexesSql: IndexesSQL): Promise<void>;
  loadPreAggregationIntoTable: (preAggregationTableName: string, loadSql: string, params: any, options: any) => Promise<any>;
  //
  tableColumnTypes: (table: string) => Promise<TableStructure>;
  // Download whole table in memory
  downloadTable: (table: string, options: ExternalDriverCompatibilities) => Promise<DownloadTableMemoryData | DownloadTableCSVData>;
  // Some drivers can implement streaming, which don't load the whole table to the memory
  streamTable?: (table: string, options: ExternalDriverCompatibilities) => Promise<StreamTableData>;
}
