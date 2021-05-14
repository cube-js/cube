/* eslint-disable max-len */

export type GenericDataBaseType = string;

export interface TableStructure {
  name: string;
  type: GenericDataBaseType;
}

export interface DownloadTableMemoryData {
  rows: object[];
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

export interface DriverInterface {
  createSchemaIfNotExists(schemaName: string): Promise<any>;
  loadPreAggregationIntoTable: (preAggregationTableName: string, loadSql: string, params: any, options: any) => Promise<any>;
  //
  tableColumnTypes: (table: string) => Promise<TableStructure>;
  // Download whole table in memory
  downloadTable: (table: string, options: ExternalDriverCompatibilities) => Promise<DownloadTableMemoryData | DownloadTableCSVData>;
  // Some drivers can implement streaming, which don't load the whole table to the memory
  streamTable?: (table: string, options: ExternalDriverCompatibilities) => Promise<StreamTableData>;
}
