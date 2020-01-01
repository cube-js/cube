export interface TransportInterface {
  request(method: string, params: any): () => Promise<void>;
}

export type CubeJSApiOptions = {
  apiUrl: string;
  headers?: Record<string, string>;
  pollInterval?: number;
  transport?: TransportInterface;
};

export enum QueryOrderOptions {
  ASC = 'asc',
  DESC = 'desc',
}

export type Annotation = {
  title: string;
  shortTitle: string;
  type: string;
  format?: 'currency' | 'percentage';
};

export type QueryAnnotations = {
  dimensions: Record<string, Annotation>;
  measures: Record<string, Annotation>;
  timeDimensions: Record<string, Annotation>;
};

export type LoadResponse = {
  annotation: QueryAnnotations;
  lastRefreshTime: string;
  query: Query;
  data: any[];
};

export type PivotConfig = {
  x?: string[];
  y?: string[];
  fillMissingDates: boolean | null;
};

export class ResultSet<T extends {} = {}> {
  static measureFromAxis(axisValues: string[]): string;

  loadResponse: LoadResponse;

  new(loadResponse: LoadResponse): ResultSet;

  series(pivotConfig: PivotConfig): T[];
  chartPivot(pivotConfig: PivotConfig): T[];
  tablePivot(pivotConfig: PivotConfig): T[];
}

export type Filter = {
  dimension?: string;
  member?: string;
  operator: string;
  values?: string[];
};

export enum TimeDimensionGranularities {
  HOUR = 'hour',
  DAY = 'day',
  WEEK = 'week',
  MONTH = 'month',
  YEAR = 'year',
}

export type TimeDimension = {
  dimension: string;
  dateRange?: string | string[];
  granularity?: TimeDimensionGranularities;
};

export type Query = {
  measures?: string[];
  dimensions?: string[];
  filters?: Filter[];
  timeDimensions?: TimeDimension[];
  segments?: string[];
  limit?: number;
  offset?: number;
  order?: {
    [key: string]: QueryOrderOptions;
  };
  timezone?: string;
  renewQuery?: boolean;
  ungrouped?: boolean;
};

export class CubejsApi {
  new(apiToken: string, options: CubeJSApiOptions): CubejsApi;

  load(query: Query, options, callback): void;
  load(query: Query, options): Promise<ResultSet>;

  sql(query: Query, options, callback): void;
  sql(query: Query, options): Promise<ResultSet>;

  meta(options, callback): void;
  meta(options): Promise<ResultSet>;
}

declare function cubejs(
  apiToken: string,
  options: CubeJSApiOptions,
): CubejsApi;

export default cubejs;
