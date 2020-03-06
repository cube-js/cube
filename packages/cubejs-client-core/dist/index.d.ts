export interface TransportInterface {
  request(method: string, params: any): () => Promise<void>;
}

export type CubeJSApiOptions = {
  apiUrl: string;
  headers?: Record<string, string>;
  pollInterval?: number;
  transport?: TransportInterface;
};

export type LoadMethodOptions = {
  mutexKey?: string;
  mutexObj?: {};
  progressCallback(result: ProgressResult): void;
  subscribe?: boolean;
};

export type LoadMethodCallback<T> = (error: Error | null, resultSet: T) => void;

export type QueryOrder = 'asc' | 'desc';

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

export type LoadResponse<T> = {
  annotation: QueryAnnotations;
  lastRefreshTime: string;
  query: Query;
  data: T[];
};

export type PivotConfig = {
  x?: string[];
  y?: string[];
  fillMissingDates?: boolean | null;
};

export type Column = {
  key: string;
  title: string;
}

export class ResultSet<T = any> {
  static measureFromAxis(axisValues: string[]): string;

  loadResponse: LoadResponse<T>;

  new(loadResponse: LoadResponse<T>): ResultSet;

  series(pivotConfig?: PivotConfig): T[];
  seriesNames(pivotConfig?: PivotConfig): Column[];

  chartPivot(pivotConfig?: PivotConfig): T[];

  tablePivot(pivotConfig?: PivotConfig): T[];
  tableColumns(pivotConfig?: PivotConfig): Column[];
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
    [key: string]: QueryOrder;
  };
  timezone?: string;
  renewQuery?: boolean;
  ungrouped?: boolean;
};

export type ProgressResponse = {
  stage: string;
  timeElapsed: number;
}

export class ProgressResult {
  new(progressResponse: ProgressResponse): ProgressResult;

  stage(): string;
  timeElapsed(): string;
}

type PrimitiveValue = boolean | string | number;
export type SqlQueryTuple = [string, PrimitiveValue];

export type SqlData = {
  aliasNameToMember: Record<string, string>;
  cacheKeyQueries: {
    queries: SqlQueryTuple[];
  };
  dataSource: boolean;
  external: boolean;
  sql: SqlQueryTuple;
};

export type SqlApiResponse = {
  sql: SqlData;
}

export class SqlQuery {
  new(sqlQuery: SqlApiResponse): SqlQuery;

  rawQuery(): SqlData;
  sql(): string;
}

export class Meta {
  new(metaResponse: {}): Meta;
}

export class CubejsApi {
  new(apiToken: string, options: CubeJSApiOptions): CubejsApi;

  load(query: Query, options?: LoadMethodOptions): Promise<ResultSet>;
  load(query: Query, options?: LoadMethodOptions, callback?: LoadMethodCallback<ResultSet>): void;

  sql(query: Query, options?: LoadMethodOptions): Promise<SqlQuery>;
  sql(query: Query, options?: LoadMethodOptions, callback?: LoadMethodCallback<SqlQuery>): void;

  meta(options?: LoadMethodOptions): Promise<Meta>;
  meta(options?: LoadMethodOptions, callback?: LoadMethodCallback<Meta>): void;
}

declare function cubejs(
  apiToken: string,
  options: CubeJSApiOptions,
): CubejsApi;

export default cubejs;
