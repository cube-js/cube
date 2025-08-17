import Meta from './Meta';
import { TimeDimensionGranularity } from './time';
import { TransportOptions } from './HttpTransport';

export type QueryOrder = 'asc' | 'desc' | 'none';

export type TQueryOrderObject = { [key: string]: QueryOrder };
export type TQueryOrderArray = Array<[string, QueryOrder]>;

export type GranularityAnnotation = {
  name: string;
  title: string;
  interval: string;
  offset?: string;
  origin?: string;
};

export type Annotation = {
  title: string;
  shortTitle: string;
  type: string;
  meta?: any;
  format?: 'currency' | 'percent' | 'number';
  drillMembers?: any[];
  drillMembersGrouped?: any;
  granularity?: GranularityAnnotation;
};

export type QueryAnnotations = {
  dimensions: Record<string, Annotation>;
  measures: Record<string, Annotation>;
  timeDimensions: Record<string, Annotation>;
  segments: Record<string, Annotation>;
};

export type QueryType = 'regularQuery' | 'compareDateRangeQuery' | 'blendingQuery';

export type DateRange = string | [string, string];

export interface TimeDimensionBase {
  dimension: string;
  granularity?: TimeDimensionGranularity;
  dateRange?: DateRange;
}

export interface TimeDimensionComparison extends TimeDimensionBase {
  compareDateRange: Array<DateRange>;
}

export type TimeDimension = TimeDimensionBase | TimeDimensionComparison;

// eslint-disable-next-line no-use-before-define
export type Filter = BinaryFilter | UnaryFilter | LogicalOrFilter | LogicalAndFilter;

export type LogicalAndFilter = {
  and: Filter[];
};

export type LogicalOrFilter = {
  or: Filter[];
};

export type UnaryOperator = 'set' | 'notSet';

export type BinaryOperator =
  | 'equals'
  | 'notEquals'
  | 'contains'
  | 'notContains'
  | 'startsWith'
  | 'notStartsWith'
  | 'endsWith'
  | 'notEndsWith'
  | 'gt'
  | 'gte'
  | 'lt'
  | 'lte'
  | 'inDateRange'
  | 'notInDateRange'
  | 'beforeDate'
  | 'beforeOrOnDate'
  | 'afterDate'
  | 'afterOrOnDate';

export interface BinaryFilter {
  /**
   * @deprecated Use `member` instead.
   */
  dimension?: string;
  member?: string;
  operator: BinaryOperator;
  values: string[];
}

export interface UnaryFilter {
  /**
   * @deprecated Use `member` instead.
   */
  dimension?: string;
  member?: string;
  operator: UnaryOperator;
  values?: never;
}

export interface Query {
  measures?: string[];
  dimensions?: string[];
  filters?: Filter[];
  timeDimensions?: TimeDimension[];
  segments?: string[];
  limit?: null | number;
  rowLimit?: null | number;
  offset?: number;
  order?: TQueryOrderObject | TQueryOrderArray;
  timezone?: string;
  renewQuery?: boolean;
  ungrouped?: boolean;
  responseFormat?: 'compact' | 'default';
  total?: boolean;
}

export type PivotQuery = Query & {
  queryType: QueryType;
};

type LeafMeasure = {
  measure: string;
  additive: boolean;
  type: 'count' | 'countDistinct' | 'sum' | 'min' | 'max' | 'number' | 'countDistinctApprox'
};

export type TransformedQuery = {
  allFiltersWithinSelectedDimensions: boolean;
  granularityHierarchies: Record<string, string[]>;
  hasMultipliedMeasures: boolean;
  hasNoTimeDimensionsWithoutGranularity: boolean;
  isAdditive: boolean;
  leafMeasureAdditive: boolean;
  leafMeasures: string[];
  measures: string[];
  sortedDimensions: string[];
  sortedTimeDimensions: [[string, string]];
  measureToLeafMeasures?: Record<string, LeafMeasure[]>;
  ownedDimensions: string[];
  ownedTimeDimensionsAsIs: [[string, string | null]];
  ownedTimeDimensionsWithRollupGranularity: [[string, string]];
};

export type PreAggregationType = 'rollup' | 'rollupJoin' | 'rollupLambda' | 'originalSql';

export type UsedPreAggregation = {
  targetTableName: string;
  type: PreAggregationType;
};

export type LoadResponseResult<T> = {
  annotation: QueryAnnotations;
  lastRefreshTime: string;
  query: Query;
  data: T[];
  external: boolean | null;
  dbType: string;
  extDbType: string;
  requestId?: string;
  usedPreAggregations?: Record<string, UsedPreAggregation>;
  transformedQuery?: TransformedQuery;
  total?: number;
};

export type LoadResponse<T> = {
  queryType: QueryType;
  results: LoadResponseResult<T>[];
  pivotQuery: PivotQuery;
  slowQuery?: boolean;
  [key: string]: any;
};

export type PivotRow = {
  xValues: Array<string | number>;
  yValuesArray: Array<[string[], string]>;
};

export type Pivot = any;
//   {
//     xValues: any;
//     yValuesArray: any[];
// };

/**
 * Configuration object that contains information about pivot axes and other options.
 *
 * Let's apply `pivotConfig` and see how it affects the axes
 * ```js
 * // Example query
 * {
 *   measures: ['Orders.count'],
 *   dimensions: ['Users.country', 'Users.gender']
 * }
 * ```
 * If we put the `Users.gender` dimension on **y** axis
 * ```js
 * resultSet.tablePivot({
 *   x: ['Users.country'],
 *   y: ['Users.gender', 'measures']
 * })
 * ```
 *
 * The resulting table will look the following way
 *
 * | Users Country | male, Orders.count | female, Orders.count |
 * | ------------- | ------------------ | -------------------- |
 * | Australia     | 3                  | 27                   |
 * | Germany       | 10                 | 12                   |
 * | US            | 5                  | 7                    |
 *
 * Now let's put the `Users.country` dimension on **y** axis instead
 * ```js
 * resultSet.tablePivot({
 *   x: ['Users.gender'],
 *   y: ['Users.country', 'measures'],
 * });
 * ```
 *
 * in this case the `Users.country` values will be laid out on **y** or **columns** axis
 *
 * | Users Gender | Australia, Orders.count | Germany, Orders.count | US, Orders.count |
 * | ------------ | ----------------------- | --------------------- | ---------------- |
 * | male         | 3                       | 10                    | 5                |
 * | female       | 27                      | 12                    | 7                |
 *
 * It's also possible to put the `measures` on **x** axis. But in either case it should always be the last item of the array.
 * ```js
 * resultSet.tablePivot({
 *   x: ['Users.gender', 'measures'],
 *   y: ['Users.country'],
 * });
 * ```
 *
 * | Users Gender | measures     | Australia | Germany | US  |
 * | ------------ | ------------ | --------- | ------- | --- |
 * | male         | Orders.count | 3         | 10      | 5   |
 * | female       | Orders.count | 27        | 12      | 7   |
 */
export type PivotConfig = {
  joinDateRange?: ((pivots: Pivot[], joinDateRange: any) => PivotRow[]) | false;
  /**
   * Dimensions to put on **x** or **rows** axis.
   */
  x?: string[];
  /**
   * Dimensions to put on **y** or **columns** axis.
   */
  y?: string[];
  /**
   * If `true` missing dates on the time dimensions will be filled with fillWithValue or `0` by default for all measures.Note: the `fillMissingDates` option set to `true` will override any **order** applied to the query
   */
  fillMissingDates?: boolean | null;
  /**
   * Value to autofill all the missing date's measure.
   */
  fillWithValue?: string | number | null;
  /**
   * Give each series a prefix alias. Should have one entry for each query:measure. See [chartPivot](#result-set-chart-pivot)
   */
  aliasSeries?: string[];
};

export type PivotConfigFull = Omit<PivotConfig, 'x' | 'y'> & {
  x: string[];
  y: string[];
};

export type DrillDownLocator = {
  xValues: string[];
  yValues?: string[];
};

export type Series<T> = {
  key: string;
  title: string;
  shortTitle: string;
  series: T[];
};

export type Column = {
  key: string;
  title: string;
  series: [];
};

export type SeriesNamesColumn = {
  key: string;
  title: string;
  shortTitle: string;
  yValues: string[];
};

export type ChartPivotRow = {
  x: string;
  xValues: string[];
  [key: string]: any;
};

export type TableColumn = {
  key: string;
  dataIndex: string;
  meta?: any;
  type: string | number;
  title: string;
  shortTitle: string;
  format?: any;
  children?: TableColumn[];
};

export type SerializedResult<T = any> = {
  loadResponse: LoadResponse<T>;
};

export type ExtractTimeMember<T> =
  T extends { dimension: infer Dimension, granularity: infer Granularity }
    ? Dimension | `${Dimension & string}.${Granularity & string}`
    : never;

export type ExtractTimeMembers<T> =
  T extends readonly [infer First, ...infer Rest]
    ? ExtractTimeMember<First> | ExtractTimeMembers<Rest>
    : never;

export type MemberType = 'measures' | 'dimensions' | 'segments';

export type TOrderMember = {
  id: string;
  title: string;
  order: QueryOrder | 'none';
};

export type TCubeMemberType = 'time' | 'number' | 'string' | 'boolean';

// @see BaseCubeMember
// @deprecated
export type TCubeMember = {
  type: TCubeMemberType;
  name: string;
  title: string;
  shortTitle: string;
  description?: string;
  /**
   * @deprecated use `public` instead
   */
  isVisible?: boolean;
  public?: boolean;
  meta?: any;
};

export type BaseCubeMember = {
  type: TCubeMemberType;
  name: string;
  title: string;
  shortTitle: string;
  description?: string;
  /**
   * @deprecated use `public` instead
   */
  isVisible?: boolean;
  public?: boolean;
  meta?: any;
  aliasMember?: string;
};

export type TCubeMeasure = BaseCubeMember & {
  aggType: 'count' | 'number';
  cumulative: boolean;
  cumulativeTotal: boolean;
  drillMembers: string[];
  drillMembersGrouped: {
    measures: string[];
    dimensions: string[];
  };
  format?: 'currency' | 'percent';
};

export type CubeTimeDimensionGranularity = {
  name: string;
  title: string;
};

export type BaseCubeDimension = BaseCubeMember & {
  primaryKey?: boolean;
  suggestFilterValues: boolean;
};

export type CubeTimeDimension = BaseCubeDimension &
  { type: 'time'; granularities?: CubeTimeDimensionGranularity[] };

export type TCubeDimension =
  (BaseCubeDimension & { type: Exclude<BaseCubeDimension['type'], 'time'> }) |
  CubeTimeDimension;

export type TCubeSegment = Omit<BaseCubeMember, 'type'>;

export type NotFoundMember = {
  title: string;
  error: string;
};

export type TCubeMemberByType<T> = T extends 'measures'
  ? TCubeMeasure
  : T extends 'dimensions'
    ? TCubeDimension
    : T extends 'segments'
      ? TCubeSegment
      : never;

export type CubeMember = TCubeMeasure | TCubeDimension | TCubeSegment;

export type TCubeFolder = {
  name: string;
  members: string[];
};

export type TCubeNestedFolder = {
  name: string;
  members: (string | TCubeNestedFolder)[];
};

export type TCubeHierarchy = {
  name: string;
  title?: string;
  levels: string[];
  public?: boolean;
};

/**
 * @deprecated use DryRunResponse
 */
export type TDryRunResponse = {
  queryType: QueryType;
  normalizedQueries: Query[];
  pivotQuery: PivotQuery;
  queryOrder: Array<{ [k: string]: QueryOrder }>;
  transformedQueries: TransformedQuery[];
};

export type DryRunResponse = {
  queryType: QueryType;
  normalizedQueries: Query[];
  pivotQuery: PivotQuery;
  queryOrder: Array<{ [k: string]: QueryOrder }>;
  transformedQueries: TransformedQuery[];
};

export type Cube = {
  name: string;
  title: string;
  description?: string;
  measures: TCubeMeasure[];
  dimensions: TCubeDimension[];
  segments: TCubeSegment[];
  folders: TCubeFolder[];
  nestedFolders: TCubeNestedFolder[];
  hierarchies: TCubeHierarchy[];
  connectedComponent?: number;
  type?: 'view' | 'cube';
  /**
   * @deprecated use `public` instead
   */
  isVisible?: boolean;
  public?: boolean;
  meta?: any;
};

export type CubeMap = {
  measures: Record<string, TCubeMeasure>;
  dimensions: Record<string, TCubeDimension>;
  segments: Record<string, TCubeSegment>;
};

export type CubesMap = Record<
  string,
  CubeMap
>;

export type MetaResponse = {
  cubes: Cube[];
};

export type FilterOperator = {
  name: string;
  title: string;
};

export type TSourceAxis = 'x' | 'y';

export type ChartType = 'line' | 'bar' | 'table' | 'area' | 'number' | 'pie';

export type TDefaultHeuristicsOptions = {
  meta: Meta;
  sessionGranularity?: TimeDimensionGranularity;
};

export type TDefaultHeuristicsResponse = {
  shouldApplyHeuristicOrder: boolean;
  pivotConfig: PivotConfig | null;
  query: Query;
  chartType?: ChartType;
  sessionGranularity?: TimeDimensionGranularity | null;
};

export type TDefaultHeuristicsState = {
  query: Query;
  chartType?: ChartType;
};

export interface TFlatFilter {
  /**
   * @deprecated Use `member` instead.
   */
  dimension?: string;
  member?: string;
  operator: BinaryOperator | UnaryOperator;
  values?: string[];
}

export type ProgressResponse = {
  stage: string;
  timeElapsed: number;
};
