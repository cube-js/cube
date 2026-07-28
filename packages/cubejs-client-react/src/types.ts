/**
 * Types used by the implementation.
 *
 * The public API of this package is declared in the hand-written `index.d.ts`
 * (referenced by `package.json#types`), which stays the source of truth for
 * everything consumers see. The types here mirror it, plus a few internal ones
 * describing state and options that are not part of the public API.
 *
 * `test/public-api.ts` asserts that the implementation still satisfies
 * `index.d.ts`, so the two cannot drift apart silently.
 */
import type * as React from 'react';
import type {
  BinaryOperator,
  CacheMode,
  CubeApi,
  DateRange,
  DeeplyReadonly,
  DryRunResponse,
  Filter,
  LoadMethodOptions,
  MemberType,
  Meta,
  PivotConfig,
  ProgressResponse,
  Query,
  QueryOrder,
  ResultSet,
  SqlQuery,
  TCubeDimension,
  TCubeMeasure,
  TCubeSegment,
  TimeDimension,
  TimeDimensionGranularity,
  TOrderMember,
  TSourceAxis,
  UnaryOperator,
} from '@cubejs-client/core';

export type CubeProviderOptions = {
  castNumerics?: boolean;
};

export type CubeProviderProps = {
  cubeApi: CubeApi | null;
  options?: CubeProviderOptions;
  children: React.ReactNode;
};

export type CubeContextProps = {
  cubeApi: CubeApi;
  options?: CubeProviderOptions;
};

export type TLoadingState = {
  isLoading: boolean;
};

export type ChartType = 'line' | 'bar' | 'table' | 'area' | 'number' | 'pie';

export type VizState = {
  query?: Query;
  pivotConfig?: PivotConfig;
  chartType?: ChartType;
};

export type QueryRendererRenderProps = {
  resultSet: ResultSet | null;
  error: Error | null;
  loadingState: TLoadingState;
  sqlQuery: SqlQuery | null;
};

export type QueryRendererProps = {
  query: Query | Query[];
  queries?: { [key: string]: Query };
  loadSql?: 'only' | boolean;
  resetResultSetOnChange?: boolean;
  updateOnlyOnStateChange?: boolean;
  cubeApi?: CubeApi;
  cache?: CacheMode;
  /**
   * The public declaration types this as returning `void`, which is a supertype
   * of `ReactNode` and so stays compatible with what is rendered here.
   */
  render: (renderProps: QueryRendererRenderProps) => React.ReactNode;
  children?: never;
};

/**
 * `resultSet` holds a map of result sets keyed by query name when the `queries`
 * prop is used, and a single result set otherwise.
 */
export type QueryRendererState = {
  isLoading?: boolean;
  error?: (Error & { response?: { plainError?: string } }) | null;
  sqlQuery?: SqlQuery | null;
  resultSet?: ResultSet | { [key: string]: ResultSet } | null;
  /**
   * Never assigned — `render()` has always read it from state rather than from
   * props, so the empty-object fallback for `resultSet` does not kick in.
   */
  queries?: { [key: string]: Query };
};

export type QueryRendererWithTotalsProps = Omit<QueryRendererProps, 'queries' | 'query'> & {
  query: Query;
};

export type SchemaChangeProps = {
  schemaVersion: number;
  refresh: () => Promise<void>;
};

export type QueryBuilderState = VizState & {
  query?: Query;
};

export type QueryBuilderProps = {
  cubeApi?: CubeApi;
  initialVizState?: VizState;
  onVizStateChanged?: (vizState: VizState) => void;
  defaultChartType?: ChartType;
  defaultQuery?: Query;
  disableHeuristics?: boolean;
  wrapWithQueryRenderer?: boolean;
  render: (renderProps: QueryBuilderRenderProps) => React.ReactNode;
  stateChangeHeuristics?: (state: QueryBuilderState, newState: QueryBuilderState) => QueryBuilderState;
  /**
   * @deprecated Controlled query
   */
  query?: Query;
  /**
   * @deprecated Controlled query setter
   */
  setQuery?: (query: Query) => void;
  /**
   * @deprecated Controlled vizState
   */
  vizState?: VizState;
  /**
   * @deprecated Controlled vizState setter
   */
  setVizState?: (vizState: VizState) => void;
  schemaVersion?: number;
  queryVersion?: number | string;
  onSchemaChange?: (props: SchemaChangeProps) => void;
};

export type QueryBuilderRenderProps = {
  resultSet?: ResultSet | null;
  error?: Error | null;
  loadingState?: TLoadingState;

  meta: Meta | undefined;
  metaError?: Error | null;
  richMetaError?: Error | null;
  metaErrorStack?: string | null;
  isFetchingMeta: boolean;
  isQueryPresent: boolean;
  measures: (TCubeMeasure & { index: number })[];
  dimensions: (TCubeDimension & { index: number })[];
  segments: (TCubeSegment & { index: number })[];
  timeDimensions: (TimeDimensionWithExtraFields & { index: number })[];

  availableMembers: AvailableMembers;

  availableFilterMembers: Array<AvailableCube<TCubeMeasure> | AvailableCube<TCubeDimension>>;
  availableMeasures: TCubeMeasure[];
  availableDimensions: TCubeDimension[];
  availableTimeDimensions: TCubeDimension[];
  availableSegments: TCubeSegment[];

  updateMeasures: MeasureUpdater;
  updateDimensions: DimensionUpdater;
  updateSegments: SegmentUpdater;
  updateTimeDimensions: TimeDimensionUpdater;
  updateFilters: FilterUpdater;
  updateQuery: (query: Query) => void;
  filters: (FilterWithExtraFields & { index: number })[];
  orderMembers: TOrderMember[];
  updateOrder: OrderUpdater;
  pivotConfig?: PivotConfig;
  updatePivotConfig: PivotConfigUpdater;
  chartType?: ChartType;
  updateChartType: (chartType: ChartType) => void;
  query: Query;
  validatedQuery: Query;
  refresh: () => void;
  missingMembers: string[];
  dryRunResponse?: DryRunResponse;
};

/**
 * The state `QueryBuilder` actually keeps. It is a superset of the public
 * `QueryBuilderState`: everything beyond `query`, `pivotConfig` and
 * `chartType` is internal bookkeeping.
 */
export type QueryBuilderInternalState = QueryBuilderState & {
  query: Query;
  validatedQuery?: Query;
  missingMembers: string[];
  isFetchingMeta: boolean;
  dryRunResponse?: DryRunResponse | null;
  meta?: Meta;
  metaError?: Error | null;
  richMetaError?: Error | null;
  metaErrorStack?: string | null;
  queryError?: Error | null;
  richQueryError?: Error | null;
  sessionGranularity?: TimeDimensionGranularity | null;
  shouldApplyHeuristicOrder?: boolean;
};

/**
 * A state update on its way through `updateVizState`. Heuristics may return a
 * partial state, and the missing pieces are filled in before it is applied.
 */
export type QueryBuilderStateUpdate = Partial<QueryBuilderInternalState>;

export type QueryMemberType = MemberType | 'timeDimensions';

export type AvailableMembers = {
  measures: AvailableCube<TCubeMeasure>[];
  dimensions: AvailableCube<TCubeDimension>[];
  segments: AvailableCube<TCubeSegment>[];
  timeDimensions: AvailableCube<TCubeDimension>[];
};

export type AvailableCube<T = any> = {
  type: 'cube' | 'view';
  public: boolean;
  cubeName: string;
  cubeTitle: string;
  members: T[];
};

export type UseCubeQueryOptions = {
  cubeApi?: CubeApi;
  skip?: boolean;
  subscribe?: boolean;
  resetResultSetOnChange?: boolean;
  castNumerics?: boolean;
  cache?: CacheMode;
};

export type UseCubeQueryResult<QueryInput, Data> = {
  error: Error | null;
  isLoading: boolean;
  // `Data` is unconstrained to mirror the public declaration, while `ResultSet`
  // requires a record type
  resultSet: ResultSet<Data extends Record<string, any> ? Data : any> | null;
  progress: ProgressResponse;
  previousQuery: QueryInput;
  refetch: () => Promise<void>;
};

export type CubeFetchOptions = {
  skip?: boolean;
  cubeApi?: CubeApi;
  query?: Query;
};

export type CubeFetchResult<T> = {
  isLoading: boolean;
  error: Error | null;
  response: T;
};

export type UseDryRunResult = CubeFetchResult<DryRunResponse>;

export type LoadLazyDryRunOptions = {
  query?: Query | Query[];
};

export type UseCubeSqlResponse = {
  sql: string;
};

/**
 * `CubeApi` methods `useCubeFetch` can dispatch to.
 */
export type CubeFetchMethod = 'meta' | 'sql' | 'dryRun';

/**
 * `useCubeFetch` accepts arrays of queries and a `baseRequestId`, neither of
 * which is part of the public `CubeFetchOptions`.
 */
export type UseCubeFetchOptions = Omit<CubeFetchOptions, 'query'> & {
  query?: Query | Query[];
  baseRequestId?: string;
  onlyViews?: boolean;
};

export type UseCubeFetchLoadOptions = {
  query?: Query | Query[];
  onlyViews?: boolean;
};

/**
 * Options of `useCubeMeta`, which forwards `onlyViews` to `CubeApi#meta`.
 */
export type CubeMetaFetchOptions = Omit<UseCubeFetchOptions, 'query'>;

/**
 * What `useCubeFetch` keeps in state. `response` is `null` until the first
 * request resolves, while the public `CubeFetchResult` type does not model it.
 */
export type CubeFetchState<T> = {
  isLoading: boolean;
  response: T | null;
};

export type UseCubeFetchResult<T> = CubeFetchResult<T> & {
  refetch: (options?: UseCubeFetchLoadOptions) => Promise<void>;
};

/**
 * `ProgressResult` keeps `progressResponse` private, but `useCubeQuery` has
 * always read it to expose the raw response.
 */
export type ProgressResultWithResponse = {
  progressResponse: ProgressResponse;
};

export type ProgressCallback = NonNullable<LoadMethodOptions['progressCallback']>;

export type MemberUpdater<T> = {
  add: (member: T) => void;
  remove: (member: { index: number }) => void;
  update: (member: { index: number }, updateWith: T) => void;
};

export type FilterExtraFields = {
  dimension: TCubeDimension | TCubeMeasure;
  operators: { name: string; title: string }[];
};

export type FilterWithExtraFields = Omit<Filter, 'dimension'> & FilterExtraFields;

export type GranularityOptions = {
  granularities: { name: string; title: string }[];
};

export type TimeDimensionExtraFields = {
  dimension: TCubeDimension & GranularityOptions;
};

export type TimeDimensionWithExtraFields = Omit<TimeDimension, 'dimension'> & TimeDimensionExtraFields;

export type DimensionUpdater = MemberUpdater<TCubeDimension>;
export type MeasureUpdater = MemberUpdater<TCubeMeasure>;
export type SegmentUpdater = MemberUpdater<TCubeSegment>;

export type TimeDimensionRangedUpdateFields = {
  granularity?: TimeDimensionGranularity;
  dateRange?: DateRange;
  dimension: TCubeDimension;
};

export type TimeDimensionComparisonUpdateFields = {
  granularity?: TimeDimensionGranularity;
  compareDateRange: Array<DateRange>;
  dimension: TCubeDimension;
};

export type TimeDimensionUpdater = MemberUpdater<
  TimeDimensionRangedUpdateFields | TimeDimensionComparisonUpdateFields
>;

export type FilterUpdateFields = {
  member?: string;
  operator: BinaryOperator | UnaryOperator;
  values?: string[];
  dimension: TCubeDimension | TCubeMeasure;
};

export type FilterUpdater = MemberUpdater<FilterUpdateFields>;

export type OrderUpdater = {
  set: (memberId: string, order: QueryOrder | 'none') => void;
  update: (order: Query['order']) => void;
  reorder: (sourceIndex: number, destinationIndex: number) => void;
};

export type PivotConfigUpdaterArgs = {
  sourceIndex: number;
  destinationIndex: number;
  sourceAxis: TSourceAxis;
  destinationAxis: TSourceAxis;
};

export type PivotConfigExtraUpdateFields = {
  limit?: number;
};

export type PivotConfigUpdater = {
  moveItem: (args: PivotConfigUpdaterArgs) => void;
  update: (pivotConfig: PivotConfig & PivotConfigExtraUpdateFields) => void;
};

/**
 * A query as accepted by `useCubeQuery`.
 */
export type ReadonlyQueryInput = DeeplyReadonly<Query | Query[]>;
