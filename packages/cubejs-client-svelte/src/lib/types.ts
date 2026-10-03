import type {
  BinaryFilter,
  BinaryOperator,
  CacheMode,
  ChartType,
  CubeApi,
  DateRange,
  DeeplyReadonly,
  DryRunResponse,
  Filter,
  FilterOperator,
  Meta,
  NotFoundMember,
  PivotConfig,
  ProgressResponse,
  Query,
  QueryOrder,
  QueryRecordType,
  ResultSet,
  SqlQuery,
  TCubeDimension,
  TCubeMeasure,
  TCubeSegment,
  TGranularityMap,
  TOrderMember,
  TSourceAxis,
  TimeDimension,
  TimeDimensionGranularity,
  UnaryFilter,
  UnaryOperator,
} from '@cubejs-client/core';
import type { Snippet } from 'svelte';
import type { Readable } from 'svelte/store';

export type Source<T> = T | Readable<T>;
export type QueryInput = DeeplyReadonly<Query | Query[]>;

export interface CubeProviderOptions {
  castNumerics?: boolean;
}

export interface CubeContextValue {
  cubeApi: Readable<CubeApi>;
  options: Readable<Readonly<CubeProviderOptions>>;
}

export interface CubeQueryOptions {
  cubeApi?: CubeApi;
  skip?: boolean;
  subscribe?: boolean;
  resetResultSetOnChange?: boolean;
  castNumerics?: boolean;
  cache?: CacheMode;
  baseRequestId?: string;
}

export interface CubeQueryState<TQuery extends QueryInput = QueryInput> {
  query: TQuery;
  previousQuery: TQuery | null;
  resultSet: ResultSet<QueryRecordType<TQuery>> | null;
  error: Error | null;
  isLoading: boolean;
  progress: ProgressResponse | null;
}

export interface CubeQueryRefetchOptions<TQuery extends QueryInput = QueryInput> {
  query?: TQuery;
  ignoreSkip?: boolean;
  baseRequestId?: string;
}

export interface CubeQueryStore<TQuery extends QueryInput = QueryInput>
  extends Readable<CubeQueryState<TQuery>> {
  refetch(options?: CubeQueryRefetchOptions<TQuery>): Promise<void>;
  start(): void;
  stop(): Promise<void>;
  destroy(): Promise<void>;
}

export interface CubeFetchOptions {
  cubeApi?: CubeApi;
  skip?: boolean;
  baseRequestId?: string;
}

export interface CubeFetchState<T> {
  response: T | null;
  error: Error | null;
  isLoading: boolean;
}

export interface CubeFetchStore<T, TRefetchOptions = void>
  extends Readable<CubeFetchState<T>> {
  refetch(options?: TRefetchOptions): Promise<void>;
  start(): void;
  stop(): Promise<void>;
  destroy(): Promise<void>;
}

export interface QueryFetchOptions extends CubeFetchOptions {
  query?: QueryInput;
}

export interface QueryFetchRefetchOptions {
  query?: QueryInput;
  ignoreSkip?: boolean;
  baseRequestId?: string;
}

export type LoadSql = boolean | 'only';

export interface QueryRendererOptions extends CubeQueryOptions {
  loadSql?: LoadSql;
}

export interface QueryRendererState<TResult = ResultSet | null> {
  resultSet: TResult;
  error: Error | null;
  isLoading: boolean;
  loadingState: { isLoading: boolean };
  progress: ProgressResponse | null;
  sqlQuery: SqlQuery | SqlQuery[] | null;
  refetch(): Promise<void>;
}

export type NamedQueries = Readonly<Record<string, DeeplyReadonly<Query>>>;

export type QueryRendererInput =
  | { query: QueryInput; queries?: never }
  | { query?: never; queries: NamedQueries };

export interface QueryRendererControllerState {
  resultSet: ResultSet | Record<string, ResultSet> | null;
  error: Error | null;
  isLoading: boolean;
  progress: ProgressResponse | null;
  sqlQuery: SqlQuery | SqlQuery[] | null;
}

export interface QueryRendererController
  extends Readable<QueryRendererControllerState> {
  refetch(): Promise<void>;
  start(): void;
  stop(): Promise<void>;
  destroy(): Promise<void>;
}

interface SharedQueryRendererProps {
  cubeApi?: CubeApi;
  skip?: boolean;
  resetResultSetOnChange?: boolean;
  castNumerics?: boolean;
  cache?: CacheMode;
  baseRequestId?: string;
}

export type SingleQueryRendererProps = SharedQueryRendererProps & {
  query: QueryInput;
  queries?: never;
  loadSql?: LoadSql;
  subscribe?: boolean;
  children?: Snippet<[QueryRendererState<ResultSet | null>]>;
};

export type NamedQueryRendererProps = SharedQueryRendererProps & {
  query?: never;
  queries: NamedQueries;
  loadSql?: false;
  subscribe?: false;
  children?: Snippet<
    [QueryRendererState<Record<string, ResultSet> | null>]
  >;
};

export interface VizState {
  query: Query;
  chartType: ChartType;
  pivotConfig: PivotConfig;
}

export interface QueryBuilderProposedState {
  query: Query;
  chartType?: ChartType;
  pivotConfig?: PivotConfig | null;
}

export interface HeuristicsContext {
  meta: Meta;
  sessionGranularity: TimeDimensionGranularity;
}

export interface QueryBuilderHeuristicResult extends QueryBuilderProposedState {
  shouldApplyHeuristicOrder?: boolean;
  sessionGranularity?: TimeDimensionGranularity | null;
}

export type StateChangeHeuristics = (
  current: Readonly<VizState>,
  proposed: Readonly<QueryBuilderProposedState>,
  context: Readonly<HeuristicsContext>
) => QueryBuilderHeuristicResult | null | undefined;

export type SelectedMember<T> =
  | (T & { index: number })
  | (NotFoundMember & { name: string; index: number });

export type SelectedMeasure = SelectedMember<TCubeMeasure>;
export type SelectedDimension = SelectedMember<TCubeDimension>;
export type SelectedSegment = SelectedMember<TCubeSegment>;

export type SelectedTimeDimension = Omit<TimeDimension, 'dimension'> & {
  index: number;
  dimension: (TCubeDimension | NotFoundMember) & {
    name: string;
    granularities: TGranularityMap[];
  };
};

export interface AvailableCube<T> {
  cubeName: string;
  cubeTitle: string;
  type: 'cube' | 'view';
  public: boolean;
  members: T[];
}

export interface AvailableMembers {
  measures: AvailableCube<TCubeMeasure>[];
  dimensions: AvailableCube<TCubeDimension>[];
  segments: AvailableCube<TCubeSegment>[];
  timeDimensions: AvailableCube<TCubeDimension>[];
}

export type TimeDimensionUpdate =
  | {
      dimension: string | TCubeDimension;
      granularity?: TimeDimensionGranularity;
      dateRange?: DateRange;
    }
  | {
      dimension: string | TCubeDimension;
      granularity?: TimeDimensionGranularity;
      compareDateRange: DateRange[];
    };

export type FilterMemberInput = string | TCubeDimension | TCubeMeasure;

export type FilterUpdate =
  | Filter
  | {
      member: FilterMemberInput;
      operator: UnaryOperator;
      values?: never;
    }
  | {
      member: FilterMemberInput;
      operator: BinaryOperator;
      values: string[];
    };

export type FilterPath = Array<number | 'and' | 'or'>;

export interface ResolvedFilter {
  filter: BinaryFilter | UnaryFilter;
  member: TCubeDimension | TCubeMeasure | NotFoundMember;
  operators: FilterOperator[];
  index: number;
  path: FilterPath;
}

export interface MemberUpdater<TAdd, TSelected = { index: number }> {
  add(member: TAdd): void;
  remove(member: TSelected): void;
  update(member: TSelected, updateWith: TAdd): void;
  set(members: TAdd[]): void;
}

export interface MovePivotItemArgs {
  sourceIndex: number;
  destinationIndex: number;
  sourceAxis: TSourceAxis;
  destinationAxis: TSourceAxis;
}

export interface SchemaChangeEvent {
  schemaVersion: number | string;
  refresh(): Promise<void>;
}

export interface QueryBuilderOptions {
  cubeApi?: CubeApi;
  defaultQuery?: DeeplyReadonly<Query>;
  defaultChartType?: ChartType;
  initialVizState?: Partial<VizState>;
  disableHeuristics?: boolean;
  stateChangeHeuristics?: StateChangeHeuristics;
  onVizStateChanged?: (vizState: Readonly<VizState>) => void;
  executeQuery?: boolean;
  subscribe?: boolean;
  resetResultSetOnChange?: boolean;
  castNumerics?: boolean;
  cache?: CacheMode;
  baseRequestId?: string;
  schemaVersion?: number | string;
  onSchemaChange?: (event: SchemaChangeEvent) => void;
}

export interface QueryBuilderState {
  query: Query;
  validatedQuery: Query | null;
  isQueryPresent: boolean;
  isValidated: boolean;
  chartType: ChartType;
  pivotConfig: PivotConfig;
  meta: Meta | null;
  isFetchingMeta: boolean;
  metaError: Error | null;
  richMetaError: unknown;
  metaErrorStack: string | null;
  dryRunError: Error | null;
  loadError: Error | null;
  error: Error | null;
  dryRunResponse: DryRunResponse | null;
  missingMembers: string[];
  measures: SelectedMeasure[];
  dimensions: SelectedDimension[];
  segments: SelectedSegment[];
  timeDimensions: SelectedTimeDimension[];
  filters: ResolvedFilter[];
  filterTree: Filter[];
  orderMembers: TOrderMember[];
  availableMeasures: TCubeMeasure[];
  availableDimensions: TCubeDimension[];
  availableTimeDimensions: TCubeDimension[];
  availableSegments: TCubeSegment[];
  availableMembers: AvailableMembers;
  availableFilterMembers: Array<
    AvailableCube<TCubeMeasure | TCubeDimension>
  >;
  resultSet: ResultSet | null;
  isLoading: boolean;
  loadingState: { isLoading: boolean };
  progress: ProgressResponse | null;
}

export interface QueryBuilderActions {
  updateQuery(patch: Partial<Query>): void;
  replaceQuery(query: Query): void;
  updateMeasures: MemberUpdater<TCubeMeasure, SelectedMeasure>;
  updateDimensions: MemberUpdater<TCubeDimension, SelectedDimension>;
  updateSegments: MemberUpdater<TCubeSegment, SelectedSegment>;
  updateTimeDimensions: MemberUpdater<TimeDimensionUpdate, SelectedTimeDimension>;
  updateFilters: MemberUpdater<FilterUpdate, ResolvedFilter>;
  updateChartType(chartType: ChartType): void;
  updateOrder: {
    set(memberId: string, order?: QueryOrder | 'none'): void;
    update(order: Query['order']): void;
    reorder(sourceIndex: number, destinationIndex: number): void;
  };
  updatePivotConfig: {
    moveItem(args: MovePivotItemArgs): void;
    update(patch: Partial<PivotConfig>): void;
    replace(config: PivotConfig): void;
  };
  setLimit(limit: number | null): void;
  setOffset(offset: number | null): void;
  refreshMeta(): Promise<void>;
  refetch(): Promise<void>;
}

export interface QueryBuilderStore extends Readable<QueryBuilderState> {
  readonly actions: QueryBuilderActions;
  start(): void;
  stop(): Promise<void>;
  destroy(): Promise<void>;
}

export type QueryBuilderRenderProps = QueryBuilderState & QueryBuilderActions;
