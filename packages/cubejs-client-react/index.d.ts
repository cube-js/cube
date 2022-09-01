/**
 * @title @cubejs-client/react
 * @permalink /@cubejs-client-react
 * @menuCategory Frontend Integrations
 * @subcategory Reference
 * @menuOrder 3
 * @description `@cubejs-client/react` provides React Components for easy Cube.js integration in a React app.
 */

declare module '@cubejs-client/react' {
  import * as React from 'react';
  import {
    CubejsApi,
    Query,
    ResultSet,
    SqlQuery,
    Filter,
    PivotConfig,
    TCubeMeasure,
    TCubeDimension,
    ProgressResponse,
    DryRunResponse,
    TOrderMember,
    QueryOrder,
    TSourceAxis,
    Meta,
    TCubeSegment,
    TimeDimension,
    TimeDimensionGranularity,
    DateRange,
    UnaryOperator,
    BinaryOperator,
    DeeplyReadonly,
    QueryRecordType,
  } from '@cubejs-client/core';

  type CubeProviderProps = {
    cubejsApi: CubejsApi | null;
    children: React.ReactNode;
  };

  /**
   * Cube.js context provider
   * ```js
   * import React from 'react';
   * import cubejs from '@cubejs-client/core';
   * import { CubeProvider } from '@cubejs-client/react';
   *
   * const API_URL = 'https://harsh-eel.aws-us-east-2.cubecloudapp.dev';
   * const CUBEJS_TOKEN =
   *   'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.* eyJpYXQiOjE1OTE3MDcxNDgsImV4cCI6MTU5NDI5OTE0OH0.* n5jGLQJ14igg6_Hri_Autx9qOIzVqp4oYxmX27V-4T4';
   *
   * const cubejsApi = cubejs(CUBEJS_TOKEN, {
   *   apiUrl: `${API_URL}/cubejs-api/v1`,
   * });
   *
   * export default function App() {
   *   return (
   *     <CubeProvider cubejsApi={cubejsApi}>
   *       //...
   *     </CubeProvider>
   *   )
   * }
   * ```
   * @stickyTypes
   * @order 10
   */
  export const CubeProvider: React.FC<CubeProviderProps>;

  type CubeContextProps = {
    cubejsApi: CubejsApi;
  };

  /**
   * In case when you need direct access to `cubejsApi` you can use `CubeContext` anywhere in your app
   *
   * ```js
   * import React from 'react';
   * import { CubeContext } from '@cubejs-client/react';
   *
   * export default function DisplayComponent() {
   *   const { cubejsApi } = React.useContext(CubeContext);
   *   const [rawResults, setRawResults] = React.useState([]);
   *   const query = {
   *     ...
   *   };
   *
   *   React.useEffect(() => {
   *     cubejsApi.load(query).then((resultSet) => {
   *       setRawResults(resultSet.rawData());
   *     });
   *   }, [query]);
   *
   *   return (
   *     <>
   *       {rawResults.map(row => (
   *         ...
   *       ))}
   *     </>
   *   )
   * }
   * ```
   */
  export const CubeContext: React.Context<CubeContextProps>;

  type TLoadingState = {
    isLoading: boolean;
  };

  type QueryRendererRenderProps = {
    resultSet: ResultSet | null;
    error: Error | null;
    loadingState: TLoadingState;
    sqlQuery: SqlQuery | null;
  };

  type QueryRendererProps = {
    /**
     * Analytic query. [Learn more about it's format](query-format)
     */
    query: Query | Query[];
    queries?: { [key: string]: Query };
    /**
     * Indicates whether the generated by `Cube.js` SQL Code should be requested. See [rest-api#sql](rest-api#api-reference-v-1-sql). When set to `only` then only the request to [/v1/sql](rest-api#api-reference-v-1-sql) will be performed. When set to `true` the sql request will be performed along with the query request. Will not be performed if set to `false`
     */
    loadSql?: 'only' | boolean;
    /**
     * When `true` the **resultSet** will be reset to `null` first on every state change
     */
    resetResultSetOnChange?: boolean;
    updateOnlyOnStateChange?: boolean;
    /**
     * `CubejsApi` instance to use
     */
    cubejsApi?: CubejsApi;
    /**
     * Output of this function will be rendered by the `QueryRenderer`
     */
    render: (renderProps: QueryRendererRenderProps) => void;
    /**
     * @hidden
     */
    children?: never;
  };

  /**
   * `<QueryRenderer />` a react component that accepts a query, fetches the given query, and uses the render prop to render the resulting data
   * @stickyTypes QueryRendererProps, QueryRendererRenderProps
   * @noInheritDoc
   */
  export class QueryRenderer extends React.Component<QueryRendererProps> {}

  export type ChartType = 'line' | 'bar' | 'table' | 'area' | 'number' | 'pie';

  type VizState = {
    query?: Query;
    pivotConfig?: PivotConfig;
    chartType?: ChartType;
  };

  type QueryBuilderProps = {
    /**
     * `CubejsApi` instance to use
     */
    cubejsApi?: CubejsApi;
    /**
     * State for the QueryBuilder to start with. Pass in the value previously saved from onVizStateChanged to restore a session.
     */
    initialVizState?: VizState;
    /**
     * Called by the `QueryBuilder` when the viz state has changed. Use it to save state outside of the `QueryBuilder` component.
     */
    onVizStateChanged?: (vizState: VizState) => void;
    /**
     * @default defaultChartType line (used when initialVizState is not set or does not contain chartType)
     */
    defaultChartType?: ChartType;
    /**
     * Default query (used when initialVizState is not set or does not contain query)
     */
    defaultQuery?: Query;
    /**
     * Defaults to `false`. This means that the default heuristics will be applied. For example: when the query is empty and you select a measure that has a default time dimension it will be pushed to the query.
     * @default disableHeuristics false
     */
    disableHeuristics?: boolean;
    wrapWithQueryRenderer?: boolean;
    render: (renderProps: QueryBuilderRenderProps) => React.ReactNode;
    /**
     * A function that accepts the `newState` just before it's applied. You can use it to override the **defaultHeuristics** or to tweak the query or the vizState in any way.
     */
    stateChangeHeuristics?: (state: QueryBuilderState, newState: QueryBuilderState) => QueryBuilderState;
    /**
     * @ignore @deprecated Controlled query
     */
    query?: Query;
    /**
     * @ignore @deprecated Controlled query setter
     */
    setQuery?: (query: Query) => void;
    /**
     * @ignore @deprecated Controlled vizState
     */
    vizState?: VizState;
    /**
     * @ignore @deprecated Controlled vizState setter
     */
    setVizState?: (vizState: VizState) => void;
    /**
     * @hidden
     */
    schemaVersion?: number;
    /**
     * @hidden
     */
    queryVersion?: number | string;
    /**
     * @hidden
     */
    onSchemaChange?: (props: SchemaChangeProps) => void;
  };

  /**
   * @hidden
   */
  type SchemaChangeProps = {
    schemaVersion: number;
    refresh: () => Promise<void>;
  };

  type QueryBuilderState = VizState & {
    query?: Query;
  };

  type QueryBuilderRenderProps = {
    // Todo: should fix DRY, duplicate props from QueryRendererRenderProps, see https://github.com/cube-js/cube.js/issues/1192
    resultSet?: ResultSet | null;
    error?: Error | null;
    loadingState?: TLoadingState;

    meta: Meta | undefined;
    metaError?: Error | null;
    richMetaError?: Error | null;
    metaErrorStack?: string | null;
    isFetchingMeta: boolean;
    /**
     * Indicates whether the query is ready to be displayed or not
     */
    isQueryPresent: boolean;
    measures: (TCubeMeasure & { index: number })[];
    dimensions: (TCubeDimension & { index: number })[];
    segments: (TCubeSegment & { index: number })[];
    timeDimensions: (TimeDimensionWithExtraFields & { index: number })[];

    availableMembers: AvailableMembers;

    availableFilterMembers: Array<AvailableCube<TCubeMeasure> | AvailableCube<TCubeDimension>>;
    /**
     * An array of available measures to select. They are loaded via the API from Cube.js Backend.
     */
    availableMeasures: TCubeMeasure[];
    /**
     * An array of available dimensions to select. They are loaded via the API from Cube.js Backend.
     */
    availableDimensions: TCubeDimension[];
    /**
     * An array of available time dimensions to select. They are loaded via the API from Cube.js Backend.
     */
    availableTimeDimensions: TCubeDimension[];
    /**
     * An array of available segments to select. They are loaded via the API from Cube.js Backend.
     */
    availableSegments: TCubeSegment[];

    updateMeasures: MeasureUpdater;
    updateDimensions: DimensionUpdater;
    updateSegments: SegmentUpdater;
    updateTimeDimensions: TimeDimensionUpdater;
    updateFilters: FilterUpdater;
    /**
     * Used for partial of full query update
     */
    updateQuery: (query: Query) => void;
    filters: (FilterWithExtraFields & { index: number })[];
    /**
     * All possible order members for the query
     */
    orderMembers: TOrderMember[];
    /**
     * Used for query order update
     */
    updateOrder: OrderUpdater;
    /**
     * See [Pivot Config](@cubejs-client-core#types-pivot-config)
     */
    pivotConfig?: PivotConfig;
    /**
     * Helper method for `pivotConfig` updates
     */
    updatePivotConfig: PivotConfigUpdater;

    /**
     * Selected chart type
     */
    chartType?: ChartType;

    /**
     * Used for chart type update
     */
    updateChartType: (chartType: ChartType) => void;
    query: Query;
    validatedQuery: Query;
    refresh: () => void;
    missingMembers: string[];
    dryRunResponse?: DryRunResponse;
  };

  export type AvailableMembers = {
    measures: AvailableCube<TCubeMeasure>[];
    dimensions: AvailableCube<TCubeDimension>[];
    segments: AvailableCube<TCubeSegment>[];
    timeDimensions: AvailableCube<TCubeDimension>[];
  };

  // todo: CubeMember
  export type AvailableCube<T = any> = {
    cubeName: string;
    cubeTitle: string;
    members: T[];
  };

  /**
   * `<QueryBuilder />` is used to build interactive analytics query builders. It abstracts state management and API calls to Cube.js Backend. It uses render prop technique and doesn’t render anything itself, but calls the render function instead.
   *
   * **Example**
   *
   * [Open in CodeSandbox](https://codesandbox.io/s/z6r7qj8wm)
   * ```js
   * import React from 'react';
   * import ReactDOM from 'react-dom';
   * import { Layout, Divider, Empty, Select } from 'antd';
   * import { QueryBuilder } from '@cubejs-client/react';
   * import cubejs from '@cubejs-client/core';
   * import 'antd/dist/antd.css';
   *
   * import ChartRenderer from './ChartRenderer';
   *
   * const cubejsApi = cubejs('YOUR-CUBEJS-API-TOKEN', {
   *   apiUrl: 'http://localhost:4000/cubejs-api/v1',
   * });
   *
   * const App = () => (
   *   <QueryBuilder
   *     query={{
   *       timeDimensions: [
   *         {
   *           dimension: 'LineItems.createdAt',
   *           granularity: 'month',
   *         },
   *       ],
   *     }}
   *     cubejsApi={cubejsApi}
   *     render={({ resultSet, measures, availableMeasures, updateMeasures }) => (
   *       <Layout.Content style={{ padding: '20px' }}>
   *         <Select
   *           mode="multiple"
   *           style={{ width: '100%' }}
   *           placeholder="Please select"
   *           onSelect={(measure) => updateMeasures.add(measure)}
   *           onDeselect={(measure) => updateMeasures.remove(measure)}
   *         >
   *           {availableMeasures.map((measure) => (
   *             <Select.Option key={measure.name} value={measure}>
   *               {measure.title}
   *             </Select.Option>
   *           ))}
   *         </Select>
   *         <Divider />
   *         {measures.length > 0 ? (
   *           <ChartRenderer resultSet={resultSet} />
   *         ) : (
   *           <Empty description="Select measure or dimension to get started" />
   *         )}
   *       </Layout.Content>
   *     )}
   *   />
   * );
   *
   * const rootElement = document.getElementById("root");
   * ReactDOM.render(<App />, rootElement);
   * ```
   * @stickyTypes QueryBuilderProps, QueryBuilderRenderProps, QueryBuilderState
   * @noInheritDoc
   * @order 2
   */
  export class QueryBuilder extends React.Component<QueryBuilderProps, QueryBuilderState> {}

  /**
   * A React hook for executing Cube.js queries
   * ```js
   * import React from 'react';
   * import { Table } from 'antd';
   * import { useCubeQuery }  from '@cubejs-client/react';
   *
   * export default function App() {
   *   const { resultSet, isLoading, error, progress } = useCubeQuery({
   *     measures: ['Orders.count'],
   *     dimensions: ['Orders.createdAt.month'],
   *   });
   *
   *   if (isLoading) {
   *     return <div>{progress?.stage || 'Loading...'}</div>;
   *   }
   *
   *   if (error) {
   *     return <div>{error.toString()}</div>;
   *   }
   *
   *   if (!resultSet) {
   *     return null;
   *   }
   *
   *   const dataSource = resultSet.tablePivot();
   *   const columns = resultSet.tableColumns();
   *
   *   return <Table columns={columns} dataSource={dataSource} />;
   * }
   *
   * ```
   * @order 1
   * @stickyTypes
   */
  export function useCubeQuery<
    TData,
    TQuery extends DeeplyReadonly<Query | Query[]> = DeeplyReadonly<Query | Query[]>>(
    query: TQuery,
    options?: UseCubeQueryOptions,
  ): UseCubeQueryResult<
    unknown extends TData
      ? QueryRecordType<TQuery>
      : TData
  >;

  type UseCubeQueryOptions = {
    /**
     * A `CubejsApi` instance to use. Taken from the context if the param is not passed
     */
    cubejsApi?: CubejsApi;
    /**
     * Query execution will be skipped when `skip` is set to `true`. You can use this flag to avoid sending incomplete queries.
     */
    skip?: boolean;
    /**
     * Use continuous fetch behavior. See [Real-Time Data Fetch](real-time-data-fetch)
     */
    subscribe?: boolean;
    /**
     * When `true` the resultSet will be reset to `null` first
     */
    resetResultSetOnChange?: boolean;
  };

  type UseCubeQueryResult<TData> = {
    error: Error | null;
    isLoading: boolean;
    resultSet: ResultSet<TData> | null;
    progress: ProgressResponse;
    refetch: () => Promise<void>;
  };

  /**
   * @hidden
   */
  type CubeFetchOptions = {
    skip?: boolean;
    cubejsApi?: CubejsApi;
    query?: Query;
  };

  /**
   * @hidden
   */
  type CubeFetchResult<T> = {
    isLoading: boolean;
    error: Error | null;
    response: T;
  };

  /**
   * @hidden
   */
  type UseDryRunResult = CubeFetchResult<DryRunResponse>;

  /**
   * @hidden
   */
  export function useDryRun(query: Query | Query[], options?: CubeFetchOptions): UseDryRunResult;

  /**
   * @hidden
   */
  export type LoadLazyDryRunOptions = {
    query?: Query | Query[];
  };

  /**
   * @hidden
   */
  export function useLazyDryRun(
    query?: Query | Query[],
    options?: CubeFetchOptions
  ): [(loadOptions?: LoadLazyDryRunOptions) => Promise<void>, UseDryRunResult];

  /**
   * @hidden
   */
  type UseCubeSqlResponse = {
    sql: string;
  };

  export function useCubeMeta(options?: Omit<CubeFetchOptions, 'query'>): CubeFetchResult<Meta>;

  /**
   * @hidden
   */
  export function useCubeSql(query: Query | Query[], options?: CubeFetchOptions): UseDryRunResult;

  /**
   * Checks whether the query is ready
   */
  export function isQueryPresent(query: Query | Query[]): boolean;

  /**
   * You can use the following methods for member manipulaltion
   * ```js
   * <QueryBuilder
   *   // ...
   *   cubejsApi={cubejsApi}
   *   render={({
   *     // ...
   *     availableMeasures,
   *     updateMeasures,
   *   }) => {
   *     return (
   *       // ...
   *       <Select
   *         mode="multiple"
   *         placeholder="Please select"
   *         onSelect={(measure) => updateMeasures.add(measure)}
   *         onDeselect={(measure) => updateMeasures.remove(measure)}
   *       >
   *         {availableMeasures.map((measure) => (
   *           <Select.Option key={measure.name} value={measure}>
   *             {measure.title}
   *           </Select.Option>
   *         ))}
   *       </Select>
   *     );
   *   }}
   * />
   * ```
   *
   * NOTE: if you need to add or remove more than one member at a time you should use `updateQuery` prop of {@see QueryBuilderRenderProps}
   * ```js
   * <QueryBuilder
   *   // ...
   *   cubejsApi={cubejsApi}
   *   render={({
   *     // ...
   *     measures,
   *     updateMeasures,
   *     updateQuery,
   *   }) => {
   *     // ...
   *     return (
   *       <>
   *         // WRONG: This code will not work properly
   *         <button
   *           onClick={() =>
   *             measures.forEach((measure) => updateMeasures.remove(measure))
   *           }
   *         >
   *           Remove all
   *         </button>
   *
   *         // CORRECT: Using `updateQuery` for removing all measures
   *         <button
   *           onClick={() =>
   *             updateQuery({
   *               measures: [],
   *             })
   *           }
   *         >
   *           Remove all
   *         </button>
   *       </>
   *     );
   *   }}
   * />
   * ```
   */
  type MemberUpdater<T> = {
    add: (member: T) => void;
    remove: (member: { index: number }) => void;
    update: (member: { index: number }, updateWith: T) => void;
  };

  type FilterExtraFields = {
    dimension: TCubeDimension | TCubeMeasure;
    operators: { name: string; title: string }[];
  };
  type FilterWithExtraFields = Omit<Filter, 'dimension'> & FilterExtraFields;

  type GranularityOptions = {
    granularities: { name: string; title: string }[];
  };
  type TimeDimensionExtraFields = {
    dimension: TCubeDimension & GranularityOptions;
  };
  type TimeDimensionWithExtraFields = Omit<TimeDimension, 'dimension'> & TimeDimensionExtraFields;

  type DimensionUpdater = MemberUpdater<TCubeDimension>;
  type MeasureUpdater = MemberUpdater<TCubeMeasure>;
  type SegmentUpdater = MemberUpdater<TCubeSegment>;

  // Only require the fields that are actually used (otherwise fields like `operators` are required just to add/update)
  type TimeDimensionRangedUpdateFields = {
    granularity?: TimeDimensionGranularity;
    dateRange?: DateRange;
    dimension: TCubeDimension;
  };
  type TimeDimensionComparisonUpdateFields = {
    granularity?: TimeDimensionGranularity;
    compareDateRange: Array<DateRange>;
    dimension: TCubeDimension;
  };
  type TimeDimensionUpdater = MemberUpdater<TimeDimensionRangedUpdateFields | TimeDimensionComparisonUpdateFields>;

  type FilterUpdateFields = {
    member?: string;
    operator: BinaryOperator | UnaryOperator;
    values?: string[];
    dimension: TCubeDimension | TCubeMeasure;
  };
  type FilterUpdater = MemberUpdater<FilterUpdateFields>;

  type OrderUpdater = {
    set: (memberId: string, order: QueryOrder | 'none') => void;
    update: (order: Query['order']) => void;
    reorder: (sourceIndex: number, destinationIndex: number) => void;
  };

  type PivotConfigUpdaterArgs = {
    sourceIndex: number;
    destinationIndex: number;
    sourceAxis: TSourceAxis;
    destinationAxis: TSourceAxis;
  };
  type PivotConfigExtraUpdateFields = {
    limit?: number;
  };
  type PivotConfigUpdater = {
    moveItem: (args: PivotConfigUpdaterArgs) => void;
    update: (pivotConfig: PivotConfig & PivotConfigExtraUpdateFields) => void;
  };
}
