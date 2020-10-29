/**
 * @title @cubejs-client/core
 * @permalink /@cubejs-client-core
 * @menuCategory Cube.js Frontend
 * @subcategory Reference
 * @menuOrder 2
 * @description Vanilla JavaScript Cube.js client.
 */

declare module '@cubejs-client/core' {
  export type TransportOptions = {
    /**
     * [jwt auth token](security)
     */
    authorization: string;
    /**
     * path to `/cubejs-api/v1`
     */
    apiUrl: string;
    /**
     * custom headers
     */
    headers?: Record<string, string>;
    credentials?: 'omit' | 'same-origin' | 'include';
  };

  export interface ITransport {
    request(method: string, params: any): () => Promise<void>;
  }

  /**
   * Default transport implementation.
   * @order 3
   */
  export class HttpTransport implements ITransport {
    constructor(options: TransportOptions);
    request(method: string, params: any): () => Promise<any>;
  }

  export type CubeJSApiOptions = {
    /**
     * URL of your Cube.js Backend. By default, in the development environment it is `http://localhost:4000/cubejs-api/v1`
     */
    apiUrl: string;
    /**
     * Transport implementation to use. [HttpTransport](#http-transport) will be used by default.
     */
    transport?: ITransport;
    headers?: Record<string, string>;
    pollInterval?: number;
    credentials?: 'omit' | 'same-origin' | 'include';
    parseDateMeasures?: boolean;
  };

  export type LoadMethodOptions = {
    /**
     * Key to store the current request's MUTEX inside the `mutexObj`. MUTEX object is used to reject orphaned queries results when new queries are sent. For example: if two queries are sent with the same `mutexKey` only the last one will return results.
     */
    mutexKey?: string;
    /**
     * Object to store MUTEX
     */
    mutexObj?: Object;
    /**
     * Pass `true` to use continuous fetch behavior.
     */
    subscribe?: boolean;
    /**
     * Function that receives `ProgressResult` on each `Continue wait` message.
     */
    progressCallback?(result: ProgressResult): void;
  };

  export type LoadMethodCallback<T> = (error: Error | null, resultSet: T) => void;

  export type QueryOrder = 'asc' | 'desc';

  export type TQueryOrderObject = { [key: string]: QueryOrder };
  export type TQueryOrderArray = Array<[string, QueryOrder]>;

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

  type PivotQuery = Query & {
    queryType: QueryType;
  };

  type QueryType = 'regularQuery' | 'compareDateRangeQuery' | 'blendingQuery';

  type LoadResponseResult<T> = {
    annotation: QueryAnnotations;
    lastRefreshTime: string;
    query: Query;
    data: T[];
  };

  export type LoadResponse<T> = {
    queryType: QueryType;
    results: LoadResponseResult<T>[];
    pivotQuery: PivotQuery;
  };

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
    /**
     * Dimensions to put on **x** or **rows** axis.
     */
    x?: string[];
    /**
     * Dimensions to put on **y** or **columns** axis.
     */
    y?: string[];
    /**
     * If `true` missing dates on the time dimensions will be filled with `0` for all measures.Note: the `fillMissingDates` option set to `true` will override any **order** applied to the query
     */
    fillMissingDates?: boolean | null;
  };

  export type DrillDownLocator = {
    xValues: string[];
    yValues?: string[];
  };

  export type Series<T> = {
    key: string;
    title: string;
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
    meta: any;
    type: string | number;
    title: string;
    shortTitle: string;
    format?: any;
    children?: TableColumn[];
  };

  export type PivotRow = {
    xValues: Array<string | number>;
    yValuesArray: Array<[string[], number]>;
  };

  /**
   * Provides a convenient interface for data manipulation.
   */
  export class ResultSet<T = any> {
    /**
     * @hidden
     */
    static measureFromAxis(axisValues: string[]): string;
    static getNormalizedPivotConfig(query: PivotQuery, pivotConfig?: Partial<PivotConfig>): PivotConfig;
    /**
     * ```js
     * import { ResultSet } from '@cubejs-client/core';
     *
     * const resultSet = await cubejsApi.load(query);
     * // You can store the result somewhere
     * const tmp = resultSet.serialize();
     *
     * // and restore it later
     * const resultSet = ResultSet.deserialize(tmp);
     * ```
     * @param data the result of [serialize](#result-set-serialize)
     */
    static deserialize<TData = any>(data: Object, options?: Object): ResultSet<TData>;

    /**
     * Can be used to stash the `ResultSet` in a storage and restored later with [deserialize](#result-set-deserialize)
     */
    serialize(): Object;

    /**
     * Can be used when you need access to the methods that can't be used with some query types (eg `compareDateRangeQuery` or `blendingQuery`)
     * ```js
     * resultSet.decompose().forEach((currentResultSet) => {
     *   console.log(currentResultSet.rawData());
     * });
     * ```
     */
    decompose(): Object;

    /**
     * @hidden
     */
    normalizePivotConfig(pivotConfig?: PivotConfig): PivotConfig;

    /**
     * Returns a measure drill down query.
     *
     * Provided you have a measure with the defined `drillMemebers` on the `Orders` cube
     * ```js
     * measures: {
     *   count: {
     *     type: `count`,
     *     drillMembers: [Orders.status, Users.city, count],
     *   },
     *   // ...
     * }
     * ```
     *
     * Then you can use the `drillDown` method to see the rows that contribute to that metric
     * ```js
     * resultSet.drillDown(
     *   {
     *     xValues,
     *     yValues,
     *   },
     *   // you should pass the `pivotConfig` if you have used it for axes manipulation
     *   pivotConfig
     * )
     * ```
     *
     * the result will be a query with the required filters applied and the dimensions/measures filled out
     * ```js
     * {
     *   measures: ['Orders.count'],
     *   dimensions: ['Orders.status', 'Users.city'],
     *   filters: [
     *     // dimension and measure filters
     *   ],
     *   timeDimensions: [
     *     //...
     *   ]
     * }
     * ```
     *
     * In case when you want to add `order` or `limit` to the query, you can simply spread it
     *
     * ```js
     * // An example for React
     * const drillDownResponse = useCubeQuery(
     *    {
     *      ...drillDownQuery,
     *      limit: 30,
     *      order: {
     *        'Orders.ts': 'desc'
     *      }
     *    },
     *    {
     *      skip: !drillDownQuery
     *    }
     *  );
     * ```
     * @returns Drill down query
     */
    drillDown(drillDownLocator: DrillDownLocator, pivotConfig?: PivotConfig): Query | null;

    /**
     * Returns an array of series with key, title and series data.
     * ```js
     * // For the query
     * {
     *   measures: ['Stories.count'],
     *   timeDimensions: [{
     *     dimension: 'Stories.time',
     *     dateRange: ['2015-01-01', '2015-12-31'],
     *     granularity: 'month'
     *   }]
     * }
     *
     * // ResultSet.series() will return
     * [
     *   {
     *     key: 'Stories.count',
     *     title: 'Stories Count',
     *     series: [
     *       { x: '2015-01-01T00:00:00', value: 27120 },
     *       { x: '2015-02-01T00:00:00', value: 25861 },
     *       { x: '2015-03-01T00:00:00', value: 29661 },
     *       //...
     *     ],
     *   },
     * ]
     * ```
     */
    series<SeriesItem = any>(pivotConfig?: PivotConfig): Series<SeriesItem>[];

    /**
     * Returns an array of series objects, containing `key` and `title` parameters.
     * ```js
     * // For query
     * {
     *   measures: ['Stories.count'],
     *   timeDimensions: [{
     *     dimension: 'Stories.time',
     *     dateRange: ['2015-01-01', '2015-12-31'],
     *     granularity: 'month'
     *   }]
     * }
     *
     * // ResultSet.seriesNames() will return
     * [
     *   {
     *     key: 'Stories.count',
     *     title: 'Stories Count',
     *     yValues: ['Stories.count'],
     *   },
     * ]
     * ```
     * @returns An array of series names
     */
    seriesNames(pivotConfig?: PivotConfig): SeriesNamesColumn[];

    /**
     * Base method for pivoting [ResultSet](#result-set) data.
     * Most of the times shouldn't be used directly and [chartPivot](#result-set-chart-pivot)
     * or (tablePivot)[#table-pivot] should be used instead.
     *
     * You can find the examples of using the `pivotConfig` [here](#pivot-config)
     * ```js
     * // For query
     * {
     *   measures: ['Stories.count'],
     *   timeDimensions: [{
     *     dimension: 'Stories.time',
     *     dateRange: ['2015-01-01', '2015-03-31'],
     *     granularity: 'month'
     *   }]
     * }
     *
     * // ResultSet.pivot({ x: ['Stories.time'], y: ['measures'] }) will return
     * [
     *   {
     *     xValues: ["2015-01-01T00:00:00"],
     *     yValuesArray: [
     *       [['Stories.count'], 27120]
     *     ]
     *   },
     *   {
     *     xValues: ["2015-02-01T00:00:00"],
     *     yValuesArray: [
     *       [['Stories.count'], 25861]
     *     ]
     *   },
     *   {
     *     xValues: ["2015-03-01T00:00:00"],
     *     yValuesArray: [
     *       [['Stories.count'], 29661]
     *     ]
     *   }
     * ]
     * ```
     * @returns An array of pivoted rows.
     */
    pivot(pivotConfig?: PivotConfig): PivotRow[];

    /**
     * Returns normalized query result data in the following format.
     *
     * You can find the examples of using the `pivotConfig` [here](#pivot-config)
     * ```js
     * // For the query
     * {
     *   measures: ['Stories.count'],
     *   timeDimensions: [{
     *     dimension: 'Stories.time',
     *     dateRange: ['2015-01-01', '2015-12-31'],
     *     granularity: 'month'
     *   }]
     * }
     *
     * // ResultSet.chartPivot() will return
     * [
     *   { "x":"2015-01-01T00:00:00", "Stories.count": 27120, "xValues": ["2015-01-01T00:00:00"] },
     *   { "x":"2015-02-01T00:00:00", "Stories.count": 25861, "xValues": ["2015-02-01T00:00:00"]  },
     *   { "x":"2015-03-01T00:00:00", "Stories.count": 29661, "xValues": ["2015-03-01T00:00:00"]  },
     *   //...
     * ]
     * ```
     */
    chartPivot(pivotConfig?: PivotConfig): ChartPivotRow[];

    /**
     * Returns normalized query result data prepared for visualization in the table format.
     *
     * You can find the examples of using the `pivotConfig` [here](#pivot-config)
     *
     * For example:
     * ```js
     * // For the query
     * {
     *   measures: ['Stories.count'],
     *   timeDimensions: [{
     *     dimension: 'Stories.time',
     *     dateRange: ['2015-01-01', '2015-12-31'],
     *     granularity: 'month'
     *   }]
     * }
     *
     * // ResultSet.tablePivot() will return
     * [
     *   { "Stories.time": "2015-01-01T00:00:00", "Stories.count": 27120 },
     *   { "Stories.time": "2015-02-01T00:00:00", "Stories.count": 25861 },
     *   { "Stories.time": "2015-03-01T00:00:00", "Stories.count": 29661 },
     *   //...
     * ]
     * ```
     * @returns An array of pivoted rows
     */
    tablePivot(pivotConfig?: PivotConfig): Array<{ [key: string]: string | number | boolean }>;

    /**
     * Returns an array of column definitions for `tablePivot`.
     *
     * For example:
     * ```js
     * // For the query
     * {
     *   measures: ['Stories.count'],
     *   timeDimensions: [{
     *     dimension: 'Stories.time',
     *     dateRange: ['2015-01-01', '2015-12-31'],
     *     granularity: 'month'
     *   }]
     * }
     *
     * // ResultSet.tableColumns() will return
     * [
     *   {
     *     key: 'Stories.time',
     *     dataIndex: 'Stories.time',
     *     title: 'Stories Time',
     *     shortTitle: 'Time',
     *     type: 'time',
     *     format: undefined,
     *   },
     *   {
     *     key: 'Stories.count',
     *     dataIndex: 'Stories.count',
     *     title: 'Stories Count',
     *     shortTitle: 'Count',
     *     type: 'count',
     *     format: undefined,
     *   },
     *   //...
     * ]
     * ```
     *
     * In case we want to pivot the table axes
     * ```js
     * // Let's take this query as an example
     * {
     *   measures: ['Orders.count'],
     *   dimensions: ['Users.country', 'Users.gender']
     * }
     *
     * // and put the dimensions on `y` axis
     * resultSet.tableColumns({
     *   x: [],
     *   y: ['Users.country', 'Users.gender', 'measures']
     * })
     * ```
     *
     * then `tableColumns` will group the table head and return
     * ```js
     * {
     *   key: 'Germany',
     *   type: 'string',
     *   title: 'Users Country Germany',
     *   shortTitle: 'Germany',
     *   meta: undefined,
     *   format: undefined,
     *   children: [
     *     {
     *       key: 'male',
     *       type: 'string',
     *       title: 'Users Gender male',
     *       shortTitle: 'male',
     *       meta: undefined,
     *       format: undefined,
     *       children: [
     *         {
     *           // ...
     *           dataIndex: 'Germany.male.Orders.count',
     *           shortTitle: 'Count',
     *         },
     *       ],
     *     },
     *     {
     *       // ...
     *       shortTitle: 'female',
     *       children: [
     *         {
     *           // ...
     *           dataIndex: 'Germany.female.Orders.count',
     *           shortTitle: 'Count',
     *         },
     *       ],
     *     },
     *   ],
     * },
     * // ...
     * ```
     * @returns An array of columns
     */
    tableColumns(pivotConfig?: PivotConfig): TableColumn[];

    query(): Query;
    rawData(): T[];
    annotation(): QueryAnnotations;
  }

  export type Filter = BinaryFilter | UnaryFilter;
  type BinaryFilter = {
    dimension?: string;
    member?: string;
    operator: BinaryOperator;
    values: string[];
    and?: BinaryFilter[];
    or?: BinaryFilter[];
  };
  type UnaryFilter = {
    dimension?: string;
    member?: string;
    operator: UnaryOperator;
    values?: never;
    and?: UnaryFilter[];
    or?: UnaryFilter[];
  };
  type UnaryOperator = 'set' | 'notSet';
  type BinaryOperator =
    | 'equals'
    | 'notEquals'
    | 'contains'
    | 'notContains'
    | 'gt'
    | 'gte'
    | 'lt'
    | 'lte'
    | 'inDateRange'
    | 'notInDateRange'
    | 'beforeDate'
    | 'afterDate';

  export type TimeDimensionGranularity = 'second' | 'minute' | 'hour' | 'day' | 'week' | 'month' | 'year';

  export type TimeDimension = {
    dimension: string;
    dateRange?: string | string[];
    granularity?: TimeDimensionGranularity;
  };

  export type Query = {
    measures?: string[];
    dimensions?: string[];
    filters?: Filter[];
    timeDimensions?: TimeDimension[];
    segments?: string[];
    limit?: number;
    offset?: number;
    order?: TQueryOrderObject | TQueryOrderArray;
    timezone?: string;
    renewQuery?: boolean;
    ungrouped?: boolean;
  };

  export type ProgressResponse = {
    stage: string;
    timeElapsed: number;
  };

  export class ProgressResult {
    stage(): string;
    timeElapsed(): string;
  }

  export type SqlQueryTuple = [string, boolean | string | number];

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
  };

  export class SqlQuery {
    rawQuery(): SqlData;
    sql(): string;
  }

  export type MemberType = 'measures' | 'dimensions' | 'segments';

  type TCubeMemberType = 'time' | 'number' | 'string' | 'boolean';

  type TCubeMember = {
    type: TCubeMemberType;
    name: string;
    title: string;
    shortTitle: string;
  };

  type TCubeMeasure = TCubeMember & {
    aggType: 'count' | 'number';
    cumulative: boolean;
    cumulativeTotal: boolean;
    drillMembers: string[];
    drillMembersGrouped: {
      measures: string[];
      dimensions: string[];
    };
  };

  type TCubeDimension = TCubeMember & {
    suggestFilterValues: boolean;
  };

  type TCubeSegment = Pick<TCubeMember, 'name' | 'shortTitle' | 'title'>;

  type TCubeMemberByType<T> = T extends 'measures'
    ? TCubeMeasure
    : T extends 'dimensions'
    ? TCubeDimension
    : T extends 'segments'
    ? TCubeSegment
    : never;

  type TDryRunResponse = {
    queryType: QueryType;
    normalizedQueries: Query[];
    pivotQuery: PivotQuery;
    queryOrder: Array<{ [k: string]: QueryOrder }>;
  };

  /**
   * Contains information about available cubes and it's members.
   * @order 4
   */
  export class Meta {
    /**
     * Get all members of a specific type for a given query.
     * If empty query is provided no filtering is done based on query context and all available members are retrieved.
     * @param query - context query to provide filtering of members available to add to this query
     */
    membersForQuery(query: Query | null, memberType: MemberType): TCubeMeasure[] | TCubeDimension[] | TCubeMember[];

    /**
     * Get meta information for a cube member
     * Member meta information contains:
     * ```javascript
     * {
     *   name,
     *   title,
     *   shortTitle,
     *   type,
     *   description,
     *   format
     * }
     * ```
     * @param memberName - Fully qualified member name in a form `Cube.memberName`
     * @return An object containing meta information about member
     */
    resolveMember<T extends MemberType>(
      memberName: string,
      memberType: T | T[]
    ): { title: string; error: string } | TCubeMemberByType<T>;
    defaultTimeDimensionNameFor(memberName: string): string;
    filterOperatorsForMember(memberName: string, memberType: MemberType | MemberType[]): any;
  }

  /**
   * Main class for accessing Cube.js API
   *
   * @order 2
   */
  export class CubejsApi {
    load(query: Query | Query[], options?: LoadMethodOptions): Promise<ResultSet>;
    /**
     * Fetch data for the passed `query`.
     *
     * ```js
     * import cubejs from '@cubejs-client/core';
     * import Chart from 'chart.js';
     * import chartjsConfig from './toChartjsData';
     *
     * const cubejsApi = cubejs('CUBEJS_TOKEN');
     *
     * const resultSet = await cubejsApi.load({
     *  measures: ['Stories.count'],
     *  timeDimensions: [{
     *    dimension: 'Stories.time',
     *    dateRange: ['2015-01-01', '2015-12-31'],
     *    granularity: 'month'
     *   }]
     * });
     *
     * const context = document.getElementById('myChart');
     * new Chart(context, chartjsConfig(resultSet));
     * ```
     * @param query - [Query object](query-format)
     */
    load(query: Query | Query[], options?: LoadMethodOptions, callback?: LoadMethodCallback<ResultSet>): void;

    /**
     * Allows you to fetch data and receive updates over time. See [Real-Time Data Fetch](real-time-data-fetch)
     *
     * ```js
     * cubejsApi.subscribe(
     *   {
     *     measures: ['Logs.count'],
     *     timeDimensions: [
     *       {
     *         dimension: 'Logs.time',
     *         granularity: 'hour',
     *         dateRange: 'last 1440 minutes',
     *       },
     *     ],
     *   },
     *   options,
     *   (error, resultSet) => {
     *     if (!error) {
     *       // handle the update
     *     }
     *   }
     * );
     * ```
     */
    subscribe(query: Query | Query[], options: LoadMethodOptions | null, callback: LoadMethodCallback<ResultSet>): void;

    sql(query: Query | Query[], options?: LoadMethodOptions): Promise<SqlQuery>;
    /**
     * Get generated SQL string for the given `query`.
     * @param query - [Query object](query-format)
     */
    sql(query: Query | Query[], options?: LoadMethodOptions, callback?: LoadMethodCallback<SqlQuery>): void;

    meta(options?: LoadMethodOptions): Promise<Meta>;
    /**
     * Get meta description of cubes available for querying.
     */
    meta(options?: LoadMethodOptions, callback?: LoadMethodCallback<Meta>): void;

    dryRun(query: Query | Query[], options?: LoadMethodOptions): Promise<TDryRunResponse>;
    /**
     * Get query related meta without query execution
     */
    dryRun(query: Query | Query[], options: LoadMethodOptions, callback?: LoadMethodCallback<TDryRunResponse>): void;
  }

  /**
   * Creates an instance of the `CubejsApi`. The API entry point.
   *
   * ```js
   * import cubejs from '@cubejs-client/core';
   * const cubejsApi = cubejs(
   *   'CUBEJS-API-TOKEN',
   *   { apiUrl: 'http://localhost:4000/cubejs-api/v1' }
   * );
   * ```
   *
   * You can also pass an async function or a promise that will resolve to the API token
   *
   * ```js
   * import cubejs from '@cubejs-client/core';
   * const cubejsApi = cubejs(
   *   async () => await Auth.getJwtToken(),
   *   { apiUrl: 'http://localhost:4000/cubejs-api/v1' }
   * );
   * ```
   *
   * @param apiToken - [API token](security) is used to authorize requests and determine SQL database you're accessing. In the development mode, Cube.js Backend will print the API token to the console on on startup. In case of async function `authorization` is updated for `options.transport` on each request.
   * @order 1
   */
  export default function cubejs(apiToken: string | (() => Promise<string>), options: CubeJSApiOptions): CubejsApi;
  export default function cubejs(options: CubeJSApiOptions): CubejsApi;

  /**
   * @hidden
   */
  export type TSourceAxis = 'x' | 'y';

  export type TDefaultHeuristicsOptions = {
    meta: Meta;
    sessionGranularity?: TimeDimensionGranularity;
  };

  export function defaultHeuristics(newQuery: Query, oldQuery: Query, options: TDefaultHeuristicsOptions): any;
  /**
   * @hidden
   */
  export function isQueryPresent(query: Query | Query[]): boolean;
  export function movePivotItem(
    pivotConfig: PivotConfig,
    sourceIndex: number,
    destinationIndex: number,
    sourceAxis: TSourceAxis,
    destinationAxis: TSourceAxis
  ): PivotConfig;
  /**
   * @hidden
   */
  export function moveItemInArray<T = any>(list: T[], sourceIndex: number, destinationIndex: number): T[];
  
  export function defaultOrder(query: Query): { [key: string]: QueryOrder }
}
