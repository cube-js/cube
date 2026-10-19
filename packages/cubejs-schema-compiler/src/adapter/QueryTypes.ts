/**
 * Shapes of the plain objects BaseQuery and its dialects pass around: query
 * members and filters as they come in, the multi-stage CTE plan and refresh
 * key tuples.
 */
import type { MemberOrderBy } from '../compiler/CubeEvaluator';
import type { SqlDefinition, SqlFn } from '../compiler/CubeSymbols';
import type { BaseQuery } from './BaseQuery';

export type GroupingSet = {
  groupType: 'Rollup' | 'Cube';
  id: number;
  subId?: number | null;
};

export type PatchMeasureExpression = {
  type: 'PatchMeasure';
  sourceMeasure: string;
  replaceAggregationType: string | null;
  addFilters: SqlDefinition[];
};

/**
 * An ad-hoc member from the SQL API, already evaluated by the API gateway.
 */
export type MemberExpression = {
  expression: SqlFn | PatchMeasureExpression;
  cubeName: string;
  name: string;
  // Without a cube name for SQL push down
  expressionName?: string;
  definition?: string;
  groupingSet?: GroupingSet | null;
  // Set when a dimension-only measure is attached to the cube it references
  originalCubeName?: string;
};

export type QueryMember = string | MemberExpression;

export type QueryTimeDimension = {
  dimension: string;
  // BaseTimeDimension.granularity is null without a granularity
  granularity?: string | null;
  dateRange?: [string, string];
  boundaryDateRange?: [string, string];
  shiftInterval?: string;
};

/**
 * A filter of an incoming query, before extractFiltersAsTree(): a member
 * condition, or an `and` / `or` group of filters. `dimension` and `measure`
 * are legacy spellings of `member`.
 */
export type QueryFilterInput = {
  member?: string;
  dimension?: string;
  measure?: string;
  operator?: string;
  values?: unknown[];
  and?: QueryFilterInput[];
  or?: QueryFilterInput[];
};

/**
 * A filter after extractFiltersAsTree(). A group keeps its operator (`and` /
 * `or`) and its items in `values`; initFilter() replaces those items in place
 * with BaseFilter / BaseGroupFilter instances, so `values` holds either.
 */
export type QueryFilterTreeItem = {
  member?: string;
  dimension?: string | null;
  measure?: string | null;
  operator?: string;
  values?: unknown[];
  dimensionGroup?: boolean;
  measureGroup?: boolean;
};

/**
 * Anything rendered into a WHERE / HAVING clause: BaseFilter, BaseGroupFilter,
 * BaseSegment, BaseTimeDimension, or an ad-hoc condition object.
 */
export type FilterToWhere = {
  filterToWhere(): string | null;
};

/**
 * What BaseGroupFilter is built from: initialized filters, segments or ad-hoc
 * conditions joined by `operator`.
 */
export type GroupFilterInput = {
  operator: string;
  values: unknown[];
  measure?: string | null;
  dimension?: string | null;
};

export type MultiStageQueryContext = {
  dimensions: QueryMember[];
  multiStageDimensions: QueryMember[];
  timeDimensions: QueryTimeDimension[];
  multiStageTimeDimensions: QueryTimeDimension[];
  filters: QueryFilterInput[];
  segments: QueryMember[];
  commonTimeShift?: string;
  timeShifts?: Record<string, string>;
};

/**
 * A multi-stage CTE: the context of its member plus the CTEs it reads from.
 * `alias` is assigned once the CTE is registered.
 */
export type MultiStageWithQuery = MultiStageQueryContext & {
  measures: string[];
  memberFrom?: MultiStageWithQuery[];
  alias?: string;
};

export type RenderedOrderBy = {
  sql: string;
  dir: MemberOrderBy['dir'];
};

export type MeasureCollectionContext = {
  hasMultipliedForPreAggregation?: boolean;
};

/**
 * A leaf of collectRootMeasureToHierarchy(): the measure to render and whether
 * its cube multiplies rows. A dimension-only member expression measure comes
 * as the expression itself.
 */
export type MeasureHierarchyItem = {
  multiplied: boolean;
  measure: string | MemberExpression;
  multiStage?: boolean;
};

export type EveryRefreshKeyParts = {
  utcOffset: number;
  interval: number;
  dayOffset: number;
  cron: boolean;
};

export type RefreshKeyQueryOptions = {
  external: boolean;
  renewalThreshold: number;
  localRefreshKey?: EveryRefreshKeyParts;
};

/**
 * A cube refresh key before its SQL is parametrized: the SQL, its options and
 * the query that rendered it.
 */
export type RefreshKeyTuple = [sql: string, options: RefreshKeyQueryOptions, query: BaseQuery];

/**
 * The value of `FILTER_PARAMS.<cube>.<member>.filter(column)`.
 */
export type FilterParamBinding = {
  __column(): unknown;
  __member(): string;
  __timeShift(): boolean;
  toString(): string;
};

/**
 * `FILTER_PARAMS` as a data model `sql` function receives it:
 * `FILTER_PARAMS.<cube>.<member>.filter(column)`.
 */
export type FilterParamsProxy = Record<string, Record<string, {
  filter(column: unknown): FilterParamBinding;
}>>;
