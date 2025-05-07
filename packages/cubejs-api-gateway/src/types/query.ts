/**
 * @license Apache-2.0
 * @copyright Cube Dev, Inc.
 * @fileoverview
 * Network query data types definition.
 */

import {
  Member,
  TimeMember,
  FilterOperator,
  QueryTimeDimensionGranularity,
} from './strings';
import { ResultType } from './enums';

/**
 * Query base filter definition.
 */
interface QueryFilter {
  member: Member;
  operator: FilterOperator;
  values?: string[];
}

/**
 * Query 'and'-filters type definition.
 */
type LogicalAndFilter = {
  and: (QueryFilter | {
    or: (QueryFilter | LogicalAndFilter)[]
  })[]
};

/**
 * Query 'or'-filters type definition.
 */
type LogicalOrFilter = {
  or: (QueryFilter | LogicalAndFilter)[]
};

export type GroupingSetType = 'Rollup' | 'Cube';

type GroupingSet = {
    groupType: GroupingSetType,
    id: number,
    subId?: null | number
};

export type EvalPatchMeasureFilterExpression = {
  sql: Function,
};

export type PatchMeasureExpression = {
  type: 'PatchMeasure',
  sourceMeasure: string,
  replaceAggregationType: string | null,
  addFilters: Array<Array<string>>,
};

export type EvalPatchMeasureExpression = {
  type: 'PatchMeasure',
  sourceMeasure: string,
  replaceAggregationType: string | null,
  addFilters: Array<EvalPatchMeasureFilterExpression>,
};

type ParsedMemberExpression = {
  expression: string[] | PatchMeasureExpression;
  cubeName: string;
  name: string;
  expressionName: string;
  definition: string;
  groupingSet?: GroupingSet
};

type MemberExpression = Omit<ParsedMemberExpression, 'expression'> & {
  expression: Function | EvalPatchMeasureExpression;
};

type InputSqlFunction = {
  cubeParams: Array<string>,
  sql: string,
};

export type InputMemberExpressionSqlFunction = {
  type: 'SqlFunction'
} & InputSqlFunction;

export type InputMemberExpressionPatchMeasure = {
  type: 'PatchMeasure',
  sourceMeasure: string,
  replaceAggregationType: string | null,
  addFilters: Array<InputSqlFunction>,
};

export type InputMemberExpressionExpr = InputMemberExpressionSqlFunction | InputMemberExpressionPatchMeasure;

// This should be aligned with cubesql side
export type InputMemberExpression = {
  cubeName: string,
  alias: string,
  expr: InputMemberExpressionExpr,
  groupingSet: GroupingSet | null,
};

/**
 * Query datetime dimension interface.
 */
interface QueryTimeDimension {
  dimension: Member;
  dateRange?: string[] | string;
  compareDateRange?: string[];
  granularity?: QueryTimeDimensionGranularity;
}

type SubqueryJoins = {
  sql: string,
  // TODO This is _always_ a member expression, maybe pass as parsed, without intermediate string?
  // TODO there are three different types instead of alternatives for this actually
  on: string | ParsedMemberExpression | MemberExpression,
  joinType: 'LEFT' | 'INNER',
  alias: string,
};

type JoinHint = Array<string>;

/**
 * Incoming network query data type.
 */
interface Query {
  measures: (Member | MemberExpression | ParsedMemberExpression)[];
  dimensions?: (Member | TimeMember | MemberExpression | ParsedMemberExpression)[];
  filters?: (QueryFilter | LogicalAndFilter | LogicalOrFilter)[];
  timeDimensions?: QueryTimeDimension[];
  segments?: (Member | MemberExpression | ParsedMemberExpression)[];
  limit?: null | number;
  offset?: number;
  total?: boolean;
  totalQuery?: boolean;
  order?: any;
  timezone?: string;
  renewQuery?: boolean;
  ungrouped?: boolean;
  responseFormat?: ResultType;

  // TODO incoming query, query with parsed exprs and query with evaluated exprs are all different types
  subqueryJoins?: Array<SubqueryJoins>,

  joinHints?: Array<JoinHint>
}

/**
 * Normalized filter interface.
 */
interface NormalizedQueryFilter extends QueryFilter {
  dimension?: Member;
}

/**
 * Normalized query interface.
 */
interface NormalizedQuery extends Query {
  filters?: NormalizedQueryFilter[];
  rowLimit?: null | number;
  order?: { id: string; desc: boolean }[];
}

export {
  QueryFilter,
  LogicalAndFilter,
  LogicalOrFilter,
  QueryTimeDimension,
  Query,
  NormalizedQueryFilter,
  NormalizedQuery,
  MemberExpression,
  ParsedMemberExpression,
};
