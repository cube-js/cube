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
  QueryOrderType,
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

type GroupingSet = {
    groupType: string,
    id: number,
    subId?: null | number
};

type MemberExpression = {
  expression: Function;
  cubeName: string;
  name: string;
  expressionName: string;
  definition: string;
  groupingSet?: GroupingSet
};

/**
 * Query datetime dimention interface.
 */
interface QueryTimeDimension {
  dimension: Member;
  dateRange?: string[] | string;
  granularity?: QueryTimeDimensionGranularity;
}

/**
 * Incoming network query data type.
 */
interface Query {
  measures: (Member | MemberExpression)[];
  dimensions?: (Member | TimeMember | MemberExpression)[];
  filters?: (QueryFilter | LogicalAndFilter | LogicalOrFilter)[];
  timeDimensions?: QueryTimeDimension[];
  segments?: (Member | MemberExpression)[];
  limit?: null | number;
  offset?: number;
  total?: boolean;
  totalQuery?: boolean;
  order?: any;
  timezone?: string;
  renewQuery?: boolean;
  ungrouped?: boolean;
  responseFormat?: ResultType;
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
  order?: [{ id: string; desc: boolean }];
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
};
