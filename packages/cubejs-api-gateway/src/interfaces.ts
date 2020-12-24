import type {
  Request as ExpressRequest,
  Response as ExpressResponse,
  NextFunction as ExpressNextFunction
} from 'express';

export interface QueryFilter {
  member: string;
  operator:
    | 'equals'
    | 'notEquals'
    | 'contains'
    | 'notContains'
    | 'gt'
    | 'gte'
    | 'lt'
    | 'lte'
    | 'set'
    | 'notSet'
    | 'inDateRange'
    | 'notInDateRange'
    | 'beforeDate'
    | 'afterDate';
  values?: string[];
}

export type QueryTimeDimensionGranularity =
  | 'hour'
  | 'day'
  | 'week'
  | 'month'
  | 'year';

export interface QueryTimeDimension {
  dimension: string;
  dateRange?: string[] | string;
  granularity?: QueryTimeDimensionGranularity;
}

export interface Query {
  measures: string[];
  dimensions?: string[];
  filters?: QueryFilter[];
  timeDimensions?: QueryTimeDimension[];
  segments?: string[];
  limit?: number;
  offset?: number;
  order?: 'asc' | 'desc';
  timezone?: string;
  renewQuery?: boolean;
  ungrouped?: boolean;
}

export interface NormalizedQueryFilter extends QueryFilter {
  dimension?: string;
}

export interface NormalizedQuery extends Query {
  filters?: NormalizedQueryFilter[];
  rowLimit?: number;
}

export interface RequestContext {
  authInfo: any;
  requestId: string;
}

export type QueryTransformerFn = (query: Query, context: RequestContext) => Promise<Query>;

// @deprecated
export type CheckAuthMiddlewareFn = (req: ExpressRequest, res: ExpressResponse, next: ExpressNextFunction) => void

// @todo ctx can be passed from SubscriptionServer that will cause incapability with Express.Request
export type CheckAuthFn = (ctx: any, authorization?: string) => any;

export type ExtendContextFn = (req: ExpressRequest) => any;
