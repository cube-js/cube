/**
 * @license Apache-2.0
 * @copyright Cube Dev, Inc.
 * @fileoverview
 * Network request data types definition.
 */

import type { Request as ExpressRequest } from 'express';
import type { DataResult } from '@cubejs-backend/native';
import { RequestType, ApiType, ResultType } from './strings';
import { Query } from './query';

/**
 * Network request context interface.
 * @todo requestId should be described in strings.
 * @todo securityContext description.
 * @todo why this is interface?
 */
interface RequestContext {
  securityContext: any;
  requestId: string;
  signedWithPlaygroundAuthSecret?: boolean;
  appName?: string,
  protocol?: string,
  apiType?: string,
}

/**
 * Additional request data record.
 * @todo any could be changed to unknown?
 * @todo Maybe we can add type limitations?
 */
type RequestExtension = Record<string, any>;

/**
 * Network request completed context data type.
 */
type ExtendedRequestContext =
  RequestContext & RequestExtension;

/**
 * Gateway request interface.
 */
interface Request extends ExpressRequest {
  context?: ExtendedRequestContext,
  signedWithPlaygroundAuthSecret?: boolean,

  /**
   * Security context object. Should be used instead of deprecated
   * Request#authInfo.
   * @todo any could be changed to unknown?
   * @todo Maybe we can add type limitations?
   */
  securityContext?: any,

  requestStarted?: Date,

  /**
   * @deprecated
   */
  authInfo?: any,
}

/**
 * Function that should provide basic query conversion mechanic.
 * Used as a part of a main configuration object of the server-core
 * to provide extendability to a query processing logic.
 */
type QueryRewriteFn =
  (query: Query, context: RequestContext) => Promise<Query>;

/**
 * Function that should provides a logic for extracting security
 * context from the request. Used as a part of a main configuration
 * object of the server-core to provide extendability to a query
 * security processing logic.
 * @todo any could be changed to unknown?
 * @todo Maybe we can add type limitations?
 */
type SecurityContextExtractorFn =
  (ctx: Readonly<RequestContext>) => any;

/**
 * Function that should provide a logic for extracting request
 * extension context from the request. Used as a part of a main
 * configuration object of the server-core to provide extendability
 * to a query processing logic.
 */
type ExtendContextFn =
  (req: ExpressRequest) =>
    Promise<RequestExtension> | RequestExtension;

type ErrorResponse = {
  error: string,
};

type MetaResponse = { cubes: any[], compilerId?: string };
type MetaResponseResultFn = (message: MetaResponse | ErrorResponse) => void;

/**
 * Function that should provides a logic for the response result
 * processing. Used as a part of a main configuration object of the
 * server-core to provide extendability for this logic.
 * @todo any could be changed to unknown?
 * @todo Maybe we can add type limitations?
 */
type ResponseResultFn =
  (
    message: (Record<string, any> | Record<string, any>[]) | DataResult | ErrorResponse,
    extra?: { status: number }
  ) => void;

/**
 * Base HTTP request parameters map data type.
 * @todo map it to Request.
 */
type BaseRequest = {
  context: RequestContext;
  res: ResponseResultFn
};

/**
 * Data query HTTP request parameters map data type.
 */
type QueryRequest = BaseRequest & {
  query: Record<string, any> | Record<string, any>[];
  queryType?: RequestType;
  apiType?: ApiType;
  resType?: ResultType
  memberToAlias?: Record<string, string>;
  expressionParams?: string[];
  exportAnnotatedSql?: boolean;
  memberExpressions?: boolean;
  disableExternalPreAggregations?: boolean;
  disableLimitEnforcing?: boolean;
};

type SqlApiRequest = BaseRequest & {
  query: Record<string, any>;
  sqlQuery?: [string, string[]];
  apiType?: ApiType;
  queryKey: any;
  streaming?: boolean;
  memberExpressions?: boolean;
};

/**
 * Pre-aggregations selector object.
 */
type PreAggsSelector = {
  contexts?: {securityContext: any}[],
  timezones: string[],
  dataSources?: string[],
  cubes?: string[],
  preAggregations?: string[],
};

/**
 * Posted pre-aggs job object.
 */
type PreAggJob = {
  request: string;
  context: {securityContext: any};
  preagg: string;
  table: string;
  target: string;
  structure: string;
  content: string;
  updated: number;
  key: any[];
  status: string;
  timezone: string;
  dataSource: string;
};

/**
 * The `/cubejs-system/v1/pre-aggregations/jobs` endpoint object type.
 */
type PreAggsJobsRequest = {
  action: 'post' | 'get' | 'delete',
  selector?: PreAggsSelector,
  tokens?: string[]
  resType?: 'object' | 'array'
};

type PreAggJobStatusItemNotFound = {
  token: string;
  status: 'not_found' | 'pre_agg_not_found';
};

type PreAggJobStatusItemFound = {
  token: string;
  status: string;
  table: string;
  selector: PreAggsSelector;
};

type PreAggJobStatusItem = PreAggJobStatusItemNotFound | PreAggJobStatusItemFound;

type PreAggJobStatusObject = {
  [token: string]: Omit<PreAggJobStatusItem, 'token'>
};

type PreAggJobStatusResponse =
  | PreAggJobStatusItem[]
  | PreAggJobStatusObject;

export {
  RequestContext,
  RequestExtension,
  ExtendedRequestContext,
  Request,
  SqlApiRequest,
  QueryRewriteFn,
  SecurityContextExtractorFn,
  ExtendContextFn,
  ResponseResultFn,
  MetaResponseResultFn,
  BaseRequest,
  QueryRequest,
  PreAggsJobsRequest,
  PreAggsSelector,
  PreAggJob,
  PreAggJobStatusItem,
  PreAggJobStatusObject,
  PreAggJobStatusResponse,
};
