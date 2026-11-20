/**
 * @license Apache-2.0
 * @copyright Cube Dev, Inc.
 * @fileoverview
 * Network request data types definition.
 */

import type { Request as ExpressRequest } from 'express';
import type { DataResult } from '@cubejs-backend/native';
import { CacheMode } from '@cubejs-backend/shared';
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

type MetaResponse = { cubes: any[], viewGroups?: any[], compilerId?: string };
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
  ) => void | Promise<void>;

/**
 * Base HTTP request parameters map data type.
 * @todo map it to Request.
 */
type BaseRequest = {
  context: RequestContext;
  res: ResponseResultFn
};

type RequestQuery = Record<string, any> | Record<string, any>[] & {
  cacheMode?: CacheMode;
};

/**
 * Data query HTTP request parameters map data type.
 */
type QueryRequest = BaseRequest & {
  query: RequestQuery;
  queryType?: RequestType;
  apiType?: ApiType;
  resType?: ResultType
  memberToAlias?: Record<string, string>;
  expressionParams?: string[];
  exportAnnotatedSql?: boolean;
  memberExpressions?: boolean;
  disableExternalPreAggregations?: boolean;
  disableLimitEnforcing?: boolean;
  cacheMode?: CacheMode;
};

type ConvertQuery = {
  input: 'sql';
  output: 'rest';
  query: string;
};

type QueryConvertRequest = BaseRequest & {
  payload: ConvertQuery;
};

type SqlApiRequest = BaseRequest & {
  query: Record<string, any>;
  sqlQuery?: [string, string[]];
  /**
   * `sqlQuery` reads Cube Store pre-aggregations, so it has to run on Cube Store.
   */
  external?: boolean;
  /**
   * Descriptions of the pre-aggregations `sqlQuery` reads, as the `sql` calls it was
   * built on reported them.
   */
  preAggregations?: any[];
  apiType?: ApiType;
  queryKey: any;
  streaming?: boolean;
  memberExpressions?: boolean;
  cacheMode?: CacheMode;
};

/**
 * Pre-aggregations selector object.
 */
type PreAggsSelector = {
  contexts: { securityContext: any }[],
  timezones: string[],
  dataSources?: string[],
  cubes?: string[],
  preAggregations?: string[],
  dateRange?: [string, string], // We expect only single date Range for rebuilding
  /**
   * `true` (the default) rebuilds every selected partition. `false` builds only
   * the partitions whose current version is not the one their refresh keys call
   * for; a job for an up-to-date partition reports `done` without a build.
   */
  force?: boolean,
  /**
   * Builds nothing: the post answers with each partition's version status,
   * `done`, `outdated` or `missing_partition`, instead of job tokens.
   */
  dryRun?: boolean,
};

/**
 * Posted pre-aggs job object.
 */
type PreAggJob = {
  request: string;
  context: { securityContext: any };
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
  action: 'post' | 'get',
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

/**
 * A dry-run post's answer for one partition: no job is queued, so no token.
 * `table` is the built table only for `done`; otherwise it is a placeholder
 * that a later build won't use.
 */
type PreAggJobDryRunItem = Omit<PreAggJobStatusItemFound, 'token'> & {
  status: 'done' | 'outdated' | 'missing_partition';
};

type PreAggJobStatusObject = {
  [token: string]: Omit<PreAggJobStatusItem, 'token'>
};

type PreAggJobStatusResponse =
  | PreAggJobStatusItem[]
  | PreAggJobStatusObject;

export type {
  ConvertQuery,
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
  RequestQuery,
  QueryRequest,
  QueryConvertRequest,
  PreAggsJobsRequest,
  PreAggsSelector,
  PreAggJob,
  PreAggJobStatusItem,
  PreAggJobDryRunItem,
  PreAggJobStatusObject,
  PreAggJobStatusResponse,
};
