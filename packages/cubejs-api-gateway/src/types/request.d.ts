/**
 * @license Apache-2.0
 * @copyright Cube Dev, Inc.
 * @fileoverview
 * Network request data types definition.
 */

import type { Request as ExpressRequest } from 'express';
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

  /**
   * @deprecated
   */
  authInfo?: any,
}

/**
 * Function that should provides basic query conversion mechanic.
 * Used as a part of a main configuration object of the server-core
 * to provide extendabillity to a query processing logic.
 */
type QueryRewriteFn =
  (query: Query, context: RequestContext) => Promise<Query>;

/**
 * Function that should provides a logic for extracting security
 * context from the request. Used as a part of a main configuration
 * object of the server-core to provide extendabillity to a query
 * security processing logic.
 * @todo any could be changed to unknown?
 * @todo Maybe we can add type limitations?
 */
type SecurityContextExtractorFn =
  (ctx: Readonly<RequestContext>) => any;

/**
 * Function that should provides a logic for extracting request
 * extesion context from the request. Used as a part of a main
 * configuration object of the server-core to provide extendabillity
 * to a query processing logic.
 */
type ExtendContextFn =
  (req: ExpressRequest) =>
    Promise<RequestExtension> | RequestExtension;

/**
 * Function that should provides a logic for the response result
 * processing. Used as a part of a main configuration object of the
 * server-core to provide extendabillity for this logic.
 * @todo any could be changed to unknown?
 * @todo Maybe we can add type limitations?
 */
type ResponseResultFn =
  (
    message: Record<string, any> | Record<string, any>[],
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
};

export {
  RequestContext,
  RequestExtension,
  ExtendedRequestContext,
  Request,
  QueryRewriteFn,
  SecurityContextExtractorFn,
  ExtendContextFn,
  ResponseResultFn,
  BaseRequest,
  QueryRequest,
};
