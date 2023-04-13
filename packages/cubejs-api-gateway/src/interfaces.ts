import type {
  Request as ExpressRequest,
  Response as ExpressResponse,
  NextFunction as ExpressNextFunction
} from 'express';

import {
  QueryTimeDimensionGranularity
} from './types/strings';

import {
  QueryType,
} from './types/enums';

import {
  QueryFilter,
  LogicalAndFilter,
  LogicalOrFilter,
  QueryTimeDimension,
  Query,
  NormalizedQueryFilter,
  NormalizedQuery,
} from './types/query';

import {
  JWTOptions,
  CheckAuthFn,
  CheckSQLAuthSuccessResponse,
  CheckSQLAuthFn,
  CanSwitchSQLUserFn,
  ContextToApiScopesFn,
} from './types/auth';

import {
  RequestContext,
  RequestExtension,
  ExtendedRequestContext,
  Request,
  QueryRewriteFn,
  SecurityContextExtractorFn,
  ExtendContextFn,
  ResponseResultFn,
  QueryRequest
} from './types/request';

export {
  QueryTimeDimensionGranularity,
  QueryType,
  QueryFilter,
  LogicalAndFilter,
  LogicalOrFilter,
  QueryTimeDimension,
  Query,
  NormalizedQueryFilter,
  NormalizedQuery,
  JWTOptions,
  CheckAuthFn,
  CheckSQLAuthSuccessResponse,
  CheckSQLAuthFn,
  CanSwitchSQLUserFn,
  RequestContext,
  RequestExtension,
  ExtendedRequestContext,
  Request,
  QueryRewriteFn,
  SecurityContextExtractorFn,
  ExtendContextFn,
  ResponseResultFn,
  QueryRequest,
  ContextToApiScopesFn,
};

/**
 * Auth middleware.
 * @deprecated
 */
export type CheckAuthMiddlewareFn =
 (
   req: Request,
   res: ExpressResponse,
   next: ExpressNextFunction,
 ) => void;

/**
 * Context rejection middleware.
 */
export type ContextRejectionMiddlewareFn =
 (
   req: Request,
   res: ExpressResponse,
   next: ExpressNextFunction,
 ) => void;

/**
 * ContextAcceptorFn type that matches the ContextAcceptor.shouldAcceptWs
 * signature from the server-core package
 */
export type ContextAcceptorFn = (context: RequestContext) => { accepted: boolean; rejectMessage?: any };

/**
 * Logger middleware.
 * @deprecated
 */
export type RequestLoggerMiddlewareFn =
  (
    req: ExpressRequest,
    res: ExpressResponse,
    next: ExpressNextFunction,
  ) => void;
