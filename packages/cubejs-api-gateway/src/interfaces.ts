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
  ResultType,
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

import {
  AliasToMemberMap,
  TransformDataResponse
} from './types/responses';

import {
  ConfigItem,
  GranularityMeta
} from './helpers/prepareAnnotation';

export {
  AliasToMemberMap,
  TransformDataResponse,
  ConfigItem,
  GranularityMeta,
  QueryTimeDimensionGranularity,
  QueryType,
  ResultType,
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
 * Context rejection middleware.
 */
export type ContextRejectionMiddlewareFn =
 (
   req: Request,
   res: ExpressResponse,
   next: ExpressNextFunction,
 ) => void;

type ContextAcceptorResult = { accepted: boolean; rejectMessage?: any };

/**
 * ContextAcceptorFn type that matches the ContextAcceptor.shouldAcceptWs
 * signature from the server-core package
 */
export type ContextAcceptorFn = (context: RequestContext) => Promise<ContextAcceptorResult> | ContextAcceptorResult;

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
