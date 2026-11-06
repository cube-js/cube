import type {
  Request as ExpressRequest,
  Response as ExpressResponse,
  NextFunction as ExpressNextFunction
} from 'express';

import {
  QueryTimeDimensionGranularity
} from './types/strings';

import {
  QueryTypeEnum,
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
  MemberExpression,
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
} from './helpers/prepare-annotation';

export {
  type AliasToMemberMap,
  type TransformDataResponse,
  type ConfigItem,
  type GranularityMeta,
  type QueryTimeDimensionGranularity,
  QueryTypeEnum as QueryType,
  ResultType,
  type QueryFilter,
  type LogicalAndFilter,
  type LogicalOrFilter,
  type QueryTimeDimension,
  type Query,
  type NormalizedQueryFilter,
  type NormalizedQuery,
  type MemberExpression,
  type JWTOptions,
  type CheckAuthFn,
  type CheckSQLAuthSuccessResponse,
  type CheckSQLAuthFn,
  type CanSwitchSQLUserFn,
  type RequestContext,
  type RequestExtension,
  type ExtendedRequestContext,
  type Request,
  type QueryRewriteFn,
  type SecurityContextExtractorFn,
  type ExtendContextFn,
  type ResponseResultFn,
  type QueryRequest,
  type ContextToApiScopesFn,
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
