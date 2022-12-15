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
  CheckRestAclFn,
  CheckSQLAuthSuccessResponse,
  CheckSQLAuthFn,
  CanSwitchSQLUserFn,
} from './types/auth';

import {
  RequestContext,
  RequestExtension,
  ExtendedRequestContext,
  Request,
  QueryRewriteFn,
  SecurityContextExtractorFn,
  ExtendContextFn,
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
  CheckRestAclFn,
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
 * Logger middleware.
 * @deprecated
 */
export type RequestLoggerMiddlewareFn =
  (
    req: ExpressRequest,
    res: ExpressResponse,
    next: ExpressNextFunction,
  ) => void;
