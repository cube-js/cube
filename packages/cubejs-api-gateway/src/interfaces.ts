import type {
  Request as ExpressRequest,
  Response as ExpressResponse,
  NextFunction as ExpressNextFunction
} from 'express';

import {
  QueryTimeDimensionGranularity
} from './type/strings';

import {
  QueryFilter,
  LogicalAndFilter,
  LogicalOrFilter,
  QueryTimeDimension,
  Query,
  NormalizedQueryFilter,
  NormalizedQuery,
} from './type/query';

import {
  JWTOptions,
  CheckAuthFn,
  CheckSQLAuthSuccessResponse,
  CheckSQLAuthFn,
} from './type/auth';

import {
  RequestContext,
  RequestExtension,
  ExtendedRequestContext,
  Request,
  QueryRewriteFn,
  SecurityContextExtractorFn,
  ExtendContextFn,
} from './type/request';

export {
  QueryTimeDimensionGranularity,
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
