/**
 * @license Apache-2.0
 * @copyright Cube Dev, Inc.
 * @fileoverview
 * Cube.js auth related data types definition.
 */

import { Request } from './request';

/**
 * Internal auth logic options object data type.
 */
type CheckAuthInternalOptions = {
  isPlaygroundCheckAuth: boolean;
};

/**
 * JWT options. Used as a part of a main configuration object of
 * the server-core to provide JWT configuration.
 */
interface JWTOptions {
  // JWK options
  jwkRetry?: number,
  jwkDefaultExpire?: number,
  jwkUrl?: ((payload: any) => string) | string,
  jwkRefetchWindow?: number,

  // JWT options
  key?: string,
  algorithms?: string[],
  issuer?: string[],
  audience?: string,
  subject?: string,
  claimsNamespace?: string,
}

/**
 * Function that should provides basic auth mechanic. Used as a part
 * of a main configuration object of the server-core to provide base
 * auth logic.
 * @todo ctx can be passed from SubscriptionServer that will cause
 * incapability with Express.Request
 */
type CheckAuthFn =
  (ctx: any, authorization?: string) => Promise<void> | void;

/**
 * Function that must provide REST API ACL validation logic.
 *
 * @throw CubejsHandlerError
 */
type CheckRestAclFn = (
  req: Request,
  whiteList: [url: string, methods: string[]][],
  blackList: [url: string, methods: string[]][],
) => Promise<void>;

/**
 * Result of the SQL auth workflow.
 */
type CheckSQLAuthSuccessResponse = {
  password: string | null,
  superuser?: boolean,
  securityContext?: any
};

/**
 * Function that should provide SQL auth mechanic. Used as a part
 * of a main configuration object of the server-core to provide SQL
 * auth logic.
 */
type CheckSQLAuthFn =
  (ctx: any, user: string | null) =>
    Promise<CheckSQLAuthSuccessResponse> |
    CheckSQLAuthSuccessResponse;

/**
 * Function that should provide changing of security context (__user field) for SQL. This function returns boolean which
 * explains to SQL APi that it's possible to change current user to user.
 */
type CanSwitchSQLUserFn =
  (current: string | null, user: string) =>
    Promise<boolean> |
    boolean;

export {
  CheckAuthInternalOptions,
  JWTOptions,
  CheckAuthFn,
  CheckRestAclFn,
  CheckSQLAuthSuccessResponse,
  CheckSQLAuthFn,
  CanSwitchSQLUserFn,
};
