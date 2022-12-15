/**
 * @license Apache-2.0
 * @copyright Cube Dev, Inc.
 * @fileoverview
 * Gateway server data types definition.
 */

import {
  QueryRewriteFn,
  ExtendContextFn,
} from './request';
import {
  JWTOptions,
  CheckAuthFn,
} from './auth';
import {
  CheckRestAclFn,
  CheckAuthMiddlewareFn,
  RequestLoggerMiddlewareFn,
} from '../interfaces';

type UserBackgroundContext = {
  /**
   * Security context object.
   * @todo any could be changed to unknown?
   * @todo Maybe we can add type limitations?
   */
  securityContext: any;

  /**
   * @deprecated
   */
  authInfo?: any;
};

/**
 * Function that should provides a logic of scheduled returning of
 * the user background context. Used as a part of a main
 * configuration object of the Gateway to provide extendabillity to
 * this logic.
 */
type ScheduledRefreshContextsFn =
  () => Promise<UserBackgroundContext[]>;

/**
 * Gateway configuration options interface.
 */
interface ApiGatewayOptions {
  standalone: boolean;
  dataSourceStorage: any;
  refreshScheduler: any;
  scheduledRefreshContexts?: ScheduledRefreshContextsFn;
  scheduledRefreshTimeZones?: String[];
  basePath: string;
  extendContext?: ExtendContextFn;
  jwt?: JWTOptions;
  requestLoggerMiddleware?: RequestLoggerMiddlewareFn;
  queryRewrite?: QueryRewriteFn;
  subscriptionStore?: any;
  enforceSecurityChecks?: boolean;
  playgroundAuthSecret?: string;
  serverCoreVersion?: string;
  checkAuth?: CheckAuthFn;
  /**
   * @deprecated Use checkAuth property instead.
   */
  checkAuthMiddleware?: CheckAuthMiddlewareFn;
  checkRestAcl?: CheckRestAclFn;
}

export {
  UserBackgroundContext,
  ScheduledRefreshContextsFn,
  ApiGatewayOptions,
};
