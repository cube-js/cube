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
  CheckAuthMiddlewareFn,
  RequestLoggerMiddlewareFn,
  ContextRejectionMiddlewareFn,
  ContextAcceptorFn,
  ContextToApiScopesFn,
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
 * configuration object of the Gateway to provide extendability to
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
  contextRejectionMiddleware?: ContextRejectionMiddlewareFn;
  wsContextAcceptor?: ContextAcceptorFn;
  checkAuth?: CheckAuthFn;
  /**
   * @deprecated Use checkAuth property instead.
   */
  checkAuthMiddleware?: CheckAuthMiddlewareFn;
  contextToApiScopes?: ContextToApiScopesFn;
}

export {
  UserBackgroundContext,
  ScheduledRefreshContextsFn,
  ApiGatewayOptions,
};
