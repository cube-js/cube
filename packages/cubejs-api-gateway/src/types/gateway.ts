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

type RequestContext = {
  // @deprecated Renamed to securityContext, please use securityContext.
  authInfo?: any;
  securityContext: any;
  requestId: string;
};

/**
 * Function that should provide a logic of scheduled returning of
 * the user background context. Used as a part of a main
 * configuration object of the Gateway to provide extendability to
 * this logic.
 */
type ScheduledRefreshContextsFn =
  () => Promise<UserBackgroundContext[]>;

type ScheduledRefreshTimeZonesFn = (context: RequestContext) => string[] | Promise<string[]>;

/**
 * Gateway configuration options interface.
 */
interface ApiGatewayOptions {
  standalone: boolean;
  gatewayPort?: number,
  dataSourceStorage: any;
  refreshScheduler: any;
  scheduledRefreshContexts?: ScheduledRefreshContextsFn;
  scheduledRefreshTimeZones?: ScheduledRefreshTimeZonesFn;
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
  contextToApiScopes?: ContextToApiScopesFn;
  event?: (name: string, props?: object) => void;
}

export {
  UserBackgroundContext,
  ScheduledRefreshContextsFn,
  ApiGatewayOptions,
};
