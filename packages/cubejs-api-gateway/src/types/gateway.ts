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

type GranularityListItem = string | {
  name: string;
  title?: string;
  format?: string;
  interval?: string;
  origin?: string;
  offset?: string;
};
type GranularityList = GranularityListItem[];
type GranularitiesOption =
  | GranularityList
  | ((context: RequestContext) => GranularityList | Promise<GranularityList>);

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
  /**
   * Enabled granularities (built-in names and/or custom definitions), or a function called per
   * request to produce the same. Drives /v1/granularities and the /v1/meta enrichment.
   * Shape mirrors `GranularityList` in @cubejs-backend/schema-compiler; redeclared locally to
   * avoid a dependency on schema-compiler from this types module.
   */
  granularities?: GranularitiesOption;
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
