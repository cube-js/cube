import {
  setupLogger,
  registerInterface,
  shutdownInterface,
  execSql,
  sql4sql,
  SqlInterfaceInstance,
  Request as NativeRequest,
  LoadRequestMeta,
  Sql4SqlResponse,
} from '@cubejs-backend/native';
import type { ShutdownMode } from '@cubejs-backend/native';
import { displayCLIWarning, getEnv } from '@cubejs-backend/shared';

import * as crypto from 'crypto';
import type { ApiGateway } from './gateway';
import type { CheckSQLAuthFn, ExtendedRequestContext, CanSwitchSQLUserFn } from './interfaces';

export type SQLServerOptions = {
  checkSqlAuth?: CheckSQLAuthFn,
  canSwitchSqlUser?: CanSwitchSQLUserFn,
  sqlPort?: number,
  pgSqlPort?: number,
  sqlUser?: string,
  sqlSuperUser?: string,
  sqlPassword?: string,
  gatewayPort?: number,
};

export type SQLServerConstructorOptions = {
  gatewayPort?: number,
};

export class SQLServer {
  protected sqlInterfaceInstance: SqlInterfaceInstance | null = null;

  protected readonly gatewayPort: number | undefined;

  public constructor(
    protected readonly apiGateway: ApiGateway,
    options: SQLServerConstructorOptions,
  ) {
    setupLogger(
      ({ event }) => apiGateway.log(event),
      process.env.CUBEJS_LOG_LEVEL === 'trace' ? 'trace' : 'warn'
    );

    // Actually, proxy is enabled in gateway
    // But passing port into registerInterface will start native gateway
    if (getEnv('nativeApiGateway')) {
      this.gatewayPort = options.gatewayPort || 7575;
    }
  }

  public getNativeGatewayPort(): number {
    if (this.gatewayPort) {
      return this.gatewayPort;
    }

    throw new Error('Native api gateway is not enabled');
  }

  public async execSql(sqlQuery: string, stream: any, securityContext?: any) {
    await execSql(this.sqlInterfaceInstance!, sqlQuery, stream, securityContext);
  }

  public async sql4sql(sqlQuery: string, disablePostProcessing: boolean, securityContext?: unknown): Promise<Sql4SqlResponse> {
    return sql4sql(this.sqlInterfaceInstance!, sqlQuery, disablePostProcessing, securityContext);
  }

  protected buildCheckSqlAuth(options: SQLServerOptions): CheckSQLAuthFn {
    return (options.checkSqlAuth && this.wrapCheckSqlAuthFn(options.checkSqlAuth))
      || this.createDefaultCheckSqlAuthFn(options);
  }

  public async init(options: SQLServerOptions): Promise<void> {
    if (this.sqlInterfaceInstance) {
      throw new Error('Unable to start SQL interface two times');
    }

    const checkSqlAuth: CheckSQLAuthFn = this.buildCheckSqlAuth(options);

    const canSwitchSqlUser: CanSwitchSQLUserFn = options.canSwitchSqlUser
      || this.createDefaultCanSwitchSqlUserFn(options);

    const contextByRequest = async (request, session) => {
      let userForContext = session.user;
      let { securityContext } = session;

      if (request.meta.changeUser && request.meta.changeUser !== session.user) {
        const canSwitch = session.superuser || await canSwitchSqlUser(session.user, request.meta.changeUser);
        if (canSwitch) {
          userForContext = request.meta.changeUser;
          const current = await checkSqlAuth(request, userForContext, null);
          securityContext = current.securityContext;
        } else {
          throw new Error(
            `You cannot change security context via __user from ${session.user} to ${request.meta.changeUser}, because it's not allowed.`
          );
        }
      }
      return this.contextByNativeReq(request, securityContext, request.id);
    };

    const canSwitchUserForSession = async (session, user) => session.superuser || canSwitchSqlUser(session.user, user);

    this.sqlInterfaceInstance = await registerInterface({
      gatewayPort: this.gatewayPort,
      pgPort: options.pgSqlPort,
      contextToApiScopes: async ({ securityContext }) => this.apiGateway.contextToApiScopesFn(
        securityContext,
        getEnv('defaultApiScope') || await this.apiGateway.contextToApiScopesDefFn()
      ),
      checkAuth: async ({ request, token }) => {
        const { securityContext } = await this.apiGateway.checkAuthFn(request, token);

        return {
          securityContext
        };
      },
      checkSqlAuth: async ({ request, user, password }) => {
        const { password: returnedPassword, superuser, securityContext, skipPasswordCheck } = await checkSqlAuth(request, user, password);

        return {
          password: returnedPassword,
          superuser: superuser || false,
          securityContext,
          skipPasswordCheck,
        };
      },
      meta: async ({ request, session, onlyCompilerId }) => {
        const context = await this.apiGateway.contextByReq(<any> request, session.securityContext, request.id);

        // eslint-disable-next-line no-async-promise-executor
        return new Promise(async (resolve, reject) => {
          try {
            await this.apiGateway.meta({
              context,
              res: (response) => {
                if ('error' in response) {
                  reject({
                    message: response.error
                  });

                  return;
                }

                if (onlyCompilerId) {
                  resolve({ compilerId: response.compilerId });
                } else {
                  resolve(response);
                }
              },
              includeCompilerId: true,
              onlyCompilerId
            });
          } catch (e) {
            reject(e);
          }
        });
      },
      load: async ({ request, session, query }) => {
        const context = await contextByRequest(request, session);

        // eslint-disable-next-line no-async-promise-executor
        return new Promise(async (resolve, reject) => {
          try {
            await this.apiGateway.load({
              query,
              queryType: 'multi',
              context,
              res: (response) => {
                if ('error' in response) {
                  reject({
                    message: response.error
                  });

                  return;
                }

                resolve(response);
              },
              apiType: 'sql',
            });
          } catch (e) {
            reject(e);
          }
        });
      },
      sqlApiLoad: async ({ request, session, query, queryKey, sqlQuery, streaming }) => {
        const context = await contextByRequest(request, session);

        // eslint-disable-next-line no-async-promise-executor
        return new Promise(async (resolve, reject) => {
          try {
            await this.apiGateway.sqlApiLoad({
              queryKey,
              query,
              sqlQuery,
              streaming,
              context,
              memberExpressions: true,
              res: (response) => {
                if ('error' in response) {
                  reject({
                    message: response.error
                  });

                  return;
                }

                resolve(response);
              },
              apiType: 'sql',
            });
          } catch (e) {
            reject(e);
          }
        });
      },
      sql: async ({ request, session, query, memberToAlias, expressionParams }) => {
        const context = await contextByRequest(request, session);

        // eslint-disable-next-line no-async-promise-executor
        return new Promise(async (resolve, reject) => {
          try {
            await this.apiGateway.sql({
              query,
              memberToAlias,
              expressionParams,
              exportAnnotatedSql: true,
              memberExpressions: true,
              disableExternalPreAggregations: true,
              queryType: 'multi',
              disableLimitEnforcing: true,
              context,
              res: (response) => {
                if ('error' in response) {
                  reject({
                    message: response.error
                  });

                  return;
                }

                resolve(response);
              },
              apiType: 'sql',
            });
          } catch (e) {
            reject(e);
          }
        });
      },
      stream: async ({ request, session, query }) => {
        const context = await contextByRequest(request, session);

        // eslint-disable-next-line no-async-promise-executor
        return new Promise(async (resolve, reject) => {
          try {
            resolve(await this.apiGateway.stream(context, query));
          } catch (e) {
            reject(e);
          }
        });
      },
      logLoadEvent: async ({ request, session, event, properties }) => {
        const context = await contextByRequest(request, session);

        this.apiGateway.log({
          type: event,
          ...properties
        }, context);
      },
      sqlGenerators: async (paramsJson: string) => {
        // TODO get rid of it
        const { request, session } = JSON.parse(paramsJson);
        const context = await this.apiGateway.contextByReq(<any> request, session.securityContext, request.id);

        // eslint-disable-next-line no-async-promise-executor
        return new Promise(async (resolve, reject) => {
          try {
            await this.apiGateway.sqlGenerators({
              context,
              res: (response) => {
                if ('error' in response) {
                  reject({
                    message: response.error
                  });

                  return;
                }

                resolve(response);
              },
            });
          } catch (e) {
            reject(e);
          }
        });
      },
      canSwitchUserForSession: async ({ session, user }) => canSwitchUserForSession(session, user)
    });
  }

  protected wrapCheckSqlAuthFn(checkSqlAuth: CheckSQLAuthFn): CheckSQLAuthFn {
    return async (req, user, password) => {
      const response = await checkSqlAuth(req, user, password);
      if (typeof response !== 'object') {
        throw new Error('checkSqlAuth must return an object');
      }

      return response;
    };
  }

  protected createDefaultCanSwitchSqlUserFn(options: SQLServerOptions): CanSwitchSQLUserFn {
    const superUser = options.sqlSuperUser || getEnv('sqlSuperUser');

    return async (current: string | null, newUser: string) => {
      if (current === newUser) {
        return true;
      }

      if (superUser) {
        return current === superUser;
      }

      return false;
    };
  }

  protected createDefaultCheckSqlAuthFn(options: SQLServerOptions): CheckSQLAuthFn {
    let allowedUser: string | null = options.sqlUser || getEnv('sqlUser');
    let allowedPassword: string | null = options.sqlPassword || getEnv('sqlPassword');

    if (!getEnv('devMode')) {
      if (!allowedUser) {
        allowedUser = 'cube';

        displayCLIWarning(
          'Option sqlUser is required in production mode. Cube.js will use \'cube\' as a default username.'
        );
      }

      if (!allowedPassword) {
        allowedPassword = crypto.randomBytes(16).toString('hex');

        displayCLIWarning(
          `Option sqlPassword is required in production mode. Cube.js has generated it as '${allowedPassword}'`
        );
      }
    }

    return async (req, user) => {
      if (allowedUser && user !== allowedUser) {
        throw new Error(`Incorrect user name "${user}" or password`);
      }

      return {
        password: allowedPassword,
        securityContext: {},
        skipPasswordCheck: getEnv('devMode') && !allowedPassword
      };
    };
  }

  protected async contextByNativeReq(req: NativeRequest<LoadRequestMeta>, securityContext, requestId: string): Promise<ExtendedRequestContext> {
    const context = await this.apiGateway.contextByReq(<any> req, securityContext, requestId);

    return {
      ...context,
      ...req.meta,
    };
  }

  public async close(): Promise<void> {
    // @todo Implement
  }

  public async shutdown(mode: ShutdownMode): Promise<void> {
    await shutdownInterface(this.sqlInterfaceInstance!, mode);
  }
}
