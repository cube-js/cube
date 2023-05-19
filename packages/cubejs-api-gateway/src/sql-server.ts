import {
  setupLogger,
  registerInterface,
  SqlInterfaceInstance,
  Request as NativeRequest,
  LoadRequestMeta,
} from '@cubejs-backend/native';
import { displayCLIWarning, getEnv } from '@cubejs-backend/shared';

import * as crypto from 'crypto';
import type { ApiGateway } from './gateway';
import type { CheckSQLAuthFn, ExtendedRequestContext, CanSwitchSQLUserFn } from './interfaces';

export type SQLServerOptions = {
  checkSqlAuth?: CheckSQLAuthFn,
  canSwitchSqlUser?: CanSwitchSQLUserFn,
  sqlPort?: number,
  pgSqlPort?: number,
  sqlNonce?: string,
  sqlUser?: string,
  sqlSuperUser?: string,
  sqlPassword?: string,
};

export class SQLServer {
  protected sqlInterfaceInstance: SqlInterfaceInstance | null = null;

  public constructor(
    protected readonly apiGateway: ApiGateway,
  ) {
    setupLogger(
      ({ event }) => apiGateway.log(event),
      process.env.CUBEJS_LOG_LEVEL === 'trace' ? 'trace' : 'warn'
    );
  }

  public async init(options: SQLServerOptions): Promise<void> {
    if (this.sqlInterfaceInstance) {
      throw new Error('Unable to start SQL interface two times');
    }

    const checkSqlAuth: CheckSQLAuthFn = (options.checkSqlAuth && this.wrapCheckSqlAuthFn(options.checkSqlAuth))
      || this.createDefaultCheckSqlAuthFn(options);

    const canSwitchSqlUser: CanSwitchSQLUserFn = options.canSwitchSqlUser
      || this.createDefaultCanSwitchSqlUserFn(options);

    const contextByRequest = async (request, session) => {
      let userForContext = session.user;

      if (request.meta.changeUser && request.meta.changeUser !== session.user) {
        const canSwitch = session.superuser || await canSwitchSqlUser(session.user, request.meta.changeUser);
        if (canSwitch) {
          userForContext = request.meta.changeUser;
        } else {
          throw new Error(
            `You cannot change security context via __user from ${session.user} to ${request.meta.changeUser}, because it's not allowed.`
          );
        }
      }
      // @todo Store security context in native for session's user, but not for switching
      const current = await checkSqlAuth(request, userForContext);
      return this.contextByNativeReq(request, current.securityContext, request.id);
    };

    this.sqlInterfaceInstance = await registerInterface({
      port: options.sqlPort,
      pgPort: options.pgSqlPort,
      nonce: options.sqlNonce,
      checkAuth: async ({ request, user }) => {
        const { password, superuser } = await checkSqlAuth(request, user);

        // Strip securityContext to improve speed deserialization
        return {
          password,
          superuser: superuser || false,
        };
      },
      meta: async ({ request, session }) => {
        // @todo Store security context in native
        const { securityContext } = await checkSqlAuth(request, session.user);
        const context = await this.apiGateway.contextByReq(<any> request, securityContext, request.id);

        // eslint-disable-next-line no-async-promise-executor
        return new Promise(async (resolve, reject) => {
          try {
            await this.apiGateway.meta({
              context,
              res: (message) => {
                resolve(message);
              },
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
              res: (message) => {
                resolve(message);
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
      sqlGenerators: async (paramsJson: string) => {
        // TODO get rid of it
        const { request, session } = JSON.parse(paramsJson);
        // @todo Store security context in native
        const { securityContext } = await checkSqlAuth(request, session.user);
        const context = await this.apiGateway.contextByReq(<any> request, securityContext, request.id);

        // eslint-disable-next-line no-async-promise-executor
        return new Promise(async (resolve, reject) => {
          try {
            await this.apiGateway.sqlGenerators({
              context,
              res: (queries) => {
                resolve(queries);
              },
            });
          } catch (e) {
            reject(e);
          }
        });
      },
    });
  }

  protected wrapCheckSqlAuthFn(checkSqlAuth: CheckSQLAuthFn): CheckSQLAuthFn {
    return async (req, user) => {
      const response = await checkSqlAuth(req, user);
      if (typeof response !== 'object' || response.password === null) {
        throw new Error('checkSqlAuth must return an object');
      }

      if (!response.password) {
        throw new Error('checkSqlAuth must return an object with password field');
      }

      return response;
    };
  }

  protected createDefaultCanSwitchSqlUserFn(options: SQLServerOptions): CanSwitchSQLUserFn {
    const superUser = options.sqlSuperUser || getEnv('sqlSuperUser');

    return async (current: string | null, _user: string) => {
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
        securityContext: {}
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
}
