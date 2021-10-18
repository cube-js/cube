import { v4 as uuidv4 } from 'uuid';

import { setLogLevel, registerInterface } from '@cubejs-backend/native';
import type { ApiGateway } from './gateway';

export interface SQLServerOptions {
  sqlPort: number,
}

export class SQLServer {
  public constructor(
    protected readonly apiGateway: ApiGateway,
  ) {
    setLogLevel(
      process.env.CUBEJS_LOG_LEVEL === 'trace' ? 'trace' : 'warn'
    );
  }

  public async init(options: SQLServerOptions): Promise<void> {
    return registerInterface({
      port: options.sqlPort,
      checkAuth: async (payload) => {
        try {
          await this.apiGateway.checkAuthFn({}, payload.authorization);

          return true;
        } catch (e) {
          return false;
        }
      },
      meta: async (payload) => {
        const authContext = await this.apiGateway.checkAuthFn({}, payload.authorization);
        const requestId = `${uuidv4()}-span-1`;
        const context = await this.apiGateway.contextByReq(<any> {}, authContext, requestId);

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
      load: async (payload) => {
        const authContext = await this.apiGateway.checkAuthFn({}, payload.authorization);
        const requestId = payload.request_id || `${uuidv4()}-span-1`;
        const context = await this.apiGateway.contextByReq(<any> {}, authContext, requestId);

        // eslint-disable-next-line no-async-promise-executor
        return new Promise(async (resolve, reject) => {
          try {
            await this.apiGateway.load({
              query: payload.query,
              queryType: 'multi',
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
    });
  }

  public async close(): Promise<void> {
    // @todo Implement
  }
}
