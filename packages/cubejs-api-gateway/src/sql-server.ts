import { v4 as uuidv4 } from 'uuid';

import {registerInterface} from '@cubejs-backend/native';
import type { ApiGateway } from './gateway';

export interface SQLServerOptions {
  sqlPort: number,
}

export class SQLServer {
  public constructor(
    protected readonly apiGateway: ApiGateway,
  ) {
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
        const requestId = `${uuidv4()}-span-${uuidv4()}`;
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
        const requestId = `${uuidv4()}-span-${uuidv4()}`;
        const context = await this.apiGateway.contextByReq(<any> {}, authContext, requestId);

        // eslint-disable-next-line no-async-promise-executor
        return new Promise(async (resolve, reject) => {
          try {
            await this.apiGateway.load({
              query: payload.query,
              queryType: 'multi',
              context,
              res: (message) => {
                console.log(message);
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
