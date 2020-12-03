import dotenv from 'dotenv';

import CubeCore, {
  CreateOptions as CoreCreateOptions,
  CubejsServerCore,
  DatabaseType,
} from '@cubejs-backend/server-core';
import { getEnv } from '@cubejs-backend/shared';
import express from 'express';
import https from 'https';
import http from 'http';
import util from 'util';
import bodyParser from 'body-parser';
import cors, { CorsOptions } from 'cors';

import { WebSocketServer, WebSocketServerOptions } from './websocket-server';

const { version } = require('../package.json');

dotenv.config();

export type InitAppFn = (app: express.Application) => void | Promise<void>;

interface HttpOptions {
  cors?: CorsOptions;
}

export interface CreateOptions extends CoreCreateOptions, WebSocketServerOptions {
  webSockets?: boolean;
  initApp?: InitAppFn;
  http?: HttpOptions;
}

type RequireOne<T, K extends keyof T> = {
  [X in Exclude<keyof T, K>]?: T[X]
} & {
  [P in K]-?: T[P]
}

export class CubejsServer {
  protected readonly core: CubejsServerCore;

  protected readonly config: RequireOne<CreateOptions, 'webSockets' | 'http'>;

  protected redirector: http.Server | null = null;

  protected server: http.Server | https.Server | null = null;

  protected socketServer: WebSocketServer | null = null;

  public constructor(config: CreateOptions = {}) {
    this.config = {
      ...config,
      webSockets: config.webSockets || getEnv('webSockets'),
      http: {
        ...config.http,
        cors: {
          allowedHeaders: 'authorization,content-type,x-request-id',
          ...config.http?.cors
        }
      }
    };

    this.core = CubeCore.create(config);
    this.redirector = null;
    this.server = null;
  }

  public async listen(options: https.ServerOptions | http.ServerOptions = {}) {
    try {
      if (this.server) {
        throw new Error('CubeServer is already listening');
      }

      const app = express();

      app.use(cors(this.config.http.cors));
      app.use(bodyParser.json({ limit: '50mb' }));

      if (this.config.initApp) {
        await this.config.initApp(app);
      }

      await this.core.initApp(app);

      const PORT = getEnv('port');
      const TLS_PORT = getEnv('tlsPort');

      const enableTls = getEnv('tls');
      if (enableTls) {
        process.emitWarning(
          'Environment variable CUBEJS_ENABLE_TLS was deprecated and will be removed. \n' +
          'Use own reverse proxy in front of Cube.js for proxying HTTPS traffic.',
          'DeprecationWarning',
        );

        this.redirector = http.createServer((req, res) => {
          if (req.headers.host) {
            res.writeHead(301, {
              Location: `https://${req.headers.host.split(':')[0]}:${TLS_PORT}${req.url}`
            });
          }

          res.end();
        });

        this.redirector.listen(PORT);
      }

      if (enableTls) {
        this.server = https.createServer(options, app);
      } else {
        const [major] = process.version.split('.');
        if (major === '8' && Object.keys(options).length) {
          process.emitWarning(
            'There is no support for passing options inside listen method in Node.js 8.',
            'CustomWarning',
          );

          this.server = http.createServer(app);
        } else {
          this.server = http.createServer(options, app);
        }
      }

      if (this.config.webSockets) {
        this.socketServer = new WebSocketServer(this.core, this.core.options);
        this.socketServer.initServer(this.server);
      }

      await this.server.listen(enableTls ? TLS_PORT : PORT);

      return {
        app,
        port: PORT,
        tlsPort: enableTls ? TLS_PORT : undefined,
        server: this.server,
        version
      };
    } catch (e) {
      if (this.core.event) {
        await this.core.event('Dev Server Fatal Error', {
          error: (e.stack || e.message || e).toString()
        });
      }
      throw e;
    }
  }

  public testConnections() {
    return this.core.testConnections();
  }

  public runScheduledRefresh(context: any, queryingOptions: any) {
    return this.core.runScheduledRefresh(context, queryingOptions);
  }

  public async close() {
    try {
      if (this.socketServer) {
        await this.socketServer.close();
      }

      if (!this.server) {
        throw new Error('CubeServer is not started.');
      }

      await util.promisify(this.server.close)();
      this.server = null;

      if (this.redirector) {
        await util.promisify(this.redirector.close)();

        this.redirector = null;
      }

      await this.core.releaseConnections();
    } catch (e) {
      if (this.core.event) {
        await this.core.event('Dev Server Fatal Error', {
          error: (e.stack || e.message || e).toString()
        });
      }

      throw e;
    }
  }

  public static createDriver(dbType: DatabaseType) {
    return CubeCore.createDriver(dbType);
  }

  public static driverDependencies(dbType: DatabaseType) {
    return CubeCore.driverDependencies(dbType);
  }

  public static apiSecret() {
    return process.env.CUBEJS_API_SECRET;
  }

  public static version() {
    return version;
  }
}
