import dotenv from '@cubejs-backend/dotenv';

import CubeCore, {
  CreateOptions as CoreCreateOptions,
  DatabaseType,
  DriverContext,
  DriverOptions,
  SystemOptions
} from '@cubejs-backend/server-core';
import { getEnv, withTimeout } from '@cubejs-backend/shared';
import express, { Express } from 'express';
import http from 'http';
import util from 'util';
import bodyParser from 'body-parser';
import cors, { CorsOptions } from 'cors';

import type { SQLServer, SQLServerOptions } from '@cubejs-backend/api-gateway';
import type { BaseDriver } from '@cubejs-backend/query-orchestrator';

import { WebSocketServer, WebSocketServerOptions } from './websocket-server';
import { gracefulHttp, GracefulHttpServer } from './server/gracefull-http';
import { gracefulMiddleware } from './graceful-middleware';
import { ServerStatusHandler } from './server-status';

const { version } = require('../../package.json');

dotenv.config({
  multiline: 'line-breaks',
});

interface HttpOptions {
  cors?: CorsOptions;
}

export interface CreateOptions extends CoreCreateOptions, WebSocketServerOptions, SQLServerOptions {
  webSockets?: boolean;
  http?: HttpOptions;
  gracefulShutdown?: number;
}

type RequireOne<T, K extends keyof T> = {
  [X in Exclude<keyof T, K>]?: T[X]
} & {
  [P in K]-?: T[P]
};

export class CubejsServer {
  protected readonly core: CubeCore;

  protected readonly config: RequireOne<CreateOptions, 'webSockets' | 'http' | 'sqlPort' | 'pgSqlPort'>;

  protected server: GracefulHttpServer | null = null;

  protected socketServer: WebSocketServer | null = null;

  protected sqlServer: SQLServer | null = null;

  protected readonly status: ServerStatusHandler = new ServerStatusHandler();

  public constructor(config: CreateOptions = {}, systemOptions?: SystemOptions) {
    this.config = {
      ...config,
      webSockets: config.webSockets || getEnv('webSockets'),
      sqlPort: config.sqlPort || getEnv('sqlPort'),
      pgSqlPort: config.pgSqlPort || getEnv('pgSqlPort'),
      gatewayPort: config.gatewayPort || getEnv('nativeApiGatewayPort'),
      http: {
        ...config.http,
        cors: {
          allowedHeaders: 'authorization,content-type,x-request-id',
          ...config.http?.cors,
        },
      },
    };

    this.core = this.createCoreInstance(this.config, systemOptions);
    this.server = null;
  }

  protected createCoreInstance(config: CreateOptions, systemOptions?: SystemOptions): CubeCore {
    return new CubeCore(config, systemOptions);
  }

  public async listen(options: http.ServerOptions = {}): Promise<{app: Express, port: number, server: GracefulHttpServer, version: any }> {
    try {
      if (this.server) {
        throw new Error('CubeServer is already listening');
      }

      const app = express();
      app.use(cors(this.config.http.cors));
      app.use(bodyParser.json({ limit: '50mb' }));

      if (this.config.gracefulShutdown) {
        app.use(gracefulMiddleware(this.status, this.config.gracefulShutdown));
      }

      await this.core.initApp(app);

      const enableTls = getEnv('tls');
      if (enableTls) {
        throw new Error('CUBEJS_ENABLE_TLS has been deprecated and removed.');
      }

      this.server = gracefulHttp(http.createServer(options, app));

      if (this.config.webSockets) {
        this.socketServer = new WebSocketServer(this.core, this.config);
        this.socketServer.initServer(this.server);
      }

      if (this.config.sqlPort || this.config.pgSqlPort) {
        this.sqlServer = this.core.initSQLServer();
        await this.sqlServer.init(this.config);
      }

      const PORT = getEnv('port');
      await this.server.listen(PORT);

      return {
        app,
        port: PORT,
        server: this.server,
        version
      };
    } catch (e: any) {
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

  // @internal
  public handleScheduledRefreshInterval(options: any) {
    return this.core.handleScheduledRefreshInterval(options);
  }

  // @internal
  public runScheduledRefresh(context: any, queryingOptions: any) {
    return this.core.runScheduledRefresh(context, queryingOptions);
  }

  // @internal
  public async getDriver(ctx: DriverContext): Promise<BaseDriver> {
    return this.core.getDriver(ctx);
  }

  public async close() {
    try {
      if (this.socketServer) {
        await this.socketServer.close();
      }

      if (this.sqlServer) {
        await this.sqlServer.close();
      }

      if (!this.server) {
        throw new Error('CubeServer is not started.');
      }

      await util.promisify(this.server.close)();
      this.server = null;

      await this.core.releaseConnections();
    } catch (e: any) {
      if (this.core.event) {
        await this.core.event('Dev Server Fatal Error', {
          error: (e.stack || e.message || e).toString()
        });
      }

      throw e;
    }
  }

  /**
   * Create driver instance.
   *
   * TODO (buntarb): there is no usage of this method across the project.
   */
  public static createDriver(dbType: DatabaseType, opt: DriverOptions) {
    return CubeCore.createDriver(dbType, opt);
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

  public async shutdown(signal: string, graceful: boolean = true) {
    try {
      const timeoutKiller = withTimeout(
        () => {
          this.core.logger('Graceful Shutdown Timeout Kill', {
            error: 'Unable to stop Cube.js in expected time, force kill',
          });

          process.exit(1);
        },
        // this.server.stop can be closed in this.config.gracefulShutdown, let's add 1s before kill
        ((this.config.gracefulShutdown || 2) + 1) * 1000,
      );

      this.status.shutdown();

      const locks: Promise<any>[] = [
        this.core.beforeShutdown()
      ];

      if (this.socketServer) {
        locks.push(
          this.socketServer.close()
        );
      }

      if (this.sqlServer) {
        locks.push(
          this.sqlServer.shutdown(graceful && (signal === 'SIGTERM') ? 'semifast' : 'fast')
        );
      }

      if (this.server) {
        locks.push(
          this.server.stop(
            (this.config.gracefulShutdown || 2) * 1000
          )
        );
      }

      const shutdownAll = async () => {
        try {
          if (graceful) {
            // Await before all connections/refresh scheduler will end jobs
            await Promise.all(locks);
          }
          await this.core.shutdown();
        } finally {
          timeoutKiller.cancel();
        }
      };

      await Promise.any([shutdownAll(), timeoutKiller]);

      return 0;
    } catch (e: any) {
      console.error('Fatal error during server shutting down: ');
      console.error(e.stack || e);

      return 1;
    }
  }
}
