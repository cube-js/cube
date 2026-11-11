import WebSocket from 'ws';
import crypto from 'crypto';
import util from 'util';
import { CancelableInterval, createCancelableInterval, getEnv } from '@cubejs-backend/shared';

import type { CubejsServerCore } from '@cubejs-backend/server-core';
import type http from 'http';
import type https from 'https';
import type { SubscriptionServer } from '@cubejs-backend/api-gateway';

export interface WebSocketServerOptions {
  processSubscriptionsInterval?: number,
  webSocketsBasePath?: string,
}

export class WebSocketServer {
  protected subscriptionsTimer: CancelableInterval | null = null;

  protected wsServer: WebSocket.Server | null = null;

  protected subscriptionServer: SubscriptionServer | null = null;

  public constructor(
    protected readonly serverCore: CubejsServerCore,
    protected readonly options: WebSocketServerOptions = {},
  ) { }

  public initServer(server: http.Server | https.Server) {
    this.wsServer = new WebSocket.Server({
      server,
      path: this.options.webSocketsBasePath,
      maxPayload: getEnv('maxRequestSize'),
    });

    const connectionIdToSocket: Record<string, any> = {};

    this.subscriptionServer = this.serverCore.initSubscriptionServer(async (connectionId: string, message: any) => {
      if (!connectionIdToSocket[connectionId]) {
        throw new Error(`Socket for ${connectionId} is not found found`);
      }

      let messageStr: string;

      try {
        messageStr = await this.serializeMessage(message);
      } catch (e: any) {
        // A message that can't be serialized (e.g. too deeply nested) must only cost its own connection
        this.serverCore.logger('WebSocket Send Error', {
          connectionId,
          error: e?.stack || String(e),
        });
        connectionIdToSocket[connectionId].close(1011, 'Internal error');
        throw e;
      }

      connectionIdToSocket[connectionId].send(messageStr);
    });

    this.wsServer.on('connection', (ws) => {
      const connectionId = crypto.randomBytes(8).toString('hex');
      connectionIdToSocket[connectionId] = ws;

      // An exception escaping these async listeners would be an unhandled rejection, which terminates the process
      ws.on('message', async (message) => {
        try {
          await this.subscriptionServer!.processMessage(connectionId, message as string);
        } catch (e: any) {
          this.serverCore.logger('WebSocket Message Error', {
            connectionId,
            error: e?.stack || String(e),
          });
          ws.close(1011, 'Internal error');
        }
      });

      const disconnect = async () => {
        delete connectionIdToSocket[connectionId];

        try {
          await this.subscriptionServer!.disconnect(connectionId);
        } catch (e: any) {
          this.serverCore.logger('WebSocket Disconnect Error', {
            connectionId,
            error: e?.stack || String(e),
          });
        }
      };

      ws.on('close', disconnect);
      ws.on('error', disconnect);
    });

    const processSubscriptionsInterval = this.options.processSubscriptionsInterval || 5 * 1000;

    this.subscriptionsTimer = createCancelableInterval(
      async () => {
        await this.subscriptionServer!.processSubscriptions();
      },
      {
        interval: processSubscriptionsInterval,
        onDuplicatedExecution: (intervalId) => this.serverCore.logger('WebSocket Server Interval Error', {
          error: `Previous interval #${intervalId} was not finished with ${processSubscriptionsInterval} interval`
        }),
      }
    );
  }

  protected async serializeMessage(message: any): Promise<string> {
    if (message.message && message.message.isWrapper) {
      // In case we have a wrapped query result, we don't want to parse/stringify
      // it again - it's too expensive, instead we serialize the rest of the message and then
      // inject query result json into message.
      const resMsg = new TextDecoder().decode(await message.message.getFinalResult());
      delete message.message;
      const messageStr = JSON.stringify(message);

      if (messageStr === '{}') {
        return `{"message":${resMsg}}`;
      }

      return `${messageStr.slice(0, -1)},"message":${resMsg}}`;
    }

    return JSON.stringify(message);
  }

  public async close() {
    if (this.subscriptionsTimer) {
      await this.subscriptionsTimer.cancel();
    }

    if (this.wsServer) {
      const close = util.promisify(this.wsServer.close.bind(this.wsServer));
      await close();
    }

    if (this.subscriptionServer) {
      this.subscriptionServer.clear();
    }
  }
}
