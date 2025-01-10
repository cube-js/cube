import WebSocket from 'ws';
import crypto from 'crypto';
import util from 'util';
import { CancelableInterval, createCancelableInterval } from '@cubejs-backend/shared';
import type { CubejsServerCore } from '@cubejs-backend/server-core';
import type http from 'http';
import type https from 'https';

export interface WebSocketServerOptions {
  processSubscriptionsInterval?: number,
  webSocketsBasePath?: string,
}

export class WebSocketServer {
  protected subscriptionsTimer: CancelableInterval | null = null;

  protected wsServer: WebSocket.Server | null = null;

  protected subscriptionServer: any = null;

  public constructor(
    protected readonly serverCore: CubejsServerCore,
    protected readonly options: WebSocketServerOptions = {},
  ) { }

  public initServer(server: http.Server | https.Server) {
    this.wsServer = new WebSocket.Server({
      server,
      path: this.options.webSocketsBasePath,
    });

    const connectionIdToSocket: Record<string, any> = {};

    this.subscriptionServer = this.serverCore.initSubscriptionServer(async (connectionId: string, message: any) => {
      if (!connectionIdToSocket[connectionId]) {
        throw new Error(`Socket for ${connectionId} is not found found`);
      }

      let messageStr: string;

      if (message.message && message.message.isWrapper) {
        // In case we have a wrapped query result, we don't want to parse/stringify
        // it again - it's too expensive, instead we serialize the rest of the message and then
        // inject query result json into message.
        const resMsg = new TextDecoder().decode(await message.message.getFinalResult());
        message.message = '~XXXXX~';
        messageStr = JSON.stringify(message);
        messageStr = messageStr.replace('"~XXXXX~"', resMsg);
      } else {
        messageStr = JSON.stringify(message);
      }

      connectionIdToSocket[connectionId].send(messageStr);
    });

    this.wsServer.on('connection', (ws) => {
      const connectionId = crypto.randomBytes(8).toString('hex');
      connectionIdToSocket[connectionId] = ws;

      ws.on('message', async (message) => {
        await this.subscriptionServer.processMessage(connectionId, message, true);
      });

      ws.on('close', async () => {
        await this.subscriptionServer.disconnect(connectionId);
      });

      ws.on('error', async () => {
        await this.subscriptionServer.disconnect(connectionId);
      });
    });

    const processSubscriptionsInterval = this.options.processSubscriptionsInterval || 5 * 1000;

    this.subscriptionsTimer = createCancelableInterval(
      async () => {
        await this.subscriptionServer.processSubscriptions();
      },
      {
        interval: processSubscriptionsInterval,
        onDuplicatedExecution: (intervalId) => this.serverCore.logger('WebSocket Server Interval Error', {
          error: `Previous interval #${intervalId} was not finished with ${processSubscriptionsInterval} interval`
        }),
      }
    );
  }

  public async close() {
    if (this.subscriptionsTimer) {
      await this.subscriptionsTimer.cancel();
    }

    if (this.wsServer) {
      const close = util.promisify(this.wsServer.close.bind(this.wsServer));
      await close();
    }

    this.subscriptionServer.clear();
  }
}
