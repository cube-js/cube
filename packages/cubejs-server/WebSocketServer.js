const WebSocket = require('ws');
const crypto = require('crypto');
const util = require('util');

class WebSocketServer {
  constructor(serverCore, options) {
    options = options || {};
    this.serverCore = serverCore;
    this.processSubscriptionsInterval = options.processSubscriptionsInterval || 5;
    this.webSocketsBasePath = options.webSocketsBasePath;
  }

  initServer(server) {
    this.wsServer = this.webSocketsBasePath ?
      new WebSocket.Server({ server, path: this.webSocketsBasePath }) : new WebSocket.Server({ server });

    const connectionIdToSocket = {};

    const subscriptionServer = this.serverCore.initSubscriptionServer((connectionId, message) => {
      if (!connectionIdToSocket[connectionId]) {
        throw new Error(`Socket for ${connectionId} is not found found`);
      }
      connectionIdToSocket[connectionId].send(JSON.stringify(message));
    });

    this.wsServer.on('connection', (ws) => {
      const connectionId = crypto.randomBytes(8).toString('hex');
      connectionIdToSocket[connectionId] = ws;
      ws.on('message', async (message) => {
        await subscriptionServer.processMessage(connectionId, message);
      });

      ws.on('close', async () => {
        await subscriptionServer.disconnect(connectionId);
      });

      ws.on('error', async () => {
        await subscriptionServer.disconnect(connectionId);
      });
    });

    this.subscriptionsTimer = setInterval(async () => {
      await subscriptionServer.processSubscriptions();
    }, this.processSubscriptionsInterval * 1000);
  }

  async close() {
    if (this.wsServer) {
      const close = util.promisify(this.wsServer.close.bind(this.wsServer));
      await close();
    }
    if (this.subscriptionsTimer) {
      clearInterval(this.subscriptionsTimer);
    }
  }
}

module.exports = WebSocketServer;
