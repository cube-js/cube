const uuid = require('uuid/v4');
const UserError = require('./UserError');

const methodParams = {
  load: ['query'],
  sql: ['query'],
  meta: [],
  subscribe: ['query'],
  unsubscribe: []
};

class SubscriptionServer {
  constructor(apiGateway, sendMessage, subscriptionStore) {
    this.apiGateway = apiGateway;
    this.sendMessage = sendMessage;
    this.subscriptionStore = subscriptionStore;
  }

  resultFn(connectionId, messageId) {
    return (message, { status } = {}) => this.sendMessage(connectionId, { messageId, message, status: status || 200 });
  }

  async processMessage(connectionId, message, isSubscription) {
    let authContext = {};
    let context = {};
    try {
      if (typeof message === 'string') {
        message = JSON.parse(message);
      }
      if (message.authorization) {
        authContext = { isSubscription: true };
        await this.apiGateway.checkAuthFn(authContext, message.authorization);
        await this.subscriptionStore.setAuthContext(connectionId, authContext);
        this.sendMessage(connectionId, { handshake: true });
        return;
      }

      if (message.unsubscribe) {
        await this.subscriptionStore.unsubscribe(connectionId, message.unsubscribe);
        return;
      }

      if (!message.messageId) {
        throw new UserError(`messageId is required`);
      }

      authContext = await this.subscriptionStore.getAuthContext(connectionId);

      if (!authContext) {
        await this.sendMessage(
          connectionId,
          {
            messageId: message.messageId,
            message: { error: 'Not authorized' },
            status: 403
          }
        );
        return;
      }

      if (!methodParams[message.method]) {
        throw new UserError(`Unsupported method: ${message.method}`);
      }

      const baseRequestId = message.requestId || `${connectionId}-${message.messageId}`;
      const requestId = `${baseRequestId}-span-${uuid()}`;
      context = await this.apiGateway.contextByReq(message, authContext.authInfo, requestId);

      const allowedParams = methodParams[message.method];
      const params = allowedParams.map(k => ({ [k]: (message.params || {})[k] }))
        .reduce((a, b) => ({ ...a, ...b }), {});
      await this.apiGateway[message.method]({
        ...params,
        context,
        isSubscription,
        res: this.resultFn(connectionId, message.messageId),
        subscriptionState: async () => {
          const subscription = await this.subscriptionStore.getSubscription(connectionId, message.messageId);
          return subscription && subscription.state;
        },
        subscribe: async (state) => this.subscriptionStore.subscribe(connectionId, message.messageId, {
          message,
          state
        }),
        unsubscribe: async () => this.subscriptionStore.unsubscribe(connectionId, message.messageId)
      });
      await this.sendMessage(connectionId, { messageProcessedId: message.messageId });
    } catch (e) {
      this.apiGateway.handleError({
        e,
        query: message.query,
        res: this.resultFn(connectionId, message.messageId),
        context
      });
    }
  }

  async processSubscriptions() {
    const allSubscriptions = await this.subscriptionStore.getAllSubscriptions();
    await Promise.all(allSubscriptions.map(async subscription => {
      await this.processMessage(subscription.connectionId, subscription.message, true);
    }));
  }

  async disconnect(connectionId) {
    await this.subscriptionStore.cleanupSubscriptions(connectionId);
  }
}

module.exports = SubscriptionServer;
