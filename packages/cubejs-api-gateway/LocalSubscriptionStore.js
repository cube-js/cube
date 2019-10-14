class LocalSubscriptionStore {
  constructor() {
    this.connections = {};
  }

  async getSubscription(connectionId, subscriptionId) {
    const connection = this.getConnection(connectionId);
    return connection.subscriptions[subscriptionId];
  }

  async subscribe(connectionId, subscriptionId, subscription) {
    const connection = this.getConnection(connectionId);
    connection.subscriptions[subscriptionId] = subscription;
  }

  async unsubscribe(connectionId, subscriptionId) {
    const connection = this.getConnection(connectionId);
    delete connection.subscriptions[subscriptionId];
  }

  async getAllSubscriptions() {
    return Object.keys(this.connections).map(connectionId => Object.keys(this.connections[connectionId].subscriptions)
      .map(subscriptionId => ({
        connectionId,
        ...this.connections[connectionId].subscriptions[subscriptionId]
      }))).reduce((a, b) => a.concat(b), []);
  }

  async cleanupSubscriptions(connectionId) {
    delete this.connections[connectionId];
  }

  async getAuthContext(connectionId) {
    return this.getConnection(connectionId).authContext;
  }

  async setAuthContext(connectionId, authContext) {
    this.getConnection(connectionId).authContext = authContext;
  }

  getConnection(connectionId) {
    if (!this.connections[connectionId]) {
      this.connections[connectionId] = { subscriptions: {} };
    }
    return this.connections[connectionId];
  }
}

module.exports = LocalSubscriptionStore;
