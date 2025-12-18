interface LocalSubscriptionStoreOptions {
  heartBeatInterval?: number;
}

export type SubscriptionId = string | number;

export type LocalSubscriptionStoreSubscription = {
  message: any,
  state: any,
  timestamp: Date,
};

export type LocalSubscriptionStoreConnection = {
  subscriptions: Map<SubscriptionId, LocalSubscriptionStoreSubscription>,
  authContext?: any,
};

export class LocalSubscriptionStore {
  protected readonly connections: Map<string, LocalSubscriptionStoreConnection> = new Map();

  protected readonly hearBeatInterval: number;

  public constructor(options: LocalSubscriptionStoreOptions = {}) {
    this.hearBeatInterval = options.heartBeatInterval || 60;
  }

  public async getSubscription(connectionId: string, subscriptionId: string) {
    const connection = this.getConnectionOrCreate(connectionId);
    return connection.subscriptions.get(subscriptionId);
  }

  public async subscribe(connectionId: string, subscriptionId: string, subscription) {
    const connection = this.getConnectionOrCreate(connectionId);
    connection.subscriptions.set(subscriptionId, {
      ...subscription,
      timestamp: new Date()
    });
  }

  public async unsubscribe(connectionId: string, subscriptionId: SubscriptionId) {
    const connection = this.getConnectionOrCreate(connectionId);
    connection.subscriptions.delete(subscriptionId);
  }

  public getAllSubscriptions() {
    const now = Date.now();
    const staleThreshold = this.hearBeatInterval * 4 * 1000;
    const result: Array<{ connectionId: string } & LocalSubscriptionStoreSubscription> = [];

    for (const [connectionId, connection] of this.connections) {
      for (const [subscriptionId, subscription] of connection.subscriptions) {
        if (now - subscription.timestamp.getTime() > staleThreshold) {
          connection.subscriptions.delete(subscriptionId);
        }
      }

      for (const [, subscription] of connection.subscriptions) {
        result.push({ connectionId, ...subscription });
      }
    }

    return result;
  }

  public async disconnect(connectionId: string) {
    this.connections.delete(connectionId);
  }

  public async getAuthContext(connectionId: string) {
    return this.getConnectionOrCreate(connectionId).authContext;
  }

  public async setAuthContext(connectionId: string, authContext) {
    this.getConnectionOrCreate(connectionId).authContext = authContext;
  }

  protected getConnectionOrCreate(connectionId: string): LocalSubscriptionStoreConnection {
    const connect = this.connections.get(connectionId);
    if (connect) {
      return connect;
    }

    const connection = { subscriptions: new Map() };
    this.connections.set(connectionId, connection);

    return connection;
  }

  public clear() {
    this.connections.clear();
  }
}
