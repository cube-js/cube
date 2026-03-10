interface LocalSubscriptionStoreOptions {
  heartBeatInterval?: number;
}

export type SubscriptionId = string | number;
const normalizeSubscriptionId = (subscriptionId: SubscriptionId): string => {
  if (typeof subscriptionId === 'number') {
    return subscriptionId.toString();
  }

  return subscriptionId;
};

export type LocalSubscriptionStoreSubscription = {
  message: any,
  state: any,
  timestamp: Date,
};

export type LocalSubscriptionStoreConnection = {
  subscriptions: Map<string, LocalSubscriptionStoreSubscription>,
  authContext?: any,
};

export class LocalSubscriptionStore {
  protected readonly connections: Map<string, LocalSubscriptionStoreConnection> = new Map();

  protected readonly heartBeatInterval: number;

  public constructor(options: LocalSubscriptionStoreOptions = {}) {
    this.heartBeatInterval = options.heartBeatInterval || 60;
  }

  public async getSubscription(connectionId: string, subscriptionId: SubscriptionId): Promise<LocalSubscriptionStoreSubscription | undefined> {
    // only get subscription, do not create connection if it doesn't exist
    const connection = this.getConnection(connectionId);
    if (!connection) {
      return undefined;
    }

    const normalizedSubscriptionId = normalizeSubscriptionId(subscriptionId);
    return connection.subscriptions.get(normalizedSubscriptionId);
  }

  public async subscribe(connectionId: string, subscriptionId: SubscriptionId, subscription) {
    const connection = this.getConnectionOrCreate(connectionId);
    const normalizedSubscriptionId = normalizeSubscriptionId(subscriptionId);
    connection.subscriptions.set(normalizedSubscriptionId, {
      ...subscription,
      timestamp: new Date()
    });
  }

  public async unsubscribe(connectionId: string, subscriptionId: SubscriptionId) {
    const connection = this.getConnection(connectionId);
    if (!connection) {
      return;
    }
    
    const normalizedSubscriptionId = normalizeSubscriptionId(subscriptionId);
    if (!connection.subscriptions.has(normalizedSubscriptionId)) {
      return;
    }

    connection.subscriptions.delete(normalizedSubscriptionId);
  }

  public getAllSubscriptions() {
    const now = Date.now();
    const staleThreshold = this.heartBeatInterval * 4 * 1000;
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
    const connect = this.getConnection(connectionId);
    if (connect) {
      return connect;
    }

    const connection: LocalSubscriptionStoreConnection = { subscriptions: new Map<string, LocalSubscriptionStoreSubscription>() };
    this.connections.set(connectionId, connection);

    return connection;
  }

  protected getConnection(connectionId: string): LocalSubscriptionStoreConnection | undefined {
    return this.connections.get(connectionId);
  }

  public clear() {
    this.connections.clear();
  }
}
