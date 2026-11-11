interface LocalSubscriptionStoreOptions {
  heartBeatInterval?: number;
}

export type LocalSubscriptionStoreSubscription = {
  message: any,
  state: any,
  timestamp: Date,
};

export class LocalSubscriptionStoreConnection {
  public readonly subscriptions: Map<string, LocalSubscriptionStoreSubscription> = new Map();

  public authContext?: any;

  protected closed: boolean = false;

  public isClosed(): boolean {
    return this.closed;
  }

  public close(): void {
    this.closed = true;
  }
}

export class LocalSubscriptionStore {
  protected readonly connections: Map<string, LocalSubscriptionStoreConnection> = new Map();

  protected readonly heartBeatInterval: number;

  public constructor(options: LocalSubscriptionStoreOptions = {}) {
    this.heartBeatInterval = options.heartBeatInterval || 60;
  }

  public connect(connectionId: string): LocalSubscriptionStoreConnection {
    const existing = this.getConnection(connectionId);
    if (existing) {
      return existing;
    }

    const connection = new LocalSubscriptionStoreConnection();
    this.connections.set(connectionId, connection);

    return connection;
  }

  public async getSubscription(connectionId: string, subscriptionId: string): Promise<LocalSubscriptionStoreSubscription | undefined> {
    // only get subscription, do not create connection if it doesn't exist
    const connection = this.getConnection(connectionId);
    if (!connection) {
      return undefined;
    }

    return connection.subscriptions.get(subscriptionId);
  }

  public async subscribe(connectionId: string, subscriptionId: string, subscription) {
    // A subscribe that finishes after the disconnect must not bring the connection back
    const connection = this.getConnection(connectionId);
    if (!connection) {
      return;
    }

    connection.subscriptions.set(subscriptionId, {
      ...subscription,
      timestamp: new Date()
    });
  }

  public async unsubscribe(connectionId: string, subscriptionId: string) {
    const connection = this.getConnection(connectionId);
    if (!connection) {
      return;
    }

    if (!connection.subscriptions.has(subscriptionId)) {
      return;
    }

    connection.subscriptions.delete(subscriptionId);
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
    const connection = this.getConnection(connectionId);
    if (!connection) {
      return;
    }

    connection.close();
    this.connections.delete(connectionId);
  }

  public async getAuthContext(connectionId: string) {
    return this.getConnection(connectionId)?.authContext;
  }

  public async setAuthContext(connectionId: string, authContext) {
    const connection = this.getConnection(connectionId);
    if (!connection) {
      return;
    }

    connection.authContext = authContext;
  }

  protected getConnection(connectionId: string): LocalSubscriptionStoreConnection | undefined {
    return this.connections.get(connectionId);
  }

  public clear() {
    for (const connection of this.connections.values()) {
      connection.close();
    }

    this.connections.clear();
  }
}
