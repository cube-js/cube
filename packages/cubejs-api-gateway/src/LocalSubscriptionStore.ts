interface LocalSubscriptionStoreOptions {
  heartBeatInterval?: number;
}

const haveCommonElement = (arr1: string[], arr2: string[]): boolean => {
  return arr1.some(element => arr2.includes(element));
}

// TODO: Check whether this is the correct way to get cube names
const getCubeNames = (query) => {
  if (!query) {
    return [];
  }

  const allColumns = [
    ...(query.measures || []).map(m => m.split('.')[0]),
    ...(query.dimensions || []).map(d => d.split('.')[0]),
  ];

  return Array.from(new Set(allColumns));
};

export class LocalSubscriptionStore {
  protected connections = {};

  protected readonly hearBeatInterval: number;

  public constructor(options: LocalSubscriptionStoreOptions = {}) {
    this.hearBeatInterval = options.heartBeatInterval || 60;
  }

  public async getSubscription(connectionId: string, subscriptionId: string) {
    const connection = this.getConnection(connectionId);
    return connection.subscriptions[subscriptionId];
  }

  public async subscribe(connectionId: string, subscriptionId: string, subscription) {
    const connection = this.getConnection(connectionId);
    connection.subscriptions[subscriptionId] = {
      ...subscription,
      cubes: getCubeNames(subscription.message?.params?.query),
      timestamp: new Date()
    };
  }

  public async unsubscribe(connectionId: string, subscriptionId: string) {
    const connection = this.getConnection(connectionId);
    delete connection.subscriptions[subscriptionId];
  }

  public async getAllSubscriptions() {
    return Object.keys(this.connections).map(connectionId => {
      Object.keys(this.connections[connectionId].subscriptions).filter(
        subscriptionId => new Date().getTime() -
          this.connections[connectionId].subscriptions[subscriptionId].timestamp.getTime() >
          this.hearBeatInterval * 4 * 1000
      ).forEach(subscriptionId => { delete this.connections[connectionId].subscriptions[subscriptionId]; });

      return Object.keys(this.connections[connectionId].subscriptions)
        .map(subscriptionId => ({
          connectionId,
          ...this.connections[connectionId].subscriptions[subscriptionId]
        }));
    }).reduce((a, b) => a.concat(b), []);
  }

  public async getSubscriptionsByCubeName(cubes: Array<string>) {
    // TODO: Implement cube filtering by auth context
    return (await this.getAllSubscriptions()).filter(subscription => haveCommonElement(cubes, subscription.cubes));
  }

  public async cleanupSubscriptions(connectionId: string) {
    delete this.connections[connectionId];
  }

  public async getAuthContext(connectionId: string) {
    return this.getConnection(connectionId).authContext;
  }

  public async setAuthContext(connectionId: string, authContext) {
    this.getConnection(connectionId).authContext = authContext;
  }

  protected getConnection(connectionId: string) {
    if (!this.connections[connectionId]) {
      this.connections[connectionId] = { subscriptions: {} };
    }

    return this.connections[connectionId];
  }

  public clear() {
    this.connections = {};
  }
}
