import {
  LocalSubscriptionStore,
} from '../../src/ws/local-subscription-store';

class TestSubscriptionStore extends LocalSubscriptionStore {
  public connectionIds(): string[] {
    return [...this.connections.keys()];
  }
}

describe('LocalSubscriptionStore', () => {
  it('stores and retrieves subscription by id', async () => {
    const store = new TestSubscriptionStore();
    store.connect('conn-1');

    await store.subscribe('conn-1', 'sub-1', {
      message: { method: 'load' },
      state: { foo: 'bar' }
    });

    const subscription = await store.getSubscription('conn-1', 'sub-1');

    expect(subscription).toBeDefined();
    expect(subscription?.message).toEqual({ method: 'load' });
    expect(subscription?.state).toEqual({ foo: 'bar' });
    expect(subscription?.timestamp).toBeInstanceOf(Date);
  });

  it('stores and retrieves subscription by string id', async () => {
    const store = new TestSubscriptionStore();
    store.connect('conn-1');

    await store.subscribe('conn-1', '123', {
      message: { method: 'load' },
      state: { answer: true }
    });

    const result = await store.getSubscription('conn-1', '123');

    expect(result).toBeDefined();
    expect(result?.state).toEqual({ answer: true });
  });

  it('does not create a connection when reading missing subscription', async () => {
    const store = new TestSubscriptionStore();

    const missing = await store.getSubscription('unknown-conn', 'sub-1');

    expect(missing).toBeUndefined();
    expect(store.connectionIds()).toEqual([]);
  });

  it('does not create a connection when unsubscribing unknown connection', async () => {
    const store = new TestSubscriptionStore();

    await store.unsubscribe('unknown-conn', 'sub-1');

    expect(store.connectionIds()).toEqual([]);
  });

  it('does not create a connection when subscribing unknown connection', async () => {
    const store = new TestSubscriptionStore();

    await store.subscribe('unknown-conn', 'sub-1', {
      message: { method: 'load' },
      state: {}
    });

    expect(store.getAllSubscriptions()).toEqual([]);
    expect(store.connectionIds()).toEqual([]);
  });

  it('does not create a connection when reading auth context of unknown connection', async () => {
    const store = new TestSubscriptionStore();

    await expect(store.getAuthContext('unknown-conn')).resolves.toBeUndefined();
    expect(store.connectionIds()).toEqual([]);
  });

  it('does not create a connection when setting auth context of unknown connection', async () => {
    const store = new TestSubscriptionStore();

    await store.setAuthContext('unknown-conn', { securityContext: {} });

    expect(store.connectionIds()).toEqual([]);
  });

  it('returns the same connection until it disconnects', async () => {
    const store = new TestSubscriptionStore();

    const connection = store.connect('conn-1');
    expect(connection.isClosed()).toBe(false);
    expect(store.connect('conn-1')).toBe(connection);

    await store.disconnect('conn-1');

    expect(connection.isClosed()).toBe(true);
    await store.setAuthContext('conn-1', { securityContext: {} });
    await expect(store.getAuthContext('conn-1')).resolves.toBeUndefined();
  });

  it('unsubscribes existing subscription', async () => {
    const store = new TestSubscriptionStore();
    store.connect('conn-1');

    await store.subscribe('conn-1', 'sub-1', {
      message: { method: 'load' },
      state: {}
    });

    await store.unsubscribe('conn-1', 'sub-1');

    const subscription = await store.getSubscription('conn-1', 'sub-1');
    expect(subscription).toBeUndefined();
  });

  it('returns all active subscriptions with connectionId', async () => {
    const store = new TestSubscriptionStore();
    store.connect('conn-1');
    store.connect('conn-2');

    await store.subscribe('conn-1', 'sub-1', {
      message: { method: 'load' },
      state: { a: 1 }
    });
    await store.subscribe('conn-2', 'sub-2', {
      message: { method: 'subscribe' },
      state: { b: 2 }
    });

    const allSubscriptions = store.getAllSubscriptions();

    expect(allSubscriptions).toHaveLength(2);
    expect(allSubscriptions).toEqual(expect.arrayContaining([
      expect.objectContaining({
        connectionId: 'conn-1',
        message: { method: 'load' },
        state: { a: 1 }
      }),
      expect.objectContaining({
        connectionId: 'conn-2',
        message: { method: 'subscribe' },
        state: { b: 2 }
      })
    ]));
  });

  it('removes stale subscriptions during getAllSubscriptions', async () => {
    const store = new TestSubscriptionStore({ heartBeatInterval: 1 });
    store.connect('conn-1');

    await store.subscribe('conn-1', 'stale', {
      message: { method: 'load' },
      state: {}
    });
    await store.subscribe('conn-1', 'active', {
      message: { method: 'load' },
      state: {}
    });

    const staleSubscription = await store.getSubscription('conn-1', 'stale');
    expect(staleSubscription).toBeDefined();
    if (!staleSubscription) {
      throw new Error('Expected stale subscription to exist');
    }
    staleSubscription.timestamp = new Date(Date.now() - 5000);

    const allSubscriptions = store.getAllSubscriptions();

    expect(allSubscriptions).toHaveLength(1);
    expect(allSubscriptions[0].connectionId).toBe('conn-1');
    expect(allSubscriptions[0].message).toEqual({ method: 'load' });

    const staleAfterCleanup = await store.getSubscription('conn-1', 'stale');
    expect(staleAfterCleanup).toBeUndefined();
  });

  it('stores and retrieves auth context', async () => {
    const store = new TestSubscriptionStore();

    const authContext = { securityContext: { userId: 42 } };
    store.connect('conn-1');
    await store.setAuthContext('conn-1', authContext);

    await expect(store.getAuthContext('conn-1')).resolves.toEqual(authContext);
  });

  it('removes connection on disconnect', async () => {
    const store = new TestSubscriptionStore();
    store.connect('conn-1');

    await store.subscribe('conn-1', 'sub-1', {
      message: { method: 'load' },
      state: {}
    });

    await store.disconnect('conn-1');

    expect(store.connectionIds()).not.toContain('conn-1');
  });

  it('clears all connections', async () => {
    const store = new TestSubscriptionStore();
    store.connect('conn-1');
    store.connect('conn-2');

    await store.subscribe('conn-1', 'sub-1', {
      message: { method: 'load' },
      state: {}
    });
    await store.subscribe('conn-2', 'sub-2', {
      message: { method: 'subscribe' },
      state: {}
    });

    store.clear();

    expect(store.connectionIds()).toEqual([]);
  });
});
