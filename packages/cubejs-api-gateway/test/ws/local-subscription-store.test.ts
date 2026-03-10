import {
  LocalSubscriptionStore,
} from '../../src/ws/local-subscription-store';

describe('LocalSubscriptionStore', () => {
  it('stores and retrieves subscription by id', async () => {
    const store = new LocalSubscriptionStore();

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
    const store = new LocalSubscriptionStore();

    await store.subscribe('conn-1', '123', {
      message: { method: 'load' },
      state: { answer: true }
    });

    const result = await store.getSubscription('conn-1', '123');

    expect(result).toBeDefined();
    expect(result?.state).toEqual({ answer: true });
  });

  it('does not create a connection when reading missing subscription', async () => {
    const store = new LocalSubscriptionStore();

    const missing = await store.getSubscription('unknown-conn', 'sub-1');

    expect(missing).toBeUndefined();
    expect(store['connections'].size).toBe(0);
  });

  it('does not create a connection when unsubscribing unknown connection', async () => {
    const store = new LocalSubscriptionStore();

    await store.unsubscribe('unknown-conn', 'sub-1');

    expect(store['connections'].size).toBe(0);
  });

  it('unsubscribes existing subscription', async () => {
    const store = new LocalSubscriptionStore();

    await store.subscribe('conn-1', 'sub-1', {
      message: { method: 'load' },
      state: {}
    });

    await store.unsubscribe('conn-1', 'sub-1');

    const subscription = await store.getSubscription('conn-1', 'sub-1');
    expect(subscription).toBeUndefined();
  });

  it('returns all active subscriptions with connectionId', async () => {
    const store = new LocalSubscriptionStore();

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
    const store = new LocalSubscriptionStore({ heartBeatInterval: 1 });

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
    const store = new LocalSubscriptionStore();

    const authContext = { securityContext: { userId: 42 } };
    await store.setAuthContext('conn-1', authContext);

    await expect(store.getAuthContext('conn-1')).resolves.toEqual(authContext);
  });

  it('removes connection on disconnect', async () => {
    const store = new LocalSubscriptionStore();

    await store.subscribe('conn-1', 'sub-1', {
      message: { method: 'load' },
      state: {}
    });

    await store.disconnect('conn-1');

    expect(store['connections'].has('conn-1')).toBe(false);
  });

  it('clears all connections', async () => {
    const store = new LocalSubscriptionStore();

    await store.subscribe('conn-1', 'sub-1', {
      message: { method: 'load' },
      state: {}
    });
    await store.subscribe('conn-2', 'sub-2', {
      message: { method: 'subscribe' },
      state: {}
    });

    store.clear();

    expect(store['connections'].size).toBe(0);
  });
});
