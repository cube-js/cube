import { SubscriptionServer } from '../../src/ws/subscription-server';
import { LocalSubscriptionStore, LocalSubscriptionStoreConnection } from '../../src/ws/local-subscription-store';
import { ApiGateway } from '../../src/gateway';
import { CubejsHandlerError } from '../../src/cubejs-handler-error';

class TestSubscriptionStore extends LocalSubscriptionStore {
  public connectionIds(): string[] {
    return [...this.connections.keys()];
  }
}

const deferred = () => {
  let resolve!: () => void;
  const promise = new Promise<void>((r) => {
    resolve = r;
  });

  return { promise, resolve };
};

const createMocks = () => {
  const sentMessages: any[] = [];

  const mockApiGateway: any = {
    checkAuthFn: jest.fn().mockResolvedValue(undefined),
    contextByReq: jest.fn().mockResolvedValue({ requestId: 'test-req' }),
    log: jest.fn(),
    handleError: jest.fn(),
    load: jest.fn().mockResolvedValue(undefined),
    sql: jest.fn().mockResolvedValue(undefined),
    dryRun: jest.fn().mockResolvedValue(undefined),
    meta: jest.fn().mockResolvedValue(undefined),
    subscribe: jest.fn().mockResolvedValue(undefined),
  };

  const mockSubscriptionStore: any = {
    connect: jest.fn().mockReturnValue(new LocalSubscriptionStoreConnection()),
    setAuthContext: jest.fn().mockResolvedValue(undefined),
    getAuthContext: jest.fn().mockResolvedValue({ securityContext: {}, expiresAt: Number.MAX_SAFE_INTEGER }),
    subscribe: jest.fn().mockResolvedValue(undefined),
    unsubscribe: jest.fn().mockResolvedValue(undefined),
    getSubscription: jest.fn().mockResolvedValue(null),
    disconnect: jest.fn().mockResolvedValue(undefined),
  };

  const mockSendMessage = jest.fn().mockImplementation(async (_connId, msg) => {
    sentMessages.push(msg);
  });

  const mockContextAcceptor = jest.fn().mockResolvedValue({ accepted: true });

  return {
    mockApiGateway,
    mockSubscriptionStore,
    mockSendMessage,
    mockContextAcceptor,
    sentMessages,
  };
};

describe('SubscriptionServer', () => {
  describe('Subscription tick', () => {
    // JSON.stringify throws on a BigInt on every Node.js version, unlike on deep nesting
    const unserializableQuery = { limit: BigInt(1) };

    const subscriptionMessage = (messageId: string, query: unknown) => ({
      method: 'subscribe',
      messageId,
      params: { query },
    });

    const createServer = () => {
      const { mockApiGateway, mockSubscriptionStore, mockSendMessage, mockContextAcceptor } = createMocks();
      mockSubscriptionStore.getAllSubscriptions = jest.fn().mockReturnValue([
        { connectionId: 'conn-1', message: subscriptionMessage('1', unserializableQuery) },
        { connectionId: 'conn-2', message: subscriptionMessage('2', { measures: ['Orders.count'] }) },
      ]);
      const server = new SubscriptionServer(mockApiGateway, mockSendMessage, mockSubscriptionStore, mockContextAcceptor);

      return { server, mockApiGateway };
    };

    it('should route a message that cannot be measured to error handling', async () => {
      const { server, mockApiGateway } = createServer();

      await expect(server.processSubscriptions()).resolves.toBeUndefined();

      expect(mockApiGateway.handleError).toHaveBeenCalledTimes(1);
      expect(mockApiGateway.handleError.mock.calls[0][0].e).toBeInstanceOf(TypeError);
      expect(mockApiGateway.subscribe).toHaveBeenCalledTimes(1);
      expect(mockApiGateway.subscribe).toHaveBeenCalledWith(expect.objectContaining({ connectionId: 'conn-2' }));
    });

    it('should not abort the tick when error handling itself fails', async () => {
      const { server, mockApiGateway } = createServer();
      mockApiGateway.handleError.mockImplementation(() => {
        throw new RangeError('Maximum call stack size exceeded');
      });

      await expect(server.processSubscriptions()).resolves.toBeUndefined();

      expect(mockApiGateway.subscribe).toHaveBeenCalledWith(expect.objectContaining({ connectionId: 'conn-2' }));
      expect(mockApiGateway.log).toHaveBeenCalledWith(
        expect.objectContaining({ type: 'WebSocket Subscription Error', connectionId: 'conn-1' })
      );
    });
  });

  describe('Disconnect during an in-flight handshake', () => {
    it('should not bring the connection back', async () => {
      const { mockApiGateway, mockSendMessage, mockContextAcceptor } = createMocks();
      const store = new TestSubscriptionStore();
      const auth = deferred();
      mockApiGateway.checkAuthFn = jest.fn(async (authContext: any) => {
        await auth.promise;
        authContext.securityContext = { userId: 42 };
      });
      const server = new SubscriptionServer(mockApiGateway, mockSendMessage, store, mockContextAcceptor);

      const inFlight = server.processMessage('conn-1', JSON.stringify({ authorization: 'token' }));
      await new Promise((resolve) => setImmediate(resolve));
      expect(mockApiGateway.checkAuthFn).toHaveBeenCalledTimes(1);

      await server.disconnect('conn-1');

      auth.resolve();
      await inFlight;

      expect(store.connectionIds()).toEqual([]);
      expect(await store.getAuthContext('conn-1')).toBeUndefined();
    });
  });

  describe('Disconnect during an in-flight subscribe', () => {
    it('should not bring the connection back', async () => {
      const { mockApiGateway, mockContextAcceptor } = createMocks();
      const store = new TestSubscriptionStore();
      const sentMessages: any[] = [];
      // A closed socket: ws drops the message without throwing
      const sendToClosedSocket = jest.fn(async (_connectionId: string, message: any) => {
        sentMessages.push(message);
      });

      const load = deferred();
      // Mirrors the gateway: load, reply, then record the subscription
      mockApiGateway.subscribe = jest.fn(async ({ res, subscribe }) => {
        await load.promise;
        await res({ data: [] });
        await subscribe({ resultHash: 'hash' });
      });

      const server = new SubscriptionServer(mockApiGateway, sendToClosedSocket, store, mockContextAcceptor);

      await server.processMessage('conn-1', JSON.stringify({ authorization: 'token' }));
      const inFlight = server.processMessage('conn-1', JSON.stringify({
        method: 'subscribe',
        messageId: '1',
        params: { query: { measures: ['Orders.count'] } },
      }));
      await new Promise((resolve) => setImmediate(resolve));
      expect(mockApiGateway.subscribe).toHaveBeenCalledTimes(1);

      await server.disconnect('conn-1');
      expect(store.connectionIds()).toEqual([]);

      load.resolve();
      await inFlight;

      expect(store.connectionIds()).toEqual([]);
      expect(store.getAllSubscriptions()).toEqual([]);

      sentMessages.length = 0;
      await server.processSubscriptions();

      expect(mockApiGateway.subscribe).toHaveBeenCalledTimes(1);
      expect(sentMessages).toEqual([]);
    });
  });

  describe('Message Validation', () => {
    it('should accept valid auth message', async () => {
      const { mockApiGateway, mockSubscriptionStore, mockSendMessage, mockContextAcceptor, sentMessages } = createMocks();
      const server = new SubscriptionServer(mockApiGateway, mockSendMessage, mockSubscriptionStore, mockContextAcceptor);

      await server.processMessage('conn-1', JSON.stringify({ authorization: 'token123' }));

      expect(mockApiGateway.checkAuthFn).toHaveBeenCalled();
      expect(sentMessages).toContainEqual({ handshake: true });
    });

    it('should accept valid unsubscribe message', async () => {
      const { mockApiGateway, mockSubscriptionStore, mockSendMessage, mockContextAcceptor } = createMocks();
      const server = new SubscriptionServer(mockApiGateway, mockSendMessage, mockSubscriptionStore, mockContextAcceptor);

      await server.processMessage('conn-1', JSON.stringify({ unsubscribe: 'msg-1' }));

      expect(mockSubscriptionStore.unsubscribe).toHaveBeenCalledWith('conn-1', 'msg-1');
    });

    it('should convert numeric unsubscribe id to string', async () => {
      const { mockApiGateway, mockSubscriptionStore, mockSendMessage, mockContextAcceptor } = createMocks();
      const server = new SubscriptionServer(mockApiGateway, mockSendMessage, mockSubscriptionStore, mockContextAcceptor);

      await server.processMessage('conn-1', JSON.stringify({ unsubscribe: 123 }));

      const callArgs = mockSubscriptionStore.unsubscribe.mock.calls[0];
      expect(typeof callArgs[1]).toBe('string');
      expect(callArgs[1]).toBe('123');
    });

    it('should accept valid load message', async () => {
      const { mockApiGateway, mockSubscriptionStore, mockSendMessage, mockContextAcceptor, sentMessages } = createMocks();
      const server = new SubscriptionServer(mockApiGateway, mockSendMessage, mockSubscriptionStore, mockContextAcceptor);

      const message = {
        method: 'load',
        messageId: '123',
        params: { query: { measures: ['Orders.count'] } }
      };
      await server.processMessage('conn-1', JSON.stringify(message));

      expect(mockApiGateway.load).toHaveBeenCalled();
      expect(sentMessages).toContainEqual({ messageProcessedId: '123' });
    });

    it('should convert numeric messageId to string', async () => {
      const { mockApiGateway, mockSubscriptionStore, mockSendMessage, mockContextAcceptor, sentMessages } = createMocks();
      const server = new SubscriptionServer(mockApiGateway, mockSendMessage, mockSubscriptionStore, mockContextAcceptor);

      const message = {
        method: 'load',
        messageId: 123,
        params: { query: { measures: ['Orders.count'] } }
      };
      await server.processMessage('conn-1', JSON.stringify(message));

      expect(mockApiGateway.load).toHaveBeenCalled();

      const processedMsg = sentMessages.find((m) => m.messageProcessedId !== undefined);
      expect(typeof processedMsg.messageProcessedId).toBe('string');
      expect(processedMsg.messageProcessedId).toBe('123');
    });

    it('should reject invalid JSON payload', async () => {
      const { mockApiGateway, mockSubscriptionStore, mockSendMessage, mockContextAcceptor } = createMocks();
      const server = new SubscriptionServer(mockApiGateway, mockSendMessage, mockSubscriptionStore, mockContextAcceptor);

      await server.processMessage('conn-1', 'not valid json');

      expect(mockApiGateway.handleError).toHaveBeenCalled();
      const errorCall = mockApiGateway.handleError.mock.calls[0][0];
      expect(errorCall.e.type).toBe('Invalid JSON payload');
    });

    it('should reject a non-object JSON payload', async () => {
      const { mockApiGateway, mockSubscriptionStore, mockSendMessage, mockContextAcceptor } = createMocks();
      const server = new SubscriptionServer(mockApiGateway, mockSendMessage, mockSubscriptionStore, mockContextAcceptor);

      for (const body of ['5', 'null', '"text"']) {
        await server.processMessage('conn-1', body);
      }

      expect(mockApiGateway.handleError).toHaveBeenCalledTimes(3);

      for (const [{ e }] of mockApiGateway.handleError.mock.calls) {
        expect(e.status).toBe(400);
        expect(e.type).toBe('Invalid message format');
      }
    });

    it('should reject message with unknown fields', async () => {
      const { mockApiGateway, mockSubscriptionStore, mockSendMessage, mockContextAcceptor } = createMocks();
      const server = new SubscriptionServer(mockApiGateway, mockSendMessage, mockSubscriptionStore, mockContextAcceptor);

      const message = {
        method: 'load',
        messageId: '123',
        params: { query: { measures: ['Orders.count'] } },
        fieldIsNotAllowed: true,
      };
      await server.processMessage('conn-1', JSON.stringify(message));

      expect(mockApiGateway.load).not.toHaveBeenCalled();
      expect(mockApiGateway.handleError).toHaveBeenCalled();
    });

    it('should reject messageId & requestId', async () => {
      const { mockApiGateway, mockSubscriptionStore, mockSendMessage, mockContextAcceptor } = createMocks();
      const server = new SubscriptionServer(mockApiGateway, mockSendMessage, mockSubscriptionStore, mockContextAcceptor);

      const message = {
        method: 'load',
        messageId: '12345678901234567', // 17 chars
        requestId: 'a'.repeat(65), // 65 chars
        params: { query: { measures: ['Orders.count'] } },
      };
      await server.processMessage('conn-1', JSON.stringify(message));

      expect(mockApiGateway.load).not.toHaveBeenCalled();
      expect(mockApiGateway.handleError).toHaveBeenCalled();
    });
  });

  describe('Request ID', () => {
    it('should reject a requestId with forbidden characters', async () => {
      const { mockApiGateway, mockSubscriptionStore, mockSendMessage, mockContextAcceptor } = createMocks();
      const server = new SubscriptionServer(mockApiGateway, mockSendMessage, mockSubscriptionStore, mockContextAcceptor);

      await server.processMessage('conn-1', JSON.stringify({
        method: 'load',
        messageId: 'msg-1',
        requestId: 'my req\n',
        params: { query: { measures: ['Orders.count'] } },
      }));

      expect(mockApiGateway.contextByReq).not.toHaveBeenCalled();
      expect(mockApiGateway.load).not.toHaveBeenCalled();
      expect(mockApiGateway.handleError).toHaveBeenCalled();
    });

    it('should reject a messageId with forbidden characters when requestId is omitted', async () => {
      const { mockApiGateway, mockSubscriptionStore, mockSendMessage, mockContextAcceptor } = createMocks();
      const server = new SubscriptionServer(mockApiGateway, mockSendMessage, mockSubscriptionStore, mockContextAcceptor);

      await server.processMessage('conn-1', JSON.stringify({
        method: 'load',
        messageId: "x' OR 1;--",
        params: { query: { measures: ['Orders.count'] } },
      }));

      expect(mockApiGateway.contextByReq).not.toHaveBeenCalled();
      expect(mockApiGateway.load).not.toHaveBeenCalled();
      expect(mockApiGateway.handleError).toHaveBeenCalled();
    });

    it('should accept a requestId of up to 128 characters', async () => {
      const { mockApiGateway, mockSubscriptionStore, mockSendMessage, mockContextAcceptor } = createMocks();
      const server = new SubscriptionServer(mockApiGateway, mockSendMessage, mockSubscriptionStore, mockContextAcceptor);

      const send = (requestId: string) => server.processMessage('conn-1', JSON.stringify({
        method: 'load',
        messageId: 'msg-1',
        requestId,
        params: { query: { measures: ['Orders.count'] } },
      }));

      await send('a'.repeat(128));
      expect(mockApiGateway.contextByReq).toHaveBeenCalledTimes(1);

      await send('a'.repeat(129));
      expect(mockApiGateway.contextByReq).toHaveBeenCalledTimes(1);
      expect(mockApiGateway.handleError).toHaveBeenCalled();
    });

    it('should pass a valid requestId through', async () => {
      const { mockApiGateway, mockSubscriptionStore, mockSendMessage, mockContextAcceptor } = createMocks();
      const server = new SubscriptionServer(mockApiGateway, mockSendMessage, mockSubscriptionStore, mockContextAcceptor);

      await server.processMessage('conn-1', JSON.stringify({
        method: 'load',
        messageId: 'msg-1',
        requestId: 'client-req-1',
        params: { query: { measures: ['Orders.count'] } },
      }));

      expect(mockApiGateway.contextByReq.mock.calls[0][2]).toMatch(/^client-req-1-span-[0-9a-f-]{36}$/);
    });
  });

  describe('Auth Flow', () => {
    it('should complete successful authorization handshake', async () => {
      const { mockApiGateway, mockSubscriptionStore, mockSendMessage, mockContextAcceptor, sentMessages } = createMocks();
      const server = new SubscriptionServer(mockApiGateway, mockSendMessage, mockSubscriptionStore, mockContextAcceptor);

      await server.processMessage('conn-1', JSON.stringify({ authorization: 'valid-token' }));

      expect(mockApiGateway.checkAuthFn).toHaveBeenCalledWith(
        expect.objectContaining({ protocol: 'ws' }),
        'valid-token'
      );
      expect(mockSubscriptionStore.setAuthContext).toHaveBeenCalled();
      expect(sentMessages).toContainEqual({ handshake: true });
    });

    it('should reject an authorization while another one is in progress', async () => {
      const { mockApiGateway, mockSendMessage, mockContextAcceptor, sentMessages } = createMocks();
      const store = new TestSubscriptionStore();
      const auth = deferred();
      mockApiGateway.checkAuthFn = jest.fn(async (authContext: any, token: string) => {
        await auth.promise;
        authContext.securityContext = { token };
      });
      const server = new SubscriptionServer(mockApiGateway, mockSendMessage, store, mockContextAcceptor);

      const first = server.processMessage('conn-1', JSON.stringify({ authorization: 'first' }));
      await server.processMessage('conn-1', JSON.stringify({ authorization: 'second' }));

      expect(mockApiGateway.checkAuthFn).toHaveBeenCalledTimes(1);
      expect(mockApiGateway.handleError).toHaveBeenCalledTimes(1);
      expect(mockApiGateway.handleError.mock.calls[0][0].e).toMatchObject({
        status: 409,
        message: 'Authorization is already in progress for this connection',
      });

      auth.resolve();
      await first;

      expect(sentMessages).toEqual([{ handshake: true }]);
      expect(await store.getAuthContext('conn-1')).toMatchObject({ securityContext: { token: 'first' } });
    });

    it('should not block an authorization on another connection', async () => {
      const { mockApiGateway, mockSendMessage, mockContextAcceptor } = createMocks();
      const store = new TestSubscriptionStore();
      const auth = deferred();
      mockApiGateway.checkAuthFn = jest.fn(async () => auth.promise);
      const server = new SubscriptionServer(mockApiGateway, mockSendMessage, store, mockContextAcceptor);

      const first = server.processMessage('conn-1', JSON.stringify({ authorization: 'token' }));
      const second = server.processMessage('conn-2', JSON.stringify({ authorization: 'token' }));

      expect(mockApiGateway.checkAuthFn).toHaveBeenCalledTimes(2);

      auth.resolve();
      await Promise.all([first, second]);

      expect(mockApiGateway.handleError).not.toHaveBeenCalled();
    });

    it('should allow a new authorization after a failed one', async () => {
      const { mockApiGateway, mockSendMessage, mockContextAcceptor, sentMessages } = createMocks();
      const store = new TestSubscriptionStore();
      mockApiGateway.checkAuthFn = jest.fn()
        .mockRejectedValueOnce(new Error('Invalid token'))
        .mockResolvedValue(undefined);
      const server = new SubscriptionServer(mockApiGateway, mockSendMessage, store, mockContextAcceptor);

      await server.processMessage('conn-1', JSON.stringify({ authorization: 'bad' }));
      await server.processMessage('conn-1', JSON.stringify({ authorization: 'good' }));

      expect(mockApiGateway.checkAuthFn).toHaveBeenCalledTimes(2);
      expect(sentMessages).toEqual([{ handshake: true }]);
    });

    it('should reject when contextAcceptor rejects', async () => {
      const { mockApiGateway, mockSubscriptionStore, mockSendMessage, mockContextAcceptor, sentMessages } = createMocks();
      mockContextAcceptor.mockResolvedValue({ accepted: false, rejectMessage: { error: 'Rejected' } });
      mockSubscriptionStore.getAuthContext.mockResolvedValue(undefined);
      const server = new SubscriptionServer(mockApiGateway, mockSendMessage, mockSubscriptionStore, mockContextAcceptor);

      await server.processMessage('conn-1', JSON.stringify({ authorization: 'token' }));

      expect(mockSubscriptionStore.setAuthContext).not.toHaveBeenCalled();
      expect(sentMessages).toContainEqual({ error: 'Rejected' });
    });

    it('should return 403 for unauthorized method call', async () => {
      const { mockApiGateway, mockSubscriptionStore, mockSendMessage, mockContextAcceptor, sentMessages } = createMocks();
      mockSubscriptionStore.getAuthContext.mockResolvedValue(null);
      const server = new SubscriptionServer(mockApiGateway, mockSendMessage, mockSubscriptionStore, mockContextAcceptor);

      const message = {
        method: 'load',
        messageId: '123',
        params: { query: {} }
      };
      await server.processMessage('conn-1', JSON.stringify(message));

      expect(mockApiGateway.load).not.toHaveBeenCalled();
      expect(sentMessages).toContainEqual({
        messageId: '123',
        message: { error: 'Not authorized' },
        status: 403
      });
    });
  });

  describe('Auth expiry', () => {
    const TTL = 300 * 1000;
    const loadMessage = (messageId: string) => JSON.stringify({ method: 'load', messageId, params: { query: {} } });

    const createServer = () => {
      const mocks = createMocks();
      const store = new TestSubscriptionStore();
      const closeConnection = jest.fn();
      // Mirrors the gateway: errors are reported through res
      mocks.mockApiGateway.handleError.mockImplementation(({ e, res }) => res({ error: e.message }, { status: e.status || 500 }));
      mocks.mockApiGateway.contextByReq.mockImplementation(async (_req, securityContext, requestId) => ({ securityContext, requestId }));
      mocks.mockApiGateway.checkAuthFn.mockImplementation(async (ctx, token) => {
        ctx.securityContext = { token };
      });

      const server = new SubscriptionServer(
        mocks.mockApiGateway,
        mocks.mockSendMessage,
        store,
        mocks.mockContextAcceptor,
        closeConnection,
      );

      return { ...mocks, store, closeConnection, server };
    };

    beforeEach(() => {
      jest.useFakeTimers();
      jest.setSystemTime(new Date('2026-01-01T00:00:00.000Z'));
    });

    afterEach(() => {
      jest.useRealTimers();
    });

    it('rejects the first query of an idle socket after the token exp', async () => {
      const { server, store, mockApiGateway, mockSendMessage, closeConnection, sentMessages } = createServer();
      const exp = Math.floor(Date.now() / 1000) + 10;
      mockApiGateway.checkAuthFn.mockImplementationOnce(async (ctx) => {
        ctx.securityContext = { exp };
      });

      await server.processMessage('conn-1', JSON.stringify({ authorization: 'token' }));
      expect(sentMessages).toEqual([{ handshake: true }]);

      jest.setSystemTime(Date.now() + 11 * 1000);
      mockApiGateway.checkAuthFn.mockRejectedValue(new CubejsHandlerError(403, 'Forbidden', 'Invalid token'));

      await server.processMessage('conn-1', loadMessage('1'));

      expect(mockApiGateway.checkAuthFn).toHaveBeenLastCalledWith(expect.objectContaining({ protocol: 'ws' }), 'token');
      expect(mockApiGateway.load).not.toHaveBeenCalled();
      expect(sentMessages[1]).toEqual({ messageId: '1', message: { error: 'Invalid token' }, status: 403 });
      expect(await store.getAuthContext('conn-1')).toBeUndefined();
      expect(closeConnection).toHaveBeenCalledWith('conn-1', 1008, 'Authorization expired');

      // The error is delivered before the socket is closed
      expect(mockSendMessage.mock.invocationCallOrder[1]).toBeLessThan(closeConnection.mock.invocationCallOrder[0]);

      await server.processMessage('conn-1', loadMessage('2'));
      expect(mockApiGateway.load).not.toHaveBeenCalled();
      expect(sentMessages[2]).toEqual({ messageId: '2', message: { error: 'Not authorized' }, status: 403 });
    });

    it('does not re-authenticate before the expiry', async () => {
      const { server, mockApiGateway, closeConnection } = createServer();

      await server.processMessage('conn-1', JSON.stringify({ authorization: 'token' }));
      jest.setSystemTime(Date.now() + TTL - 1000);
      await server.processMessage('conn-1', loadMessage('1'));

      expect(mockApiGateway.checkAuthFn).toHaveBeenCalledTimes(1);
      expect(mockApiGateway.load).toHaveBeenCalledTimes(1);
      expect(closeConnection).not.toHaveBeenCalled();
    });

    it('re-runs checkAuth after the TTL and dispatches with the refreshed context', async () => {
      const { server, mockApiGateway, closeConnection } = createServer();

      await server.processMessage('conn-1', JSON.stringify({ authorization: 'token' }));

      jest.setSystemTime(Date.now() + TTL + 1000);
      mockApiGateway.checkAuthFn.mockImplementation(async (ctx, token) => {
        ctx.securityContext = { token, refreshed: true };
      });

      await server.processMessage('conn-1', loadMessage('1'));
      await server.processMessage('conn-1', loadMessage('2'));

      expect(mockApiGateway.checkAuthFn).toHaveBeenCalledTimes(2);
      expect(mockApiGateway.load).toHaveBeenCalledTimes(2);
      expect(mockApiGateway.load.mock.calls[0][0].context.securityContext).toEqual({ token: 'token', refreshed: true });
      expect(closeConnection).not.toHaveBeenCalled();
    });

    it('ignores an exp that was already in the past when checkAuth accepted the token', async () => {
      const { server, mockApiGateway } = createServer();
      mockApiGateway.checkAuthFn.mockImplementation(async (ctx) => {
        ctx.securityContext = { exp: Math.floor(Date.now() / 1000) - 60 };
      });

      await server.processMessage('conn-1', JSON.stringify({ authorization: 'token' }));
      await server.processMessage('conn-1', loadMessage('1'));

      expect(mockApiGateway.checkAuthFn).toHaveBeenCalledTimes(1);
      expect(mockApiGateway.load).toHaveBeenCalledTimes(1);
    });

    it('revokes the connection when checkAuth fails after the TTL', async () => {
      const { server, store, mockApiGateway, closeConnection } = createServer();

      await server.processMessage('conn-1', JSON.stringify({ authorization: 'token' }));
      jest.setSystemTime(Date.now() + TTL + 1000);
      mockApiGateway.checkAuthFn.mockRejectedValue(new CubejsHandlerError(403, 'Forbidden', 'Invalid token'));

      await server.processMessage('conn-1', loadMessage('1'));

      expect(mockApiGateway.load).not.toHaveBeenCalled();
      expect(await store.getAuthContext('conn-1')).toBeUndefined();
      expect(closeConnection).toHaveBeenCalledWith('conn-1', 1008, 'Authorization expired');
    });

    it('revokes the connection when the context acceptor rejects the refreshed context', async () => {
      const { server, store, mockApiGateway, mockContextAcceptor, closeConnection, sentMessages } = createServer();

      await server.processMessage('conn-1', JSON.stringify({ authorization: 'token' }));
      jest.setSystemTime(Date.now() + TTL + 1000);
      mockContextAcceptor.mockResolvedValue({ accepted: false, rejectMessage: { error: 'Rejected' } });

      await server.processMessage('conn-1', loadMessage('1'));

      expect(mockApiGateway.load).not.toHaveBeenCalled();
      expect(sentMessages[1]).toEqual({ messageId: '1', message: { error: 'Not authorized' }, status: 403 });
      expect(await store.getAuthContext('conn-1')).toBeUndefined();
      expect(closeConnection).toHaveBeenCalledWith('conn-1', 1008, 'Authorization expired');
    });

    it('stops refreshing subscriptions after the expiry', async () => {
      const { server, store, mockApiGateway, closeConnection } = createServer();
      mockApiGateway.subscribe.mockImplementation(async ({ subscribe }) => subscribe({ resultHash: 'hash' }));

      await server.processMessage('conn-1', JSON.stringify({ authorization: 'token' }));
      await server.processMessage('conn-1', JSON.stringify({ method: 'subscribe', messageId: '1', params: { query: {} } }));
      await server.processMessage('conn-1', JSON.stringify({ method: 'subscribe', messageId: '2', params: { query: {} } }));
      expect(store.getAllSubscriptions()).toHaveLength(2);

      await server.processSubscriptions();
      expect(mockApiGateway.subscribe).toHaveBeenCalledTimes(4);

      // Below the stale-subscription threshold, so only the auth expiry can stop the refresh
      jest.setSystemTime(Date.now() + 200 * 1000);
      await server.processMessage('conn-1', JSON.stringify({ method: 'subscribe', messageId: '1', params: { query: {} } }));
      await server.processMessage('conn-1', JSON.stringify({ method: 'subscribe', messageId: '2', params: { query: {} } }));
      jest.setSystemTime(Date.now() + 101 * 1000);
      mockApiGateway.subscribe.mockClear();
      mockApiGateway.checkAuthFn.mockClear();
      mockApiGateway.checkAuthFn.mockRejectedValue(new CubejsHandlerError(403, 'Forbidden', 'Invalid token'));

      await server.processSubscriptions();

      expect(mockApiGateway.checkAuthFn).toHaveBeenCalledTimes(1);
      expect(mockApiGateway.subscribe).not.toHaveBeenCalled();
      expect(store.getAllSubscriptions()).toEqual([]);
      expect(closeConnection).toHaveBeenCalledWith('conn-1', 1008, 'Authorization expired');
    });

    it('shares one re-authentication between concurrent messages', async () => {
      const { server, mockApiGateway } = createServer();

      await server.processMessage('conn-1', JSON.stringify({ authorization: 'token' }));
      jest.setSystemTime(Date.now() + TTL + 1000);

      await Promise.all([
        server.processMessage('conn-1', loadMessage('1')),
        server.processMessage('conn-1', loadMessage('2')),
        server.processMessage('conn-1', loadMessage('3')),
      ]);

      expect(mockApiGateway.checkAuthFn).toHaveBeenCalledTimes(2);
      expect(mockApiGateway.load).toHaveBeenCalledTimes(3);
    });

    it('revokes the previous context when an explicit re-authentication fails', async () => {
      const { server, store, mockApiGateway, closeConnection, sentMessages } = createServer();

      await server.processMessage('conn-1', JSON.stringify({ authorization: 'token' }));
      mockApiGateway.checkAuthFn.mockRejectedValue(new CubejsHandlerError(403, 'Forbidden', 'Invalid token'));

      await server.processMessage('conn-1', JSON.stringify({ authorization: 'expired-token' }));

      expect(sentMessages[1]).toEqual({ messageId: undefined, message: { error: 'Invalid token' }, status: 403 });
      expect(await store.getAuthContext('conn-1')).toBeUndefined();
      expect(closeConnection).toHaveBeenCalledWith('conn-1', 1008, 'Authorization failed');

      await server.processMessage('conn-1', loadMessage('1'));
      expect(mockApiGateway.load).not.toHaveBeenCalled();
      expect(sentMessages[2]).toEqual({ messageId: '1', message: { error: 'Not authorized' }, status: 403 });
    });

    it('revokes the previous context when the acceptor rejects an explicit re-authentication', async () => {
      const { server, store, mockContextAcceptor, closeConnection, sentMessages } = createServer();

      await server.processMessage('conn-1', JSON.stringify({ authorization: 'token' }));
      mockContextAcceptor.mockResolvedValue({ accepted: false, rejectMessage: { error: 'Rejected' } });

      await server.processMessage('conn-1', JSON.stringify({ authorization: 'other-token' }));

      expect(sentMessages[1]).toEqual({ error: 'Rejected' });
      expect(await store.getAuthContext('conn-1')).toBeUndefined();
      expect(closeConnection).toHaveBeenCalledWith('conn-1', 1008, 'Authorization failed');
    });

    it('keeps a fresh socket open when its first handshake fails', async () => {
      const { server, mockApiGateway, closeConnection, sentMessages } = createServer();
      mockApiGateway.checkAuthFn.mockRejectedValue(new CubejsHandlerError(403, 'Forbidden', 'Invalid token'));

      await server.processMessage('conn-1', JSON.stringify({ authorization: 'expired-token' }));

      expect(sentMessages).toEqual([{ messageId: undefined, message: { error: 'Invalid token' }, status: 403 }]);
      expect(closeConnection).not.toHaveBeenCalled();
    });

    it('replaces the context on a successful re-authentication', async () => {
      const { server, mockApiGateway, closeConnection } = createServer();

      await server.processMessage('conn-1', JSON.stringify({ authorization: 'token' }));
      await server.processMessage('conn-1', JSON.stringify({ authorization: 'metadata-only-token' }));
      await server.processMessage('conn-1', loadMessage('1'));

      expect(mockApiGateway.load.mock.calls[0][0].context.securityContext).toEqual({ token: 'metadata-only-token' });
      expect(closeConnection).not.toHaveBeenCalled();
    });
  });

  describe('Method Dispatch', () => {
    it('should call load method correctly', async () => {
      const { mockApiGateway, mockSubscriptionStore, mockSendMessage, mockContextAcceptor } = createMocks();
      const server = new SubscriptionServer(mockApiGateway, mockSendMessage, mockSubscriptionStore, mockContextAcceptor);

      const message = {
        method: 'load',
        messageId: '123',
        params: { query: { measures: ['Orders.count'] }, queryType: 'multi' }
      };
      await server.processMessage('conn-1', JSON.stringify(message));

      expect(mockApiGateway.load).toHaveBeenCalledWith(
        expect.objectContaining({
          query: { measures: ['Orders.count'] },
          queryType: 'multi',
          connectionId: 'conn-1',
          apiType: 'ws',
        })
      );
    });

    it('should call sql method correctly', async () => {
      const { mockApiGateway, mockSubscriptionStore, mockSendMessage, mockContextAcceptor } = createMocks();
      const server = new SubscriptionServer(mockApiGateway, mockSendMessage, mockSubscriptionStore, mockContextAcceptor);

      const message = {
        method: 'sql',
        messageId: '123',
        params: { query: { measures: ['Orders.count'] } }
      };
      await server.processMessage('conn-1', JSON.stringify(message));

      expect(mockApiGateway.sql).toHaveBeenCalledWith(
        expect.objectContaining({
          query: { measures: ['Orders.count'] },
          connectionId: 'conn-1',
        })
      );
    });

    it('should call meta method correctly', async () => {
      const { mockApiGateway, mockSubscriptionStore, mockSendMessage, mockContextAcceptor } = createMocks();
      const server = new SubscriptionServer(mockApiGateway, mockSendMessage, mockSubscriptionStore, mockContextAcceptor);

      const message = {
        method: 'meta',
        messageId: '123',
      };
      await server.processMessage('conn-1', JSON.stringify(message));

      expect(mockApiGateway.meta).toHaveBeenCalledWith(
        expect.objectContaining({
          connectionId: 'conn-1',
          apiType: 'ws',
        })
      );
    });

    it('should forward onlyViews param for meta', async () => {
      const { mockApiGateway, mockSubscriptionStore, mockSendMessage, mockContextAcceptor } = createMocks();
      const server = new SubscriptionServer(mockApiGateway, mockSendMessage, mockSubscriptionStore, mockContextAcceptor);

      const message = {
        method: 'meta',
        messageId: '123',
        params: { onlyViews: true }
      };
      await server.processMessage('conn-1', JSON.stringify(message));

      expect(mockApiGateway.meta).toHaveBeenCalledWith(
        expect.objectContaining({
          onlyViews: true,
          connectionId: 'conn-1',
          apiType: 'ws',
        })
      );
    });

    it('should reject a non-boolean onlyViews param for meta', async () => {
      const { mockApiGateway, mockSubscriptionStore, mockSendMessage, mockContextAcceptor } = createMocks();
      const server = new SubscriptionServer(mockApiGateway, mockSendMessage, mockSubscriptionStore, mockContextAcceptor);

      const message = {
        method: 'meta',
        messageId: '123',
        params: { onlyViews: 'true' }
      };
      await server.processMessage('conn-1', JSON.stringify(message));

      expect(mockApiGateway.meta).not.toHaveBeenCalled();
      expect(mockApiGateway.handleError).toHaveBeenCalled();
    });

    it('should still reject unknown meta params', async () => {
      const { mockApiGateway, mockSubscriptionStore, mockSendMessage, mockContextAcceptor } = createMocks();
      const server = new SubscriptionServer(mockApiGateway, mockSendMessage, mockSubscriptionStore, mockContextAcceptor);

      const message = {
        method: 'meta',
        messageId: '123',
        params: { somethingElse: true }
      };
      await server.processMessage('conn-1', JSON.stringify(message));

      expect(mockApiGateway.meta).not.toHaveBeenCalled();
      expect(mockApiGateway.handleError).toHaveBeenCalled();
    });

    it('should forward cache param as cacheMode for load', async () => {
      const { mockApiGateway, mockSubscriptionStore, mockSendMessage, mockContextAcceptor } = createMocks();
      const server = new SubscriptionServer(mockApiGateway, mockSendMessage, mockSubscriptionStore, mockContextAcceptor);

      const message = {
        method: 'load',
        messageId: '123',
        params: { query: { measures: ['Orders.count'] }, cache: 'no-cache' }
      };
      await server.processMessage('conn-1', JSON.stringify(message));

      expect(mockApiGateway.load).toHaveBeenCalledWith(
        expect.objectContaining({
          query: { measures: ['Orders.count'] },
          cacheMode: 'no-cache',
          connectionId: 'conn-1',
          apiType: 'ws',
        })
      );
      // cache should be remapped, not passed through as-is
      expect(mockApiGateway.load).not.toHaveBeenCalledWith(
        expect.objectContaining({ cache: 'no-cache' })
      );
    });

    it('should forward cache param as cacheMode for subscribe', async () => {
      const { mockApiGateway, mockSubscriptionStore, mockSendMessage, mockContextAcceptor } = createMocks();
      const server = new SubscriptionServer(mockApiGateway, mockSendMessage, mockSubscriptionStore, mockContextAcceptor);

      const message = {
        method: 'subscribe',
        messageId: '123',
        params: { query: { measures: ['Orders.count'] }, cache: 'no-cache' }
      };
      await server.processMessage('conn-1', JSON.stringify(message));

      expect(mockApiGateway.subscribe).toHaveBeenCalledWith(
        expect.objectContaining({
          query: { measures: ['Orders.count'] },
          cacheMode: 'no-cache',
          connectionId: 'conn-1',
        })
      );
    });

    it('should call subscribe method correctly', async () => {
      const { mockApiGateway, mockSubscriptionStore, mockSendMessage, mockContextAcceptor } = createMocks();
      const server = new SubscriptionServer(mockApiGateway, mockSendMessage, mockSubscriptionStore, mockContextAcceptor);

      const message = {
        method: 'subscribe',
        messageId: '123',
        params: { query: { measures: ['Orders.count'] } }
      };
      await server.processMessage('conn-1', JSON.stringify(message));

      expect(mockApiGateway.subscribe).toHaveBeenCalledWith(
        expect.objectContaining({
          query: { measures: ['Orders.count'] },
          connectionId: 'conn-1',
        })
      );
    });
  });

  describe('Unsubscribe method', () => {
    const createServer = () => {
      const { mockApiGateway, mockSendMessage, mockContextAcceptor, sentMessages } = createMocks();
      mockApiGateway.unsubscribe = ApiGateway.prototype.unsubscribe;
      const store = new LocalSubscriptionStore();
      const server = new SubscriptionServer(mockApiGateway, mockSendMessage, store, mockContextAcceptor);

      return { server, store, mockApiGateway, sentMessages };
    };

    it('should drop the subscription with the same messageId', async () => {
      const { server, store, mockApiGateway, sentMessages } = createServer();

      await server.processMessage('conn-1', JSON.stringify({ authorization: 'token' }));
      await store.subscribe('conn-1', 'sub-1', { message: {}, state: {} });
      await store.subscribe('conn-1', 'sub-2', { message: {}, state: {} });

      await server.processMessage('conn-1', JSON.stringify({ method: 'unsubscribe', messageId: 'sub-1' }));

      expect(await store.getSubscription('conn-1', 'sub-1')).toBeUndefined();
      expect(await store.getSubscription('conn-1', 'sub-2')).toBeDefined();
      expect(sentMessages).toContainEqual({ messageProcessedId: 'sub-1' });
      expect(mockApiGateway.handleError).not.toHaveBeenCalled();
    });

    it('should require authorization', async () => {
      const { server, store, sentMessages } = createServer();

      store.connect('conn-1');
      await store.subscribe('conn-1', 'sub-1', { message: {}, state: {} });

      await server.processMessage('conn-1', JSON.stringify({ method: 'unsubscribe', messageId: 'sub-1' }));

      expect(await store.getSubscription('conn-1', 'sub-1')).toBeDefined();
      expect(sentMessages).toContainEqual({
        messageId: 'sub-1',
        message: { error: 'Not authorized' },
        status: 403,
      });
    });
  });

  describe('Error replies', () => {
    const deepArray = (depth: number) => `${'['.repeat(depth)}0${']'.repeat(depth)}`;

    it('should not echo an invalid nested messageId', async () => {
      const { mockApiGateway, mockSubscriptionStore, mockSendMessage, mockContextAcceptor, sentMessages } = createMocks();
      const server = new SubscriptionServer(mockApiGateway, mockSendMessage, mockSubscriptionStore, mockContextAcceptor);

      await server.processMessage('conn-1', `{"method":"meta","messageId":${deepArray(12000)}}`);

      expect(mockApiGateway.handleError).toHaveBeenCalledTimes(1);
      const { res } = mockApiGateway.handleError.mock.calls[0][0];
      await res({ error: 'Invalid message format' }, { status: 400 });

      expect(sentMessages).toEqual([{ messageId: undefined, message: { error: 'Invalid message format' }, status: 400 }]);
    });

    it('should echo a valid messageId of an otherwise invalid message', async () => {
      const { mockApiGateway, mockSubscriptionStore, mockSendMessage, mockContextAcceptor, sentMessages } = createMocks();
      const server = new SubscriptionServer(mockApiGateway, mockSendMessage, mockSubscriptionStore, mockContextAcceptor);

      await server.processMessage('conn-1', JSON.stringify({ method: 'meta', messageId: 7, unknown: true }));

      const { res } = mockApiGateway.handleError.mock.calls[0][0];
      await res({ error: 'Invalid message format' }, { status: 400 });

      expect(sentMessages).toEqual([{ messageId: '7', message: { error: 'Invalid message format' }, status: 400 }]);
    });

    it('should not pass an unvalidated top-level query to error handling', async () => {
      const { mockApiGateway, mockSubscriptionStore, mockSendMessage, mockContextAcceptor } = createMocks();
      const server = new SubscriptionServer(mockApiGateway, mockSendMessage, mockSubscriptionStore, mockContextAcceptor);

      await server.processMessage('conn-1', `{"method":"meta","messageId":"1","query":${deepArray(12000)}}`);

      expect(mockApiGateway.handleError.mock.calls[0][0].query).toBeUndefined();
    });

    it('should contain a failed error reply instead of rejecting', async () => {
      const { mockApiGateway, mockSubscriptionStore, mockContextAcceptor } = createMocks();
      const failingSend = jest.fn().mockRejectedValue(new RangeError('Maximum call stack size exceeded'));
      const server = new SubscriptionServer(mockApiGateway, failingSend, mockSubscriptionStore, mockContextAcceptor);

      await server.processMessage('conn-1', 'not valid json');

      const { res } = mockApiGateway.handleError.mock.calls[0][0];
      await expect(res({ error: 'Invalid JSON payload' }, { status: 400 })).resolves.toBeUndefined();
      expect(mockApiGateway.log).toHaveBeenCalledWith(
        expect.objectContaining({ type: 'WebSocket Send Error' }),
        expect.anything()
      );
    });

    it('should contain a failed handshake reply', async () => {
      const { mockApiGateway, mockSubscriptionStore, mockContextAcceptor } = createMocks();
      const sendError = new Error('Socket for conn-1 is not found');
      const failingSend = jest.fn().mockRejectedValue(sendError);
      const server = new SubscriptionServer(mockApiGateway, failingSend, mockSubscriptionStore, mockContextAcceptor);

      await server.processMessage('conn-1', JSON.stringify({ authorization: 'token' }));

      expect(failingSend).toHaveBeenCalledTimes(1);
      expect(failingSend).toHaveBeenCalledWith('conn-1', { handshake: true });
      expect(mockApiGateway.handleError).toHaveBeenCalledTimes(1);
      const { e, res } = mockApiGateway.handleError.mock.calls[0][0];
      expect(e).toBe(sendError);

      await expect(res({ error: 'Internal Server Error' }, { status: 500 })).resolves.toBeUndefined();
      expect(failingSend).toHaveBeenLastCalledWith(
        'conn-1',
        { messageId: undefined, message: { error: 'Internal Server Error' }, status: 500 }
      );
      expect(mockApiGateway.log).toHaveBeenCalledWith(
        expect.objectContaining({ type: 'WebSocket Send Error', error: sendError.stack }),
        expect.anything()
      );
    });

    it('should contain a failed reply from a gateway method', async () => {
      const { mockApiGateway, mockSubscriptionStore, mockContextAcceptor } = createMocks();
      const failingSend = jest.fn().mockRejectedValue(new Error('Socket is gone'));
      const replies: Promise<string>[] = [];
      const settle = (reply: Promise<unknown>) => reply.then(() => 'resolved', () => 'rejected');
      // Mirrors the gateway: await `res`, then pass the same `res` to error handling without awaiting it
      mockApiGateway.load = jest.fn(async ({ res }) => {
        try {
          const reply = res({ data: [] });
          replies.push(settle(reply));
          await reply;
        } catch (e: any) {
          replies.push(settle(res({ error: e.message }, { status: 500 })));
        }
      });
      const server = new SubscriptionServer(mockApiGateway, failingSend, mockSubscriptionStore, mockContextAcceptor);

      await server.processMessage('conn-1', JSON.stringify({
        method: 'load',
        messageId: '1',
        params: { query: { measures: ['Orders.count'] } },
      }));

      expect(mockApiGateway.load).toHaveBeenCalledTimes(1);
      // A rejecting `res` would take the catch branch and leave a second, un-awaited rejection
      expect(await Promise.all(replies)).toEqual(['resolved']);
      expect(mockApiGateway.log).toHaveBeenCalledWith(
        expect.objectContaining({ type: 'WebSocket Send Error' }),
        expect.anything()
      );
    });
  });
});
