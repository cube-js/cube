import { SubscriptionServer } from '../../src/ws/subscription-server';
import { LocalSubscriptionStore } from '../../src/ws/local-subscription-store';

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
    connect: jest.fn().mockReturnValue({ isClosed: () => false }),
    setAuthContext: jest.fn().mockResolvedValue(undefined),
    getAuthContext: jest.fn().mockResolvedValue({ securityContext: {} }),
    subscribe: jest.fn().mockResolvedValue(undefined),
    unsubscribe: jest.fn().mockResolvedValue(undefined),
    getSubscription: jest.fn().mockResolvedValue(null),
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
    const deepQuery = (depth: number) => {
      let value: unknown = 0;

      for (let i = 0; i < depth; i++) {
        value = [value];
      }
      return value;
    };

    const subscriptionMessage = (messageId: string, query: unknown) => ({
      method: 'subscribe',
      messageId,
      params: { query },
    });

    const createServer = () => {
      const { mockApiGateway, mockSubscriptionStore, mockSendMessage, mockContextAcceptor } = createMocks();
      mockSubscriptionStore.getAllSubscriptions = jest.fn().mockReturnValue([
        { connectionId: 'conn-1', message: subscriptionMessage('1', deepQuery(12000)) },
        { connectionId: 'conn-2', message: subscriptionMessage('2', { measures: ['Orders.count'] }) },
      ]);
      const server = new SubscriptionServer(mockApiGateway, mockSendMessage, mockSubscriptionStore, mockContextAcceptor);

      return { server, mockApiGateway };
    };

    it('should route a message that cannot be measured to error handling', async () => {
      const { server, mockApiGateway } = createServer();

      await expect(server.processSubscriptions()).resolves.toBeUndefined();

      expect(mockApiGateway.handleError).toHaveBeenCalledTimes(1);
      expect(mockApiGateway.handleError.mock.calls[0][0].e).toBeInstanceOf(RangeError);
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

    it('should reject when contextAcceptor rejects', async () => {
      const { mockApiGateway, mockSubscriptionStore, mockSendMessage, mockContextAcceptor, sentMessages } = createMocks();
      mockContextAcceptor.mockResolvedValue({ accepted: false, rejectMessage: { error: 'Rejected' } });
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
