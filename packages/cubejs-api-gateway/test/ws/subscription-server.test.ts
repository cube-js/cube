import { SubscriptionServer } from '../../src/ws/subscription-server';

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

    it('should accept unsubscribe with numeric messageId', async () => {
      const { mockApiGateway, mockSubscriptionStore, mockSendMessage, mockContextAcceptor } = createMocks();
      const server = new SubscriptionServer(mockApiGateway, mockSendMessage, mockSubscriptionStore, mockContextAcceptor);

      await server.processMessage('conn-1', JSON.stringify({ unsubscribe: 123 }));

      expect(mockSubscriptionStore.unsubscribe).toHaveBeenCalledWith('conn-1', 123);
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

    it('should accept messageId as number', async () => {
      const { mockApiGateway, mockSubscriptionStore, mockSendMessage, mockContextAcceptor, sentMessages } = createMocks();
      const server = new SubscriptionServer(mockApiGateway, mockSendMessage, mockSubscriptionStore, mockContextAcceptor);

      const message = {
        method: 'load',
        messageId: 123,
        params: { query: { measures: ['Orders.count'] } }
      };
      await server.processMessage('conn-1', JSON.stringify(message));

      expect(mockApiGateway.load).toHaveBeenCalled();
      expect(sentMessages).toContainEqual({ messageProcessedId: 123 });
    });

    it('should reject invalid JSON payload', async () => {
      const { mockApiGateway, mockSubscriptionStore, mockSendMessage, mockContextAcceptor } = createMocks();
      const server = new SubscriptionServer(mockApiGateway, mockSendMessage, mockSubscriptionStore, mockContextAcceptor);

      await server.processMessage('conn-1', 'not valid json');

      expect(mockApiGateway.handleError).toHaveBeenCalled();
      const errorCall = mockApiGateway.handleError.mock.calls[0][0];
      expect(errorCall.e.type).toBe('Invalid JSON payload');
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
});
