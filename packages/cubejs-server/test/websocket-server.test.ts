import http from 'http';
import { AddressInfo } from 'net';
import WebSocket from 'ws';

import { LocalSubscriptionStore, SubscriptionServer } from '@cubejs-backend/api-gateway';
import type { WebSocketSendMessageFn } from '@cubejs-backend/api-gateway';

import { WebSocketServer, WS_CLOSE_AUTH_TIMEOUT } from '../src/websocket-server';

const deferred = () => {
  let resolve!: () => void;
  const promise = new Promise<void>((r) => {
    resolve = r;
  });

  return { promise, resolve };
};

class TestSubscriptionStore extends LocalSubscriptionStore {
  public connectionIds(): string[] {
    return [...this.connections.keys()];
  }
}

describe('WebSocketServer', () => {
  let httpServer: http.Server;
  let wsServer: WebSocketServer;
  let logger: jest.Mock;
  let processMessage: jest.Mock;
  let disconnect: jest.Mock;

  beforeEach(async () => {
    logger = jest.fn();
    processMessage = jest.fn();
    disconnect = jest.fn().mockResolvedValue(undefined);

    const serverCore: any = {
      logger,
      initSubscriptionServer: (sendMessage: WebSocketSendMessageFn) => ({
        processMessage: (connectionId: string, body: string) => processMessage(sendMessage, connectionId, body),
        processSubscriptions: async () => undefined,
        isAuthorized: async () => false,
        disconnect,
        clear: () => undefined,
      }),
    };

    httpServer = http.createServer();
    wsServer = new WebSocketServer(serverCore, { processSubscriptionsInterval: 60 * 1000 });
    wsServer.initServer(httpServer);
    await new Promise<void>((resolve) => httpServer.listen(0, '127.0.0.1', resolve));
  });

  afterEach(async () => {
    await wsServer.close();
    await new Promise((resolve) => httpServer.close(resolve));
  });

  const exchange = (body: string) => new Promise<{ code: number, messages: string[] }>((resolve, reject) => {
    const { port } = httpServer.address() as AddressInfo;
    const ws = new WebSocket(`ws://127.0.0.1:${port}/`);
    const messages: string[] = [];

    ws.on('open', () => ws.send(body));
    ws.on('message', (data) => {
      messages.push(data.toString());
      ws.close();
    });
    ws.on('close', (code) => resolve({ code, messages }));
    ws.on('error', reject);
  });

  test('closes only the affected connection when a reply cannot be serialized', async () => {
    let sendError: unknown;
    // JSON.stringify throws on a BigInt on every Node.js version, unlike on deep nesting
    processMessage.mockImplementation(async (sendMessage: WebSocketSendMessageFn, connectionId: string) => {
      try {
        await sendMessage(connectionId, { messageId: BigInt(1), message: { error: 'Invalid message format' }, status: 400 });
      } catch (e) {
        sendError = e;
      }
    });

    const { code } = await exchange('{}');

    expect(code).toBe(1011);
    expect(sendError).toBeInstanceOf(TypeError);
    expect(logger).toHaveBeenCalledWith('WebSocket Send Error', expect.objectContaining({ error: expect.any(String) }));

    processMessage.mockImplementation(async (sendMessage: WebSocketSendMessageFn, connectionId: string) => {
      await sendMessage(connectionId, { handshake: true });
    });

    await expect(exchange('{}')).resolves.toEqual({ code: 1005, messages: ['{"handshake":true}'] });
  });

  test('contains an exception thrown while processing a message', async () => {
    processMessage.mockRejectedValue(new Error('boom'));

    const { code } = await exchange('{}');

    expect(code).toBe(1011);
    expect(logger).toHaveBeenCalledWith('WebSocket Message Error', expect.objectContaining({ error: expect.stringContaining('boom') }));
  });

  test('drops a reply to a client that has already disconnected', async () => {
    let send: (() => Promise<void>) | undefined;
    processMessage.mockImplementation(async (sendMessage: WebSocketSendMessageFn, connectionId: string) => {
      send = () => sendMessage(connectionId, { messageId: '1', message: {}, status: 200 });
      await send();
    });

    const disconnected = new Promise((resolve) => disconnect.mockImplementation(async () => resolve(undefined)));
    await exchange('{}');
    await disconnected;

    await expect(send!()).resolves.toBeUndefined();
    expect(logger).not.toHaveBeenCalled();
  });
});

describe('WebSocketServer with SubscriptionServer', () => {
  let httpServer: http.Server;
  let wsServer: WebSocketServer;
  let store: TestSubscriptionStore;
  let gateway: any;
  let disconnected: Promise<void>;
  let accepted: boolean;

  beforeEach(async () => {
    accepted = true;
    store = new TestSubscriptionStore();
    gateway = {
      checkAuthFn: jest.fn().mockResolvedValue(undefined),
      contextByReq: jest.fn().mockResolvedValue({ requestId: 'test-req' }),
      log: jest.fn(),
      handleError: jest.fn(),
    };

    let subscriptionServer: SubscriptionServer;
    const serverCore: any = {
      logger: jest.fn(),
      initSubscriptionServer: (sendMessage: WebSocketSendMessageFn) => {
        subscriptionServer = new SubscriptionServer(
          gateway,
          sendMessage,
          store,
          async () => (accepted ? { accepted: true } : { accepted: false, rejectMessage: { error: 'Rejected' } }),
        );
        disconnected = new Promise((resolve) => {
          const disconnect = subscriptionServer.disconnect.bind(subscriptionServer);
          subscriptionServer.disconnect = async (connectionId: string) => {
            await disconnect(connectionId);
            resolve();
          };
        });

        return subscriptionServer;
      },
    };

    httpServer = http.createServer();
    wsServer = new WebSocketServer(serverCore, { processSubscriptionsInterval: 60 * 1000, webSocketsAuthTimeout: 200 });
    wsServer.initServer(httpServer);
    await new Promise<void>((resolve) => httpServer.listen(0, '127.0.0.1', resolve));
  });

  afterEach(async () => {
    await wsServer.close();
    await new Promise((resolve) => httpServer.close(resolve));
  });

  const connect = (messages: unknown[]) => new Promise<{ code: number, received: any[], ws: WebSocket }>((resolve, reject) => {
    const { port } = httpServer.address() as AddressInfo;
    const ws = new WebSocket(`ws://127.0.0.1:${port}/`);
    const received: any[] = [];

    ws.on('open', () => messages.forEach((m) => ws.send(JSON.stringify(m))));
    ws.on('message', (data) => received.push(JSON.parse(data.toString())));
    ws.on('close', (code) => resolve({ code, received, ws }));
    ws.on('error', reject);
  });

  test('closes a connection that never authorizes', async () => {
    const { code, received } = await connect([
      { method: 'load', messageId: '1', params: { query: { measures: ['Orders.count'] } } },
    ]);

    expect(code).toBe(WS_CLOSE_AUTH_TIMEOUT);
    expect(received).toEqual([{ messageId: '1', message: { error: 'Not authorized' }, status: 403 }]);
    await disconnected;
    expect(store.connectionIds()).toEqual([]);
  });

  test('closes a connection whose authorization was rejected', async () => {
    accepted = false;

    const { code, received } = await connect([{ authorization: 'token' }]);

    expect(code).toBe(WS_CLOSE_AUTH_TIMEOUT);
    expect(received).toEqual([{ error: 'Rejected' }]);
  });

  test('closes a connection whose authorization failed', async () => {
    gateway.checkAuthFn.mockRejectedValue(new Error('Invalid token'));

    const { code } = await connect([{ authorization: 'token' }]);

    expect(code).toBe(WS_CLOSE_AUTH_TIMEOUT);
    expect(gateway.handleError).toHaveBeenCalled();
  });

  test('keeps an authorized connection open past the authorization timeout', async () => {
    const { port } = httpServer.address() as AddressInfo;
    const ws = new WebSocket(`ws://127.0.0.1:${port}/`);
    const closed = jest.fn();
    ws.on('close', closed);

    const handshake = new Promise((resolve) => ws.on('message', resolve));
    ws.on('open', () => ws.send(JSON.stringify({ authorization: 'token' })));
    expect(JSON.parse((await handshake as Buffer).toString())).toEqual({ handshake: true });

    await new Promise((resolve) => setTimeout(resolve, 400));
    expect(closed).not.toHaveBeenCalled();
    expect(ws.readyState).toBe(WebSocket.OPEN);

    ws.close();
    await disconnected;
  });

  test('does not bring the connection back when a subscribe finishes after the disconnect', async () => {
    const load = deferred();
    const started = deferred();
    const finished = deferred();
    // Mirrors the gateway: load, reply, then record the subscription
    gateway.subscribe = jest.fn(async ({ res, subscribe }) => {
      started.resolve();
      await load.promise;
      await res({ data: [] });
      await subscribe({ resultHash: 'hash' });
      finished.resolve();
    });

    const { port } = httpServer.address() as AddressInfo;
    const ws = new WebSocket(`ws://127.0.0.1:${port}/`);
    const received: any[] = [];
    ws.on('open', () => ws.send(JSON.stringify({ authorization: 'token' })));
    ws.on('message', (data) => {
      const message = JSON.parse(data.toString());
      received.push(message);

      if (message.handshake) {
        ws.send(JSON.stringify({ method: 'subscribe', messageId: '1', params: { query: { measures: ['Orders.count'] } } }));
      }
    });

    await started.promise;
    expect(store.connectionIds()).toHaveLength(1);

    ws.close();
    await disconnected;
    expect(store.connectionIds()).toEqual([]);

    load.resolve();
    await finished.promise;

    expect(store.connectionIds()).toEqual([]);
    expect(store.getAllSubscriptions()).toEqual([]);
    expect(received).toEqual([{ handshake: true }]);
    expect(gateway.handleError).not.toHaveBeenCalled();
  });
});
