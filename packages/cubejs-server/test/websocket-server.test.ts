import http from 'http';
import { AddressInfo } from 'net';
import WebSocket from 'ws';

import type { WebSocketSendMessageFn } from '@cubejs-backend/api-gateway';

import { WebSocketServer } from '../src/websocket-server';

const deepArray = (depth: number) => {
  let value: unknown = 0;

  for (let i = 0; i < depth; i++) {
    value = [value];
  }
  return value;
};

describe('WebSocketServer', () => {
  let httpServer: http.Server;
  let wsServer: WebSocketServer;
  let logger: jest.Mock;
  let processMessage: jest.Mock;

  beforeEach(async () => {
    logger = jest.fn();
    processMessage = jest.fn();

    const serverCore: any = {
      logger,
      initSubscriptionServer: (sendMessage: WebSocketSendMessageFn) => ({
        processMessage: (connectionId: string, body: string) => processMessage(sendMessage, connectionId, body),
        processSubscriptions: async () => undefined,
        disconnect: async () => undefined,
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
    processMessage.mockImplementation(async (sendMessage: WebSocketSendMessageFn, connectionId: string) => {
      try {
        await sendMessage(connectionId, { messageId: deepArray(12000), message: { error: 'Invalid message format' }, status: 400 });
      } catch (e) {
        sendError = e;
      }
    });

    const { code } = await exchange('{}');

    expect(code).toBe(1011);
    expect(sendError).toBeInstanceOf(RangeError);
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
});
