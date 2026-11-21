/// <reference types="node" />
// @vitest-environment node
import { createServer, type Server } from 'node:http';
import { once } from 'node:events';
import { afterAll, beforeAll, describe, expect, test } from 'vitest';
import HttpTransport from '../src/HttpTransport.js';

let server: Server;
let apiUrl: string;

beforeAll(async () => {
  server = createServer();
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const address = server.address();
  if (!address || typeof address === 'string') {
    throw new Error('Expected a TCP server address');
  }
  apiUrl = `http://127.0.0.1:${address.port}`;
});

afterAll(async () => {
  server.closeAllConnections();
  await new Promise<void>((resolve, reject) => {
    server.close(error => (error ? reject(error) : resolve()));
  });
});

describe('native fetch cancellation', () => {
  test.each(['constructor', 'request'] as const)('reports a %s fetchTimeout as timeout', async (source) => {
    const transport = new HttpTransport({
      apiUrl,
      ...(source === 'constructor' ? { fetchTimeout: 20 } : {}),
    });
    const result = await transport.request('load', {
      ...(source === 'request' ? { fetchTimeout: 20 } : {}),
    }).subscribe(response => response);
    expect(result).toEqual({ error: 'timeout' });
  });

  test('preserves explicit cancellation as aborted', async () => {
    const controller = new AbortController();
    controller.abort();
    const transport = new HttpTransport({ apiUrl, signal: controller.signal });
    expect(await transport.request('load', {}).subscribe(response => response)).toEqual({ error: 'aborted' });
  });
});
