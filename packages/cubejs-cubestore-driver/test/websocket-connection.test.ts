import { Socket } from 'net';

import { WebSocketConnection } from '../src/WebSocketConnection';
import { MessageTooLargeError, QueryError } from '../src/errors';
import { QueryResultFormat } from '../codegen';
import { answeredBy, buildErrorMessage, MockCubeStoreServer } from './mock-cubestore-server';

const JEST_TIMEOUT = 60 * 1000;

/**
 * The error Node hands to a pending write when the peer is gone, as seen in
 * `ConnectionError: CubeStore connection error: write EPIPE`.
 */
const epipe = () => Object.assign(new Error('write EPIPE'), {
  code: 'EPIPE',
  errno: -32,
  syscall: 'write',
});

const nextTurn = () => new Promise((resolve) => { setImmediate(resolve); });

describe('WebSocketConnection', () => {
  let server: MockCubeStoreServer;
  let connection: WebSocketConnection | null = null;

  beforeEach(async () => {
    server = await MockCubeStoreServer.start();
    connection = null;
  });

  afterEach(async () => {
    connection?.close();
    await server.stop();
  });

  const query = (sql: string) => connection!.query(sql, [], { responseFormat: QueryResultFormat.Legacy });

  /**
   * The mock answers every query with an error naming the connection that
   * served it, so a rejection carrying that marker means the query made a full
   * round trip. A rejection with anything else (`ConnectionError: ... write
   * EPIPE`) means the driver dropped the query instead of delivering it.
   */
  const expectAnsweredBy = async (promise: Promise<any>, connectionIndex: number) => {
    await expect(promise).rejects.toThrow(QueryError);
    await expect(promise).rejects.toThrow(answeredBy(connectionIndex));
  };

  // The socket the driver is writing to right now.
  const clientSocket = (): Socket => (connection as any).webSocket._socket;

  it('resends a query when the write fails with EPIPE', async () => {
    connection = new WebSocketConnection(server.url);

    // Establish the connection with a first, successfully answered query.
    await expectAnsweredBy(query('SELECT 1'), 0);

    // Keep the outgoing frame in the socket write buffer, then break the socket
    // the way Node does once Cube Store is gone: the buffered write fails with
    // EPIPE while `ws` still reports the connection as OPEN.
    const socket = clientSocket();
    socket.cork();
    const promise = query('SELECT 2');
    await nextTurn();
    socket.destroy(epipe());

    // The query never reached Cube Store, so it has to be resent over a new
    // connection instead of failing with the write error.
    await expectAnsweredBy(promise, 1);

    expect(server.received.map((message) => [message.connectionIndex, message.query])).toEqual([
      [0, 'SELECT 1'],
      [1, 'SELECT 2'],
    ]);
  }, JEST_TIMEOUT);

  it('resends every query that was in flight when the write failed', async () => {
    connection = new WebSocketConnection(server.url);

    await expectAnsweredBy(query('SELECT 1'), 0);

    const socket = clientSocket();
    socket.cork();
    const promises = [query('SELECT 2'), query('SELECT 3'), query('SELECT 4')];
    await nextTurn();
    socket.destroy(epipe());

    await Promise.all(promises.map((promise) => expectAnsweredBy(promise, 1)));

    expect(
      server.received.filter((message) => message.connectionIndex === 1).map((message) => message.query).sort()
    ).toEqual(['SELECT 2', 'SELECT 3', 'SELECT 4']);
  }, JEST_TIMEOUT);

  it('resends a query when the socket is no longer writable', async () => {
    connection = new WebSocketConnection(server.url);

    await expectAnsweredBy(query('SELECT 1'), 0);

    // Half-close the socket: `ws` still reports OPEN, but the write fails.
    clientSocket().end();

    await expectAnsweredBy(query('SELECT 2'), 1);
  }, JEST_TIMEOUT);

  it('resends a query when Cube Store closes the connection without answering', async () => {
    connection = new WebSocketConnection(server.url);

    server.handler = (message, mockConnection) => {
      if (mockConnection.index === 0) {
        // Cube Store went away in the middle of the query.
        mockConnection.ws.close();
        return;
      }

      mockConnection.ws.send(buildErrorMessage(message.messageId, answeredBy(mockConnection.index)));
    };

    await expectAnsweredBy(query('SELECT 1'), 1);
  }, JEST_TIMEOUT);

  describe('message size limit', () => {
    const MAX_MESSAGE_SIZE = 1024 * 1024;

    beforeEach(() => {
      process.env.CUBEJS_CUBESTORE_MAX_MESSAGE_SIZE = String(MAX_MESSAGE_SIZE);
    });

    afterEach(() => {
      delete process.env.CUBEJS_CUBESTORE_MAX_MESSAGE_SIZE;
    });

    it('reports a response that is over the limit instead of retrying it', async () => {
      connection = new WebSocketConnection(server.url);

      server.handler = (message, mockConnection) => {
        mockConnection.ws.send(Buffer.alloc(MAX_MESSAGE_SIZE * 2));
      };

      const promise = query('SELECT 1');

      await expect(promise).rejects.toThrow(MessageTooLargeError);
      await expect(promise).rejects.toThrow(
        'Cube Store response size exceeds the maximum message size of 1 MB. ' +
        'Reduce the amount of data the query returns, e.g. by adding filters or a limit, ' +
        'or raise CUBEJS_CUBESTORE_MAX_MESSAGE_SIZE.'
      );

      // Re-running the query would only produce the same oversized response.
      expect(server.received).toHaveLength(1);
    }, JEST_TIMEOUT);

    it('reports a request that is over the limit without sending it', async () => {
      connection = new WebSocketConnection(server.url);

      const promise = query(`SELECT ${'x'.repeat(MAX_MESSAGE_SIZE + 1)}`);

      await expect(promise).rejects.toThrow(MessageTooLargeError);
      await expect(promise).rejects.toThrow(
        /Cube Store request size of \d+(\.\d+)? MB exceeds the maximum message size of 1 MB/
      );

      expect(server.connections).toHaveLength(0);
    }, JEST_TIMEOUT);

    it('reports a request Cube Store refused as too big instead of retrying it', async () => {
      connection = new WebSocketConnection(server.url);

      server.handler = (message, mockConnection) => {
        // How Cube Store rejects a message that doesn't fit into its limits.
        mockConnection.ws.close(1009, 'Message too big');
      };

      const promise = query('SELECT 1');

      await expect(promise).rejects.toThrow(MessageTooLargeError);
      await expect(promise).rejects.toThrow(
        'Cube Store closed the connection: message size exceeds the maximum message size Cube Store accepts'
      );

      expect(server.received).toHaveLength(1);
    }, JEST_TIMEOUT);
  });

  it('rejects a query when the connection cannot be re-established', async () => {
    process.env.CUBEJS_CUBESTORE_MAX_CONNECT_RETRIES = '2';

    try {
      const { url } = server;
      await server.stop();

      connection = new WebSocketConnection(url);

      await expect(query('SELECT 1')).rejects.toThrow('CubeStore connection failed after 2 retries');
    } finally {
      delete process.env.CUBEJS_CUBESTORE_MAX_CONNECT_RETRIES;
    }
  }, JEST_TIMEOUT);
});
