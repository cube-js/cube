import { Socket } from 'net';

import { WebSocketConnection } from '../src/WebSocketConnection';
import { MessageTooLargeError, QueryError } from '../src/errors';
import { QueryResultFormat } from '../codegen';
import {
  answeredBy,
  buildErrorMessage,
  buildResultMessage,
  MockConnection,
  MockCubeStoreServer,
} from './mock-cubestore-server';

const QUERY_RESULT = [{ answer: 42 }];

// Decoding a result set is a native addon and is orthogonal to the transport
// under test, so only that step is stubbed: the socket, the WebSocket framing
// and the flatbuffers protocol stay real.
jest.mock('@cubejs-backend/native', () => ({
  parseCubestoreResultMessage: jest.fn(async () => [{ answer: 42 }]),
}));

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

/**
 * Waits until the frame the driver just handed to `ws` has reached the socket
 * write buffer, where a corked socket holds it.
 */
const waitForBufferedWrite = async (socket: Socket) => {
  const deadline = Date.now() + 5000;

  while (!socket.writableLength) {
    if (Date.now() > deadline) {
      throw new Error('Timed out waiting for a buffered write');
    }

    await new Promise((resolve) => { setImmediate(resolve); });
  }
};

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

  it('resolves a query with the result Cube Store sent', async () => {
    connection = new WebSocketConnection(server.url);

    server.handler = (message, mockConnection) => {
      mockConnection.ws.send(buildResultMessage(message.messageId));
    };

    await expect(query('SELECT 1')).resolves.toEqual(QUERY_RESULT);

    // And the same after the connection had to be re-established mid-query.
    const socket = clientSocket();
    socket.cork();
    const promise = query('SELECT 2');
    await waitForBufferedWrite(socket);
    socket.destroy(epipe());

    await expect(promise).resolves.toEqual(QUERY_RESULT);
    expect(server.received.map((message) => message.query)).toEqual(['SELECT 1', 'SELECT 2']);
  }, JEST_TIMEOUT);

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
    await waitForBufferedWrite(socket);
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
    await waitForBufferedWrite(socket);
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

    it('reports a response that is over the limit once its extra round is spent', async () => {
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

      // Re-sent once, since a fatal close on a sole in-flight query can just as
      // well be an ordinary disconnect. The second oversized response spends
      // its extra round and the size is reported rather than retried again.
      expect(server.received).toHaveLength(2);
    }, JEST_TIMEOUT);

    it('resends the other queries in flight and attributes the limit to the offender', async () => {
      connection = new WebSocketConnection(server.url);

      // Cube Store answers the small query before it is done producing the big
      // one. Ordering the two sends rather than spacing them apart in time
      // keeps the test independent of how fast the driver is scheduled: the
      // answer to the small query is on the wire, and therefore processed,
      // before the oversized frame that tears the connection down.
      const answeredSmall = new Set<number>();
      const deferredBig = new Map<number, MockConnection>();
      const sendOversized = (mockConnection: MockConnection) => {
        mockConnection.ws.send(Buffer.alloc(MAX_MESSAGE_SIZE * 2));
      };

      server.handler = (message, mockConnection) => {
        if (message.query === 'SELECT big') {
          // On the first connection the small query is left in flight, so that
          // the oversized response kills it along with the query it belongs to.
          if (mockConnection.index === 0 || answeredSmall.has(mockConnection.index)) {
            sendOversized(mockConnection);
          } else {
            deferredBig.set(mockConnection.index, mockConnection);
          }

          return;
        }

        if (mockConnection.index > 0) {
          mockConnection.ws.send(buildErrorMessage(message.messageId, answeredBy(mockConnection.index)));
          answeredSmall.add(mockConnection.index);

          const deferred = deferredBig.get(mockConnection.index);
          if (deferred) {
            deferredBig.delete(mockConnection.index);
            sendOversized(deferred);
          }
        }
      };

      const big = query('SELECT big');
      // Asserted below, handled here so that a rejection arriving earlier than
      // expected is reported as a failed assertion and not as an unhandled one.
      big.catch(() => {
        // noop
      });
      const small = query('SELECT small');

      // The small query is unrelated to the size limit: it gets resent and
      // answered rather than failing with an error about a limit it never
      // approached.
      await expectAnsweredBy(small, 1);

      // Which leaves the offending query alone on the connection, where the
      // oversized response can be attributed to it.
      await expect(big).rejects.toThrow(MessageTooLargeError);
      await expect(big).rejects.toThrow('Cube Store response size exceeds the maximum message size of 1 MB');
    }, JEST_TIMEOUT);

    it('gives up when an over-limit response keeps killing the connection', async () => {
      connection = new WebSocketConnection(server.url);

      // The oversized response always wins the race against the small query's
      // answer, so re-sending never shrinks the set of messages in flight and
      // never leaves the offender alone to be attributed.
      const arrived = new Map<number, Set<string>>();

      server.handler = (message, mockConnection) => {
        const queries = arrived.get(mockConnection.index) || new Set<string>();
        queries.add(message.query);
        arrived.set(mockConnection.index, queries);

        if (queries.has('SELECT big') && queries.has('SELECT small')) {
          mockConnection.ws.send(Buffer.alloc(MAX_MESSAGE_SIZE * 2));
        }
      };

      const big = query('SELECT big');
      const small = query('SELECT small');

      // Both have to settle rather than being re-sent forever, even at the cost
      // of blaming the size on a query that never approached the limit.
      await expect(big).rejects.toThrow(MessageTooLargeError);
      await expect(small).rejects.toThrow(MessageTooLargeError);
    }, JEST_TIMEOUT);

    it('gives a query its extra round back once an ordinary disconnect intervenes', async () => {
      connection = new WebSocketConnection(server.url);

      const arrived = new Map<number, Set<string>>();
      const longMessageIds = new Map<number, number>();
      const sendOversized = (mockConnection: MockConnection) => {
        mockConnection.ws.send(Buffer.alloc(MAX_MESSAGE_SIZE * 2));
      };

      server.handler = (message, mockConnection) => {
        const { index } = mockConnection;
        const queries = arrived.get(index) || new Set<string>();
        queries.add(message.query);
        arrived.set(index, queries);

        if (message.query === 'SELECT long') {
          longMessageIds.set(index, message.messageId);
        }

        // Act once both are in flight, so the oversized response is never
        // attributable to the query that caused it.
        if (!queries.has('SELECT long') || !queries.has('SELECT big')) {
          return;
        }

        if (index === 1) {
          // An ordinary disconnect, unrelated to message size.
          mockConnection.ws.terminate();
          return;
        }

        const longMessageId = longMessageIds.get(index);
        if (index >= 3 && longMessageId !== undefined) {
          // The slow query finally answers, which leaves the offender alone.
          mockConnection.ws.send(buildErrorMessage(longMessageId, answeredBy(index)));
        }

        sendOversized(mockConnection);
      };

      const long = query('SELECT long');
      const big = query('SELECT big');
      big.catch(() => {
        // noop
      });

      // Two size incidents with an ordinary disconnect in between: the slow
      // query is innocent in both, so the round it is owed has to survive the
      // disconnect rather than being spent by the first incident.
      await expectAnsweredBy(long, 3);

      await expect(big).rejects.toThrow(MessageTooLargeError);
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

    it('reports a request Cube Store refused as too big once its extra round is spent', async () => {
      connection = new WebSocketConnection(server.url);

      server.handler = (message, mockConnection) => {
        // How Cube Store rejects a message that doesn't fit into its limits,
        // naming the size and the limit in the close reason.
        mockConnection.ws.close(
          1009,
          'Message of 16452 bytes exceeds the maximum message size of 4096 bytes'
        );
      };

      const promise = query('SELECT 1');

      await expect(promise).rejects.toThrow(MessageTooLargeError);
      // The reason replaces the generic clause rather than being appended to
      // it, so the sizes are stated once.
      await expect(promise).rejects.toThrow(
        'Cube Store closed the connection: ' +
        'Message of 16452 bytes exceeds the maximum message size of 4096 bytes. ' +
        'Reduce the size of the query and of the inline tables it sends, or raise ' +
        'CUBESTORE_TRANSPORT_MAX_MESSAGE_SIZE or CUBESTORE_TRANSPORT_MAX_FRAME_SIZE ' +
        'on the Cube Store side.'
      );

      // Re-sent once before the size is reported, same as an over-limit
      // response: one 1009 close is indistinguishable from a restart.
      expect(server.received).toHaveLength(2);
    }, JEST_TIMEOUT);

    it('falls back to the generic wording when 1009 carries no reason', async () => {
      connection = new WebSocketConnection(server.url);

      // An intermediary, or a Cube Store from before it sent a reason.
      server.handler = (message, mockConnection) => {
        mockConnection.ws.close(1009);
      };

      const promise = query('SELECT 1');

      await expect(promise).rejects.toThrow(MessageTooLargeError);
      await expect(promise).rejects.toThrow(
        'Cube Store closed the connection: message size exceeds the maximum message size Cube Store accepts. ' +
        'Reduce the size of the query and of the inline tables it sends, or raise ' +
        'CUBESTORE_TRANSPORT_MAX_MESSAGE_SIZE or CUBESTORE_TRANSPORT_MAX_FRAME_SIZE ' +
        'on the Cube Store side.'
      );
    }, JEST_TIMEOUT);

    it('does not double the full stop when 1009 carries a punctuated reason', async () => {
      connection = new WebSocketConnection(server.url);

      server.handler = (message, mockConnection) => {
        mockConnection.ws.close(1009, 'Message too big.');
      };

      const promise = query('SELECT 1');

      await expect(promise).rejects.toThrow(
        'Cube Store closed the connection: Message too big. Reduce the size of the query'
      );
    }, JEST_TIMEOUT);

    it('does not send the user to the message limit when the frame limit refused it', async () => {
      connection = new WebSocketConnection(server.url);

      // What Cube Store closes with when CUBESTORE_TRANSPORT_MAX_FRAME_SIZE is
      // configured below the message limit and is the one that fired.
      server.handler = (message, mockConnection) => {
        mockConnection.ws.close(
          1009,
          'Message of 9437184 bytes exceeds the maximum frame size of 4194304 bytes'
        );
      };

      const promise = query('SELECT 1');

      await expect(promise).rejects.toThrow(
        'Cube Store closed the connection: ' +
        'Message of 9437184 bytes exceeds the maximum frame size of 4194304 bytes. ' +
        'Reduce the size of the query and of the inline tables it sends, or raise ' +
        'CUBESTORE_TRANSPORT_MAX_MESSAGE_SIZE or CUBESTORE_TRANSPORT_MAX_FRAME_SIZE ' +
        'on the Cube Store side.'
      );
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
