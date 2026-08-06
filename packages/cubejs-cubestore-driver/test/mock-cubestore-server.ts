import { AddressInfo, Socket } from 'net';
import * as flatbuffers from 'flatbuffers';
import WebSocket from 'ws';

import {
  HttpCommand,
  HttpError,
  HttpMessage,
  HttpQuery,
  HttpQueryResult,
  HttpQueryResultArrow,
  HttpQueryResultData,
} from '../codegen';

export interface ReceivedMessage {
  connectionIndex: number;
  messageId: number;
  query: string;
}

export interface MockConnection {
  index: number;
  ws: WebSocket;
  socket: Socket;
}

export type MessageHandler = (message: ReceivedMessage, connection: MockConnection) => void;

/**
 * Cube Store answers a query either with a result set or with an error. Tests
 * use the error variant, because it's the only answer that can be asserted
 * without the native result parser, and it's enough to tell "the query reached
 * Cube Store and was answered" from "the query failed on the transport".
 */
export function buildErrorMessage(messageId: number, error: string): Buffer {
  const builder = new flatbuffers.Builder(1024);
  const errorOffset = builder.createString(error);
  const commandOffset = HttpError.createHttpError(builder, errorOffset);
  const message = HttpMessage.createHttpMessage(builder, messageId, HttpCommand.HttpError, commandOffset, 0);
  builder.finish(message);

  return Buffer.from(builder.asUint8Array());
}

/**
 * A successful answer. Its payload is decoded by the native result parser, so
 * tests that use it stub that parser out.
 */
export function buildResultMessage(messageId: number, data: Buffer = Buffer.alloc(0)): Buffer {
  const builder = new flatbuffers.Builder(1024);
  const dataOffset = HttpQueryResultArrow.createDataVector(builder, data);
  const arrowOffset = HttpQueryResultArrow.createHttpQueryResultArrow(builder, dataOffset, true);
  const commandOffset = HttpQueryResult.createHttpQueryResult(
    builder,
    HttpQueryResultData.HttpQueryResultArrow,
    arrowOffset
  );
  const message = HttpMessage.createHttpMessage(builder, messageId, HttpCommand.HttpQueryResult, commandOffset, 0);
  builder.finish(message);

  return Buffer.from(builder.asUint8Array());
}

export function answeredBy(connectionIndex: number): string {
  return `answered by connection #${connectionIndex}`;
}

/**
 * A minimal Cube Store look-alike: it speaks the same WebSocket + flatbuffers
 * protocol, so the driver talks to it over real TCP sockets, which can then be
 * broken in the exact ways a real Cube Store restart breaks them.
 */
export class MockCubeStoreServer {
  public readonly connections: MockConnection[] = [];

  public readonly received: ReceivedMessage[] = [];

  /**
   * Replies to every query with an error naming the connection that received
   * it, so a test can tell which connection answered. Can be replaced to
   * emulate a Cube Store that goes away instead of answering.
   */
  public handler: MessageHandler = (message, connection) => {
    connection.ws.send(buildErrorMessage(message.messageId, answeredBy(connection.index)));
  };

  protected constructor(protected readonly wss: WebSocket.Server) {
    wss.on('connection', (ws: WebSocket, request: any) => {
      const connection: MockConnection = {
        index: this.connections.length,
        ws,
        socket: request.socket,
      };
      this.connections.push(connection);

      ws.on('message', (raw: Buffer) => {
        const httpMessage = HttpMessage.getRootAsHttpMessage(new flatbuffers.ByteBuffer(raw));
        const message: ReceivedMessage = {
          connectionIndex: connection.index,
          messageId: httpMessage.messageId(),
          query: httpMessage.command(new HttpQuery())?.query() || '',
        };
        this.received.push(message);
        this.handler(message, connection);
      });
      // Connections are torn down by the tests on purpose, nothing to report.
      ws.on('error', () => {
        // noop
      });
    });
  }

  public static async start(): Promise<MockCubeStoreServer> {
    const wss = new WebSocket.Server({ host: '127.0.0.1', port: 0 });
    await new Promise<void>((resolve, reject) => {
      wss.once('listening', resolve);
      wss.once('error', reject);
    });

    return new MockCubeStoreServer(wss);
  }

  public get url(): string {
    return `ws://127.0.0.1:${(this.wss.address() as AddressInfo).port}`;
  }

  public connection(index: number): MockConnection {
    if (!this.connections[index]) {
      throw new Error(`Connection #${index} was never established`);
    }

    return this.connections[index];
  }

  public async waitForConnections(count: number, timeout: number = 20000): Promise<void> {
    await this.waitFor(() => this.connections.length >= count, timeout, `${count} connection(s)`);
  }

  public async waitForMessages(count: number, timeout: number = 20000): Promise<void> {
    await this.waitFor(() => this.received.length >= count, timeout, `${count} message(s)`);
  }

  protected async waitFor(condition: () => boolean, timeout: number, description: string): Promise<void> {
    const deadline = Date.now() + timeout;

    while (!condition()) {
      if (Date.now() > deadline) {
        throw new Error(`Timed out waiting for ${description}`);
      }

      await new Promise((resolve) => { setTimeout(resolve, 25); });
    }
  }

  public async stop(): Promise<void> {
    for (const connection of this.connections) {
      connection.ws.terminate();
    }

    await new Promise<void>((resolve) => {
      this.wss.close(() => resolve());
    });
  }
}
