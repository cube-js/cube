import WebSocket from 'ws';
import * as flatbuffers from 'flatbuffers';
import { v4 as uuidv4 } from 'uuid';
import { InlineTable } from '@cubejs-backend/base-driver';
import { getEnv, getProcessUid } from '@cubejs-backend/shared';
import { parseCubestoreResultMessage } from '@cubejs-backend/native';
import { ConnectionError, MessageTooLargeError, QueryError } from './errors';
import {
  BinaryValue,
  BoolValue,
  Float64Value,
  HttpCommand,
  HttpError,
  HttpMessage,
  HttpParameter,
  HttpParameterValue,
  HttpQuery,
  HttpTable,
  Int64Value,
  NullValue,
  QueryResultFormat,
  StringValue,
} from '../codegen';

// The WebSocket close code for a message that is too big to be processed: `ws`
// closes with it when an incoming message is over `maxPayload`, and a peer that
// refuses a message of ours is expected to close with it as well.
const MESSAGE_TOO_BIG_CLOSE_CODE = 1009;

// The `ws` error code for an incoming message bigger than `maxPayload`.
const MAX_PAYLOAD_EXCEEDED_CODE = 'WS_ERR_UNSUPPORTED_MESSAGE_LENGTH';

function formatSize(bytes: number): string {
  const units: [number, string][] = [[1024 * 1024, 'MB'], [1024, 'KB']];

  for (const [unit, name] of units) {
    if (bytes >= unit) {
      return `${Math.round((bytes / unit) * 10) / 10} ${name}`;
    }
  }

  return `${bytes} bytes`;
}

interface SentMessage {
  resolve: (value: any) => void;
  reject: (reason?: any) => void;
  buffer: Uint8Array;
  // How many connections died under this message from a failure that can't be
  // attributed to a single message. Used to give it exactly one more round.
  fatalRounds: number;
}

export type QueryParameter = null | boolean | number | string | Buffer;

export type WebSocketQueryOptions = {
  inlineTables?: InlineTable[];
  queryTracingObj?: any;
  responseFormat: QueryResultFormat;
};

interface CubeStoreWebSocket extends WebSocket {
  readyPromise: Promise<CubeStoreWebSocket>;
  lastHeartBeat: Date;
  sentMessages: Record<number, SentMessage>;
  sendAsync: (message: Uint8Array) => Promise<void>;
  // A failure that killed this socket and that re-sending can't fix, so the
  // message that caused it is rejected instead of being re-sent.
  fatalError: Error | null;
  // Stops the heartbeat interval, which otherwise keeps a reference to this socket
  // (and the socket itself alive) even when nothing else points to the connection.
  teardown: () => void;
}

export class WebSocketConnection {
  protected messageCounter: number;

  protected readonly maxConnectRetries: number;

  protected readonly noHeartBeatTimeout: number;

  protected readonly maxMessageSize: number;

  protected currentConnectionTry: number;

  protected webSocket: CubeStoreWebSocket | null = null;

  private readonly url: string;

  private readonly connectionId: string;

  private cubeStoreVersion: string | null = null;

  // Set by close(), and never unset: a closed connection stays open only for the
  // messages already in flight and never establishes another socket. Without it
  // close() does not actually close anything -- the 'close' handler re-sends
  // whatever was pending over a fresh connection, and the heartbeat on that one
  // keeps it (and the orchestrator holding it) alive for the life of the process.
  private closed: boolean = false;

  // When close() was asked for, so the heartbeat can bound the wait for the
  // messages that were still in flight at that point.
  private closedAt: Date | null = null;

  public constructor(url: string) {
    this.url = url;
    this.messageCounter = 1;
    this.maxConnectRetries = getEnv('cubeStoreMaxConnectRetries');
    this.noHeartBeatTimeout = getEnv('cubeStoreNoHeartBeatTimeout');
    this.maxMessageSize = getEnv('cubeStoreMaxMessageSize');
    this.currentConnectionTry = 0;
    this.connectionId = uuidv4();
  }

  protected async initWebSocket(): Promise<CubeStoreWebSocket> {
    if (this.closed) {
      // Refusing here is what makes close() terminal: it covers a query issued
      // after the release as well as the re-send path, whose `catch` then
      // rejects the messages that were in flight instead of re-opening for them.
      throw new ConnectionError('Cube Store connection is closed');
    }

    if (!this.webSocket) {
      const headers: Record<string, string> = {};
      headers['x-process-id'] = getProcessUid();

      const webSocket = new WebSocket(this.url, { headers, maxPayload: this.maxMessageSize }) as CubeStoreWebSocket;
      webSocket.on('upgrade', (response: any) => {
        this.cubeStoreVersion = response.headers['x-cubestore-version'] || null;
      });

      webSocket.readyPromise = new Promise<CubeStoreWebSocket>((resolve, reject) => {
        webSocket.lastHeartBeat = new Date();
        const pingInterval = setInterval(() => {
          if (webSocket.readyState === WebSocket.OPEN) {
            webSocket.ping();
          }

          if (this.closed) {
            // A closed connection is kept only for the messages already in
            // flight, and this is where that wait is both re-checked and
            // bounded. It has to be bounded here rather than by the heartbeat
            // check below: Cube Store keeps answering the pings of a connection
            // whose query is simply never completed, so that check never fires
            // and a single wedged message would keep this socket -- and this
            // interval, which is what makes it reachable -- for the life of the
            // process. Pinging continues meanwhile, so a connection that is
            // draining legitimately is not dropped for inactivity.
            if (this.closedAt && new Date().getTime() - this.closedAt.getTime() > this.noHeartBeatTimeout * 1000) {
              this.abandonClose(webSocket);
            } else {
              this.closeIfDrained(webSocket);
            }

            return;
          }

          if (new Date().getTime() - webSocket.lastHeartBeat.getTime() > this.noHeartBeatTimeout * 1000) {
            webSocket.close();
          }
        }, 5000);

        webSocket.teardown = () => clearInterval(pingInterval);

        webSocket.sendAsync = async (message: Uint8Array) => new Promise<void>((resolveSend) => {
          // If socket is closing this message should be resent
          if (webSocket.readyState !== WebSocket.OPEN) {
            resolveSend();
            return;
          }

          webSocket.send(message, (err) => {
            if (err) {
              // The write failed (EPIPE/ECONNRESET when Cube Store dropped the
              // connection). The message stays registered in `sentMessages` and
              // terminating gets 'close' to re-send it over a new connection --
              // failing it here would surface a spurious `write EPIPE` for a
              // query that never reached Cube Store.
              webSocket.terminate();
            }

            resolveSend();
          });
        });
        webSocket.on('open', () => resolve(webSocket));
        webSocket.on('error', (err) => {
          if ((err as any).code === MAX_PAYLOAD_EXCEEDED_CODE) {
            // Cube Store answered with a message bigger than this connection
            // accepts, and `ws` is tearing the connection down. Neither
            // reconnecting nor retrying the query helps: the response would be
            // just as big. Pending messages are rejected by the 'close' handler.
            webSocket.fatalError = new MessageTooLargeError(
              `Cube Store response size exceeds the maximum message size of ${formatSize(this.maxMessageSize)}. ` +
              'Reduce the amount of data the query returns, e.g. by adding filters or a limit, ' +
              'or raise CUBEJS_CUBESTORE_MAX_MESSAGE_SIZE.',
              err
            );

            if (webSocket === this.webSocket) {
              this.webSocket = null;
            }

            // No-op if the connection was already established.
            reject(webSocket.fatalError);

            return;
          }

          this.currentConnectionTry += 1;

          // The socket is done either way, so stop its heartbeat now rather than
          // relying on a 'close' that may not follow: the interval is what keeps
          // the socket, and everything reachable from it, alive.
          webSocket.teardown();

          if (this.closed) {
            // Nothing to retry towards: `initWebSocket()` refuses once closed.
            // Whatever was written to the socket is rejected by the 'close'
            // handler, which `ws` emits after an 'error' -- the drain bound
            // cannot catch it any more, since `teardown()` above has already
            // stopped the interval that evaluates it.
            if (webSocket === this.webSocket) {
              this.webSocket = null;
            }

            // A no-op once this socket has opened, and the only thing left that
            // can settle `readyPromise` if it has not: `teardown()` has stopped
            // the interval, the 'close' handler does not touch it, and the retry
            // that used to settle it by adoption is skipped just above. A socket
            // closed while still CONNECTING would otherwise leave the
            // `sendMessage` awaiting it pending for good -- a request hung on
            // nothing, which is worse than the failure it replaced.
            reject(new ConnectionError('Cube Store connection is closed'));

            return;
          }

          if (this.currentConnectionTry < this.maxConnectRetries) {
            setTimeout(async () => {
              // `.then(resolve, reject)` rather than `resolve(promise)`: once
              // this socket's `readyPromise` has settled, `resolve` returns
              // without adopting -- so a rejection handed to it is never
              // observed, and an unhandled rejection is fatal under Node's
              // default. The retry above cannot reject for `closed` any more,
              // but a close landing between this timer being armed and firing
              // still can, as can anything else `initWebSocket()` throws.
              this.initWebSocket().then(resolve, reject);
            }, this.retryWaitTime());
          } else {
            reject(new ConnectionError(
              `CubeStore connection failed after ${this.maxConnectRetries} retries: ${err.message}`,
              err
            ));
          }

          if (webSocket === this.webSocket) {
            this.webSocket = null;
          }
        });
        webSocket.on('pong', () => {
          if (webSocket === this.webSocket) {
            this.currentConnectionTry = 0;
          }
          webSocket.lastHeartBeat = new Date();
        });
        webSocket.on('close', (code: number, reason: Buffer) => {
          webSocket.teardown();

          const pending = Object.keys(webSocket.sentMessages);

          if (pending.length) {
            // Cube Store names the size and the limit that refused it here,
            // which is strictly better than the generic wording, so it
            // replaces it rather than being appended to it. Peer-supplied
            // text lands in an error a user reads, so control characters are
            // folded out and the trailing full stop is normalised rather than
            // assumed absent. A peer that closes with 1009 and no reason --
            // an intermediary, or an older Cube Store -- keeps the generic
            // wording, which is the only reason it still exists.
            const closeReason = reason?.length
              // eslint-disable-next-line no-control-regex
              ? `${reason}`.replace(/[\u0000-\u001F\u007F]+/g, ' ').replace(/\s*\.?\s*$/, '')
              : '';
            const fatalError = webSocket.fatalError || (
              // Cube Store refused a message that didn't fit into its limits.
              code === MESSAGE_TOO_BIG_CLOSE_CODE ? new MessageTooLargeError(
                `Cube Store closed the connection: ${closeReason || 'message size exceeds the maximum message size Cube Store accepts'}. `
                + 'Reduce the size of the query and of the inline tables it sends, or raise '
                + 'CUBESTORE_TRANSPORT_MAX_MESSAGE_SIZE or CUBESTORE_TRANSPORT_MAX_FRAME_SIZE '
                + 'on the Cube Store side.'
              ) : null
            );

            // The connection multiplexes messages and an oversized one can't be
            // attributed -- `ws` drops the frame before its message id is read
            // -- so every message in flight gets one more round, which answers
            // the innocent ones and usually leaves the offender alone to be
            // named next time. That includes a message that was alone in
            // flight: `fatalError` says an oversized frame was seen on this
            // socket, not which query produced it, and it can be the response
            // to a query rejected on an earlier round that was still on the
            // wire. Whatever is still in flight after its round is failed
            // regardless: an offender whose response keeps arriving before the
            // other answers would otherwise be re-sent forever.
            if (fatalError) {
              // eslint-disable-next-line no-restricted-syntax
              for (const key of pending) {
                const sentMessage = webSocket.sentMessages[key];
                sentMessage.fatalRounds += 1;

                if (sentMessage.fatalRounds > 1) {
                  delete webSocket.sentMessages[key];
                  sentMessage.reject(fatalError);
                }
              }

              if (!Object.keys(webSocket.sentMessages).length) {
                if (webSocket === this.webSocket) {
                  this.webSocket = null;
                }

                return;
              }
            } else {
              // Only consecutive unattributable failures count towards giving
              // up on a message: a query that outlived an ordinary disconnect
              // gets its extra round back, so a later, unrelated oversized
              // response can't fail it on the spot. The loop the counter
              // bounds is fatal every round, so nothing resets there.
              // eslint-disable-next-line no-restricted-syntax
              for (const key of pending) {
                webSocket.sentMessages[key].fatalRounds = 0;
              }
            }

            setTimeout(async () => {
              try {
                // Everything this re-send was scheduled for has been settled in
                // the meantime, so there is nothing left to carry over and a new
                // connection would just stay open on its heartbeat.
                if (!Object.keys(webSocket.sentMessages).length) {
                  return;
                }

                const nextWebSocket = await this.initWebSocket();
                const resent = Object.keys(webSocket.sentMessages);

                // Register the whole batch before writing any of it. Writing
                // yields, and a socket that closes in between must find every
                // message of the batch: the ones not registered yet would end
                // up on a socket whose 'close' has already been handled, with
                // nobody left to write or to re-send them.
                // eslint-disable-next-line no-restricted-syntax
                for (const key of resent) {
                  nextWebSocket.sentMessages[key] = webSocket.sentMessages[key];
                }

                // eslint-disable-next-line no-restricted-syntax
                for (const key of resent) {
                  await nextWebSocket.sendAsync(webSocket.sentMessages[key].buffer);
                }
              } catch (e) {
                // eslint-disable-next-line no-restricted-syntax
                for (const key of Object.keys(webSocket.sentMessages)) {
                  webSocket.sentMessages[key].reject(e);
                }
              }
            }, this.retryWaitTime());
          }

          if (webSocket === this.webSocket) {
            this.webSocket = null;
          }
        });
        webSocket.on('message', async (msg: Buffer) => {
          const buf = new flatbuffers.ByteBuffer(msg);
          const httpMessage = HttpMessage.getRootAsHttpMessage(buf);

          const resolver = webSocket.sentMessages[httpMessage.messageId()];
          if (!resolver) {
            throw new QueryError(`Cube Store missed message id: ${httpMessage.messageId()}`);
          }

          delete webSocket.sentMessages[httpMessage.messageId()];

          if (httpMessage.commandType() === HttpCommand.HttpError) {
            resolver.reject(new QueryError(`${httpMessage.command(new HttpError())?.error()}`));
            this.closeIfDrained(webSocket);
            return;
          }

          try {
            const nativeResMsg = await parseCubestoreResultMessage(msg);
            resolver.resolve(nativeResMsg);
          } catch (e) {
            resolver.reject(e);
          }

          this.closeIfDrained(webSocket);
        });
      });

      webSocket.sentMessages = {};
      webSocket.fatalError = null;
      this.webSocket = webSocket;
    }

    return this.webSocket!.readyPromise;
  }

  private retryWaitTime() {
    return 1000 * (this.currentConnectionTry + 1);
  }

  private async sendMessage(messageId: number, buffer: Uint8Array): Promise<any> {
    if (buffer.length > this.maxMessageSize) {
      // Cube Store would close the connection on such a message, which shows up
      // as an unrelated `write EPIPE`, so report it before sending anything.
      // This only catches what is over our own limit: Cube Store applies its
      // own, by default stricter, CUBESTORE_TRANSPORT_MAX_MESSAGE_SIZE, and a
      // message it refuses is reported once it closes the connection.
      throw new MessageTooLargeError(
        `Cube Store request size of ${formatSize(buffer.length)} exceeds the maximum message size of ` +
        `${formatSize(this.maxMessageSize)}. Reduce the size of the query and of the inline tables it sends, ` +
        'or raise CUBEJS_CUBESTORE_MAX_MESSAGE_SIZE together with CUBESTORE_TRANSPORT_MAX_MESSAGE_SIZE ' +
        'on the Cube Store side.'
      );
    }

    const socket = await this.initWebSocket();
    return new Promise((resolve, reject) => {
      socket.sentMessages[messageId] = { resolve, reject, buffer, fatalRounds: 0 };

      // If socket is closing this message should be resent
      if (socket.readyState === WebSocket.OPEN) {
        socket.send(buffer, (err) => {
          if (err) {
            // Leave the message registered and let 'close' re-send it over a
            // new connection instead of failing it with the write error.
            socket.terminate();
          }
        });
      } else if (socket.readyState === WebSocket.CLOSED) {
        // 'close' already fired for this socket, so no re-send is going to pick
        // this message up and nothing would ever settle it. That handler also
        // dropped `this.webSocket`, so trying again establishes a fresh
        // connection rather than failing a query that was never written. The
        // `messageId` is deliberately kept, like a re-send: Cube Store
        // de-duplicates on `(connection_id, message_id)`.
        delete socket.sentMessages[messageId];
        this.sendMessage(messageId, buffer).then(resolve, reject);
      }
    });
  }

  protected serializeParameter(builder: flatbuffers.Builder, parameter: unknown) {
    if (parameter === null || parameter === undefined) {
      const httpParameterValueOffset = NullValue.createNullValue(builder);

      return HttpParameter.createHttpParameter(
        builder,
        HttpParameterValue.NullValue,
        httpParameterValueOffset
      );
    }

    switch (typeof parameter) {
      case 'object':
      {
        if (Buffer.isBuffer(parameter)) {
          const valueOffset = BinaryValue.createVVector(builder, parameter);
          const httpParameterValueOffset = BinaryValue.createBinaryValue(builder, valueOffset);

          return HttpParameter.createHttpParameter(
            builder,
            HttpParameterValue.BinaryValue,
            httpParameterValueOffset
          );
        } else {
          throw new Error('Parameter with type: object is not supported');
        }
      }
      case 'boolean':
      {
        const httpParameterValueOffset = BoolValue.createBoolValue(
          builder,
          parameter
        );

        return HttpParameter.createHttpParameter(
          builder,
          HttpParameterValue.BoolValue,
          httpParameterValueOffset
        );
      }
      case 'number':
      {
        if (Number.isInteger(parameter)) {
          const httpParameterValueOffset = Int64Value.createInt64Value(builder, BigInt(parameter));

          return HttpParameter.createHttpParameter(
            builder,
            HttpParameterValue.Int64Value,
            httpParameterValueOffset
          );
        } else {
          const httpParameterValueOffset = Float64Value.createFloat64Value(builder, parameter);

          return HttpParameter.createHttpParameter(
            builder,
            HttpParameterValue.Float64Value,
            httpParameterValueOffset
          );
        }
      }
      case 'string':
      {
        const valueOffset = builder.createString(parameter);
        const httpParameterValueOffset = StringValue.createStringValue(builder, valueOffset);

        return HttpParameter.createHttpParameter(
          builder,
          HttpParameterValue.StringValue,
          httpParameterValueOffset
        );
      }
      default:
        throw new Error(`Parameter with type: ${typeof parameter} is not supported`);
    }
  }

  public async query(query: string, parameters: QueryParameter[], options: WebSocketQueryOptions): Promise<any[]> {
    const { inlineTables, queryTracingObj, responseFormat } = options;

    const builder = new flatbuffers.Builder(1024);
    const queryOffset = builder.createString(query);

    let traceObjOffset: number | null = null;
    if (queryTracingObj) {
      traceObjOffset = builder.createString(JSON.stringify(queryTracingObj));
    }

    let inlineTablesOffset: number | null = null;
    if (inlineTables && inlineTables.length > 0) {
      const inlineTableOffsets: number[] = [];
      for (const table of inlineTables) {
        const nameOffset = builder.createString(table.name);
        const columnOffsets: number[] = [];
        for (const column of table.columns) {
          const columnOffset = builder.createString(column.name);
          columnOffsets.push(columnOffset);
        }
        const columnsOffset = HttpTable.createColumnsVector(builder, columnOffsets);
        const typeOffsets: number[] = [];
        for (const column of table.columns) {
          const typeOffset = builder.createString(column.type);
          typeOffsets.push(typeOffset);
        }
        const typesOffset = HttpTable.createColumnsVector(builder, typeOffsets);
        const csvRowsOffset = builder.createString(table.csvRows);
        HttpTable.startHttpTable(builder);
        HttpTable.addName(builder, nameOffset);
        HttpTable.addColumns(builder, columnsOffset);
        HttpTable.addTypes(builder, typesOffset);
        HttpTable.addCsvRows(builder, csvRowsOffset);
        const inlineTableOffset = HttpTable.endHttpTable(builder);
        inlineTableOffsets.push(inlineTableOffset);
      }
      inlineTablesOffset = HttpQuery.createInlineTablesVector(builder, inlineTableOffsets);
    }

    let parametersOffset: flatbuffers.Offset | null = null;
    if (parameters.length > 0) {
      const httpParameterValues: flatbuffers.Offset[] = [];

      for (const parameter of parameters) {
        httpParameterValues.push(this.serializeParameter(builder, parameter));
      }

      parametersOffset = HttpQuery.createParametersVector(
        builder,
        httpParameterValues
      );
    }

    HttpQuery.startHttpQuery(builder);
    HttpQuery.addQuery(builder, queryOffset);

    if (traceObjOffset) {
      HttpQuery.addTraceObj(builder, traceObjOffset);
    }

    if (inlineTablesOffset) {
      HttpQuery.addInlineTables(builder, inlineTablesOffset);
    }

    if (parametersOffset) {
      HttpQuery.addParameters(builder, parametersOffset);
    }

    HttpQuery.addResponseFormat(builder, responseFormat);

    const httpQueryOffset = HttpQuery.endHttpQuery(builder);
    const messageId = this.messageCounter++;
    const connectionIdOffset = builder.createString(this.connectionId);
    const message = HttpMessage.createHttpMessage(builder, messageId, HttpCommand.HttpQuery, httpQueryOffset, connectionIdOffset);
    builder.finish(message);
    return this.sendMessage(messageId, builder.asUint8Array());
  }

  public async getCubeStoreVersion(): Promise<string> {
    if (this.webSocket) {
      await this.webSocket.readyPromise;
    }

    return this.cubeStoreVersion ?? '0.0.0';
  }

  /**
   * Closes the connection for good. Messages already in flight are given the
   * chance to be answered -- an eviction can happen mid-query, and failing one
   * that Cube Store is already working on would fail the request -- so the
   * socket goes away once the last of them settles, or right away if there are
   * none.
   */
  public close() {
    this.closed = true;
    this.closedAt = new Date();

    // Straight away when there is nothing in flight; otherwise the heartbeat
    // takes it from here, both to notice the drain finishing and to bound it.
    this.closeIfDrained(this.webSocket);
  }

  /**
   * Gives up on the messages a closed connection was waiting for, so that one
   * Cube Store never answers costs a query rather than a permanently open
   * socket.
   */
  private abandonClose(socket: CubeStoreWebSocket) {
    const unanswered = Object.keys(socket.sentMessages);

    socket.teardown();

    if (socket === this.webSocket) {
      this.webSocket = null;
    }

    const error = new ConnectionError(
      `Cube Store connection was closed with ${unanswered.length} message(s) still unanswered`
    );

    // eslint-disable-next-line no-restricted-syntax
    for (const key of unanswered) {
      const message = socket.sentMessages[key];
      delete socket.sentMessages[key];
      message.reject(error);
    }

    socket.terminate();
  }

  /**
   * Closes `socket` once nothing is in flight on it. A no-op until close() has
   * been called, so an ordinary idle connection is left alone.
   *
   * Takes the socket rather than reading `this.webSocket`: a late answer can
   * arrive on a socket the re-send path has already superseded, and that must
   * not be read as the current connection having drained.
   */
  private closeIfDrained(socket: CubeStoreWebSocket | null) {
    if (!this.closed || !socket || socket !== this.webSocket) {
      return;
    }

    if (Object.keys(socket.sentMessages).length) {
      return;
    }

    // Stop the heartbeat before closing rather than leaving it to the 'close'
    // handler: it is the timer, not the socket, that keeps this whole graph
    // reachable, so it should not outlive the decision to close by even a tick.
    socket.teardown();
    this.webSocket = null;
    socket.close();
  }
}
