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

          if (new Date().getTime() - webSocket.lastHeartBeat.getTime() > this.noHeartBeatTimeout * 1000) {
            webSocket.close();
          }
        }, 5000);

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

          if (this.currentConnectionTry < this.maxConnectRetries) {
            setTimeout(async () => {
              resolve(this.initWebSocket());
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
        webSocket.on('close', (code: number) => {
          clearInterval(pingInterval);

          const pending = Object.keys(webSocket.sentMessages);

          if (pending.length) {
            const fatalError = webSocket.fatalError || (
              // Cube Store refused a message that didn't fit into its limits.
              code === MESSAGE_TOO_BIG_CLOSE_CODE ? new MessageTooLargeError(
                'Cube Store closed the connection: message size exceeds the maximum message size Cube Store accepts. ' +
                'Reduce the size of the query and of the inline tables it sends, or raise ' +
                'CUBESTORE_TRANSPORT_MAX_MESSAGE_SIZE on the Cube Store side.'
              ) : null
            );

            // The connection multiplexes messages and an oversized one can't be
            // attributed -- `ws` drops the frame before its message id is read
            // -- so only a message that was alone in flight can be failed with
            // it. Anything else is re-sent as usual: answering the innocent
            // messages leaves the offender alone, and the next round names it.
            if (fatalError && pending.length === 1) {
              webSocket.sentMessages[pending[0]].reject(fatalError);
              delete webSocket.sentMessages[pending[0]];

              if (webSocket === this.webSocket) {
                this.webSocket = null;
              }

              return;
            }

            setTimeout(async () => {
              try {
                const nextWebSocket = await this.initWebSocket();
                // eslint-disable-next-line no-restricted-syntax
                for (const key of Object.keys(webSocket.sentMessages)) {
                  nextWebSocket.sentMessages[key] = webSocket.sentMessages[key];
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
            return;
          }

          try {
            const nativeResMsg = await parseCubestoreResultMessage(msg);
            resolver.resolve(nativeResMsg);
          } catch (e) {
            resolver.reject(e);
          }
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
      socket.sentMessages[messageId] = { resolve, reject, buffer };

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
        // this message up and nothing would ever settle it.
        delete socket.sentMessages[messageId];
        reject(new ConnectionError('CubeStore connection is closed'));
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

  public close() {
    if (this.webSocket) {
      this.webSocket.close();
    }
  }
}
