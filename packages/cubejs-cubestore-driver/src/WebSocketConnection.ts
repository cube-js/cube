import WebSocket from 'ws';
import * as flatbuffers from 'flatbuffers';
import { v4 as uuidv4 } from 'uuid';
import { InlineTable } from '@cubejs-backend/base-driver';
import { getEnv, getProcessUid } from '@cubejs-backend/shared';
import { parseCubestoreResultMessage } from '@cubejs-backend/native';
import { ConnectionError, QueryError } from './errors';
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

interface SentMessage {
  resolve: (value: any) => void;
  reject: (reason?: any) => void;
  buffer: Uint8Array;
  // How many times this message was re-sent over a freshly established
  // connection. Used to give up instead of retrying forever.
  resendCount: number;
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
  sendAsync: (message: Uint8Array, messageId?: number) => Promise<void>;
  // Set as soon as the 'close' handler has scheduled a re-send of the messages
  // that are still in flight on this socket.
  resendScheduled: boolean;
}

export class WebSocketConnection {
  protected messageCounter: number;

  protected readonly maxConnectRetries: number;

  protected readonly noHeartBeatTimeout: number;

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
    this.currentConnectionTry = 0;
    this.connectionId = uuidv4();
  }

  protected async initWebSocket(): Promise<CubeStoreWebSocket> {
    if (!this.webSocket) {
      const headers: Record<string, string> = {};
      headers['x-process-id'] = getProcessUid();

      const webSocket = new WebSocket(this.url, { headers }) as CubeStoreWebSocket;
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

        webSocket.sendAsync = async (message: Uint8Array, messageId?: number) => new Promise<void>((resolveSend) => {
          // If socket is closing this message should be resent
          if (webSocket.readyState !== WebSocket.OPEN) {
            resolveSend();
            return;
          }

          webSocket.send(message, (err) => {
            if (err) {
              // The write failed (EPIPE/ECONNRESET when Cube Store dropped the
              // connection). The message stays registered in `sentMessages`, so
              // it's re-sent once this socket is closed and a new one is
              // established -- failing it here would surface a spurious
              // `write EPIPE` to the user for a perfectly retryable query.
              this.handleSendError(webSocket, err, messageId);
            }

            resolveSend();
          });
        });
        webSocket.on('open', () => resolve(webSocket));
        webSocket.on('error', (err) => {
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
        webSocket.on('close', () => {
          clearInterval(pingInterval);

          if (Object.keys(webSocket.sentMessages).length) {
            webSocket.resendScheduled = true;

            setTimeout(async () => {
              try {
                const nextWebSocket = await this.initWebSocket();
                // eslint-disable-next-line no-restricted-syntax
                for (const key of Object.keys(webSocket.sentMessages)) {
                  const sentMessage = webSocket.sentMessages[key];

                  if (sentMessage.resendCount >= this.maxConnectRetries) {
                    sentMessage.reject(new ConnectionError(
                      `CubeStore connection lost: message wasn't delivered after ${sentMessage.resendCount} retries`
                    ));
                  } else {
                    sentMessage.resendCount += 1;
                    nextWebSocket.sentMessages[key] = sentMessage;
                    await nextWebSocket.sendAsync(sentMessage.buffer, Number(key));
                  }
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
      webSocket.resendScheduled = false;
      this.webSocket = webSocket;
    }

    return this.webSocket!.readyPromise;
  }

  private retryWaitTime() {
    return 1000 * (this.currentConnectionTry + 1);
  }

  /**
   * Handles a failed write to an already established socket, e.g. `write EPIPE`
   * when Cube Store closed the connection between the `readyState` check and the
   * actual write to the underlying TCP socket.
   *
   * Such a message is not lost: it stays registered in `sentMessages` and is
   * re-sent by the 'close' handler over a freshly established connection, so it
   * must not be rejected here. The socket is terminated to make sure that
   * 'close' (and with it the re-send) really happens.
   */
  private handleSendError(webSocket: CubeStoreWebSocket, err: Error, messageId?: number) {
    if (webSocket.readyState !== WebSocket.CLOSED) {
      if (webSocket.readyState === WebSocket.OPEN) {
        // The socket is broken, but `ws` doesn't know it yet. Terminating it
        // emits 'close', which re-sends everything still pending on it.
        webSocket.terminate();
      }

      // 'close' is still to come and will re-send pending messages.
      return;
    }

    // The socket is already closed and the re-send loop is not going to pick
    // this message up, so there's nothing left to wait for.
    if (!webSocket.resendScheduled && messageId !== undefined) {
      const sentMessage = webSocket.sentMessages[messageId];
      if (sentMessage) {
        delete webSocket.sentMessages[messageId];
        sentMessage.reject(new ConnectionError(
          `CubeStore connection error: ${err.message}`,
          err
        ));
      }
    }
  }

  private async sendMessage(messageId: number, buffer: Uint8Array): Promise<any> {
    const socket = await this.initWebSocket();
    return new Promise((resolve, reject) => {
      socket.sentMessages[messageId] = {
        resolve,
        reject,
        buffer,
        resendCount: 0,
      };

      // If socket is closing this message should be resent
      if (socket.readyState === WebSocket.OPEN) {
        socket.send(buffer, (err) => {
          if (err) {
            this.handleSendError(socket, err, messageId);
          }
        });
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
