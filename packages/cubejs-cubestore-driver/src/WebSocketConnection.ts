import WebSocket from 'ws';
import * as flatbuffers from 'flatbuffers';
import { v4 as uuidv4 } from 'uuid';
import { InlineTable } from '@cubejs-backend/base-driver';
import { getEnv } from '@cubejs-backend/shared';
import { parseCubestoreResultMessage } from '@cubejs-backend/native';
import { ConnectionError, QueryError } from './errors';
import {
  HttpCommand,
  HttpError,
  HttpMessage,
  HttpQuery,
  HttpResultSet,
  HttpTable
} from '../codegen';

interface SentMessage {
  resolve: (value: any) => void;
  reject: (reason?: any) => void;
  buffer: Uint8Array;
}

interface CubeStoreWebSocket extends WebSocket {
  readyPromise: Promise<CubeStoreWebSocket>;
  lastHeartBeat: Date;
  sentMessages: Record<number, SentMessage>;
  sendAsync: (message: Uint8Array) => Promise<void>;
}

export class WebSocketConnection {
  protected messageCounter: number;

  protected readonly maxConnectRetries: number;

  protected readonly noHeartBeatTimeout: number;

  protected currentConnectionTry: number;

  protected webSocket: CubeStoreWebSocket | null = null;

  private readonly url: string;

  private readonly connectionId: string;

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
      const webSocket = new WebSocket(this.url) as CubeStoreWebSocket;
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

        webSocket.sendAsync = async (message: Uint8Array) => new Promise<void>((resolveSend, rejectSend) => {
          // If socket is closing this message should be resent
          if (webSocket.readyState === WebSocket.OPEN) {
            webSocket.send(message, (err) => {
              if (err) {
                rejectSend(new ConnectionError(
                  `CubeStore connection error: ${err.message}`,
                  err
                ));
              } else {
                resolveSend();
              }
            });
          }
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
          const resolvers = webSocket.sentMessages[httpMessage.messageId()];
          delete webSocket.sentMessages[httpMessage.messageId()];
          if (!resolvers) {
            throw new QueryError(`Cube Store missed message id: ${httpMessage.messageId()}`);
          }

          if (getEnv('nativeOrchestrator') && msg.length > 1000) {
            try {
              const nativeResMsg = await parseCubestoreResultMessage(msg);
              resolvers.resolve(nativeResMsg);
            } catch (e) {
              resolvers.reject(e);
            }
          } else {
            const commandType = httpMessage.commandType();

            if (commandType === HttpCommand.HttpError) {
              resolvers.reject(new QueryError(`${httpMessage.command(new HttpError())?.error()}`));
            } else if (commandType === HttpCommand.HttpResultSet) {
              const resultSet = httpMessage.command(new HttpResultSet());

              if (!resultSet) {
                resolvers.reject(new QueryError('Empty resultSet'));
                return;
              }

              const columnsLen = resultSet.columnsLength();
              const columns: Array<string> = [];
              for (let i = 0; i < columnsLen; i++) {
                const columnName = resultSet.columns(i);
                if (!columnName) {
                  resolvers.reject(new QueryError('Column name is not defined'));
                  return;
                }
                columns.push(columnName);
              }

              const rowLen = resultSet.rowsLength();
              const result: any[] = [];
              for (let i = 0; i < rowLen; i++) {
                const row = resultSet.rows(i);
                if (!row) {
                  resolvers.reject(new QueryError('Null row'));
                  return;
                }
                const valueLen = row.valuesLength();
                const rowObj = {};
                for (let j = 0; j < valueLen; j++) {
                  const value = row.values(j);
                  rowObj[columns[j]] = value?.stringValue();
                }
                result.push(rowObj);
              }

              resolvers.resolve(result);
            } else {
              resolvers.reject(new QueryError('Unsupported command'));
            }
          }
        });
      });

      webSocket.sentMessages = {};
      this.webSocket = webSocket;
    }

    return this.webSocket!.readyPromise;
  }

  private retryWaitTime() {
    return 1000 * (this.currentConnectionTry + 1);
  }

  private async sendMessage(messageId: number, buffer: Uint8Array): Promise<any> {
    const socket = await this.initWebSocket();
    return new Promise((resolve, reject) => {
      if (socket.readyState === WebSocket.OPEN) {
        socket.send(buffer, (err) => {
          if (err) {
            delete socket.sentMessages[messageId];
            reject(new ConnectionError(
              `CubeStore connection error: ${err.message}`,
              err
            ));
          }
        });
      }

      socket.sentMessages[messageId] = {
        resolve,
        reject,
        buffer
      };
    });
  }

  public async query(query: string, inlineTables: InlineTable[], queryTracingObj?: any): Promise<any[]> {
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
    HttpQuery.startHttpQuery(builder);
    HttpQuery.addQuery(builder, queryOffset);
    if (traceObjOffset) {
      HttpQuery.addTraceObj(builder, traceObjOffset);
    }
    if (inlineTablesOffset) {
      HttpQuery.addInlineTables(builder, inlineTablesOffset);
    }
    const httpQueryOffset = HttpQuery.endHttpQuery(builder);
    const messageId = this.messageCounter++;
    const connectionIdOffset = builder.createString(this.connectionId);
    const message = HttpMessage.createHttpMessage(builder, messageId, HttpCommand.HttpQuery, httpQueryOffset, connectionIdOffset);
    builder.finish(message);
    return this.sendMessage(messageId, builder.asUint8Array());
  }

  public close() {
    if (this.webSocket) {
      this.webSocket.close();
    }
  }
}
