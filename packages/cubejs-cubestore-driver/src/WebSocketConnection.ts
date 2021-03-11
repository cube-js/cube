import WebSocket from 'ws';
import { flatbuffers } from 'flatbuffers';
import { ConnectionConfig } from './types';
import { HttpCommand, HttpError, HttpMessage, HttpQuery, HttpResultSet } from '../codegen/HttpMessage';

export class WebSocketConnection {
  protected readonly config: any;

  protected messageCounter: number;

  protected webSocket: any;

  public constructor(config?: Partial<ConnectionConfig>) {
    this.config = config;
    this.messageCounter = 1;
  }

  protected async initWebSocket() {
    if (!this.webSocket) {
      const webSocket: any = new WebSocket(
        this.config.url ||
        `ws://${this.config.host || 'localhost'}:${this.config.port || '3030'}/ws`
      );
      webSocket.readyPromise = new Promise<WebSocket>((resolve, reject) => {
        webSocket.lastHeartBeat = new Date();
        const pingInterval = setInterval(() => {
          if (webSocket.readyState !== WebSocket.CLOSED) {
            webSocket.ping();
          }
          if (new Date().getTime() - webSocket.lastHeartBeat.getTime() > 30000) {
            webSocket.close();
          }
        }, 5000);
        webSocket.sendAsync = async (message) => new Promise<void>((resolveSend, rejectSend) => {
          webSocket.send(message, (err) => {
            if (err) {
              rejectSend(err);
            } else {
              resolveSend();
            }
          });
        });
        webSocket.on('open', () => resolve(webSocket));
        webSocket.on('error', (err) => {
          reject(err);
          if (webSocket === this.webSocket) {
            this.webSocket = undefined;
          }
        });
        webSocket.on('pong', () => {
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
            }, 1000);
          }

          if (webSocket === this.webSocket) {
            this.webSocket = undefined;
          }
        });
        webSocket.on('message', (msg) => {
          const buf = new flatbuffers.ByteBuffer(msg);
          const httpMessage = HttpMessage.getRootAsHttpMessage(buf);
          const resolvers = webSocket.sentMessages[httpMessage.messageId()];
          delete webSocket.sentMessages[httpMessage.messageId()];
          if (!resolvers) {
            throw new Error(`Cube Store missed message id: ${httpMessage.messageId()}`); // logging
          }
          const commandType = httpMessage.commandType();
          if (commandType === HttpCommand.HttpError) {
            resolvers.reject(new Error(`${httpMessage.command(new HttpError())?.error()}`));
          } else if (commandType === HttpCommand.HttpResultSet) {
            const resultSet = httpMessage.command(new HttpResultSet());
            if (!resultSet) {
              resolvers.reject(new Error('Empty resultSet'));
              return;
            }
            const columnsLen = resultSet.columnsLength();
            const columns: Array<string> = [];
            for (let i = 0; i < columnsLen; i++) {
              const columnName = resultSet.columns(i);
              if (!columnName) {
                resolvers.reject(new Error('Column name is not defined'));
                return;
              }
              columns.push(columnName);
            }
            const rowLen = resultSet.rowsLength();
            const result: any[] = [];
            for (let i = 0; i < rowLen; i++) {
              const row = resultSet.rows(i);
              if (!row) {
                resolvers.reject(new Error('Null row'));
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
            resolvers.reject(new Error('Unsupported command'));
          }
        });
      });
      webSocket.sentMessages = {};
      this.webSocket = webSocket;
    }
    return this.webSocket.readyPromise;
  }

  private async sendMessage(messageId: number, buffer: Uint8Array): Promise<any> {
    const socket = await this.initWebSocket();
    return new Promise((resolve, reject) => {
      socket.send(buffer, (err) => {
        if (err) {
          delete socket.sentMessages[messageId];
          reject(err);
        }
      });
      socket.sentMessages[messageId] = {
        resolve,
        reject,
        buffer
      };
    });
  }

  public async query(query: string): Promise<any[]> {
    const builder = new flatbuffers.Builder(1024);
    const queryOffset = builder.createString(query);
    const httpQueryOffset = HttpQuery.createHttpQuery(builder, queryOffset);
    const messageId = this.messageCounter++;
    const message = HttpMessage.createHttpMessage(builder, messageId, HttpCommand.HttpQuery, httpQueryOffset);
    builder.finish(message);
    return this.sendMessage(messageId, builder.asUint8Array());
  }

  public close() {
    if (this.webSocket) {
      this.webSocket.close();
    }
  }
}
