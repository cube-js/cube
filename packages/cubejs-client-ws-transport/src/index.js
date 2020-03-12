import WebSocket from 'isomorphic-ws';

class WebSocketTransportResult {
  constructor({ status, message }) {
    this.status = status;
    this.result = message;
  }

  async json() {
    return this.result;
  }
}

class WebSocketTransport {
  constructor({ authorization, apiUrl, hearBeatInterval }) {
    this.authorization = authorization;
    this.apiUrl = apiUrl;
    this.messageCounter = 1;
    this.messageIdToSubscription = {};
    this.messageQueue = [];
    this.hearBeatInterval = hearBeatInterval || 60;
  }

  set authorization(token) {
    this.token = token;
    if (this.ws) {
      this.ws.close();
    }
  }

  get authorization() {
    return this.token;
  }

  initSocket() {
    if (this.ws) {
      return this.ws.initPromise;
    }

    const ws = new WebSocket(this.apiUrl);

    ws.messageIdSent = {};

    ws.sendMessage = (message) => {
      if (!message.messageId || message.messageId && !ws.messageIdSent[message.messageId]) {
        ws.send(JSON.stringify(message));
        ws.messageIdSent[message.messageId] = true;
      }
    };

    ws.sendQueue = () => {
      this.messageQueue.forEach(message => ws.sendMessage(message));
      this.messageQueue = [];
    };

    ws.reconcile = () => {
      if (new Date().getTime() - ws.lastMessageTimestamp.getTime() > 4 * this.hearBeatInterval * 1000) {
        ws.close();
      } else {
        Object.keys(this.messageIdToSubscription).forEach(messageId => {
          ws.sendMessage(this.messageIdToSubscription[messageId].message);
        });
      }
    };

    ws.lastMessageTimestamp = new Date();

    ws.initPromise = new Promise(resolve => {
      ws.onopen = () => {
        ws.sendMessage({ authorization: this.authorization });
      };

      ws.onmessage = (message) => {
        ws.lastMessageTimestamp = new Date();
        message = JSON.parse(message.data);
        if (message.handshake) {
          ws.reconcile();
          ws.reconcileTimer = setInterval(() => {
            ws.messageIdSent = {};
            ws.reconcile();
          }, this.hearBeatInterval * 1000);
          resolve();
        }
        if (this.messageIdToSubscription[message.messageId]) {
          this.messageIdToSubscription[message.messageId].callback(new WebSocketTransportResult(message));
        }
        ws.sendQueue();
      };

      ws.onclose = () => {
        if (ws && ws.readyState !== WebSocket.CLOSED && ws.readyState !== WebSocket.CLOSING) {
          ws.close();
        }
        if (ws.reconcileTimer) {
          clearInterval(ws.reconcileTimer);
          ws.reconcileTimer = null;
        }
        if (this.ws === ws) {
          this.ws = null;
          if (Object.keys(this.messageIdToSubscription).length) {
            this.initSocket();
          }
        }
      };

      ws.onerror = ws.onclose;
    });

    this.ws = ws;

    return this.ws.initPromise;
  }

  sendMessage(message) {
    if (message.unsubscribe && this.messageQueue.find(m => m.messageId === message.unsubscribe)) {
      this.messageQueue = this.messageQueue.filter(m => m.messageId !== message.unsubscribe);
    } else {
      this.messageQueue.push(message);
    }
    setTimeout(async () => {
      await this.initSocket();
      this.ws.sendQueue();
    }, 100);
  }

  request(method, { baseRequestId, ...params }) {
    const message = {
      messageId: this.messageCounter++,
      method,
      params
    };

    const pendingResults = [];
    let nextMessage = null;

    const runNextMessage = () => {
      if (nextMessage) {
        nextMessage(pendingResults.pop());
        nextMessage = null;
      }
    };

    this.messageIdToSubscription[message.messageId] = {
      message,
      callback: (result) => {
        pendingResults.push(result);
        runNextMessage();
      }
    };

    const transport = this;

    let spanCounter = 1;

    return {
      async subscribe(callback) {
        transport.sendMessage({
          requestId: baseRequestId && `${baseRequestId}-span-${spanCounter++}`,
          ...message
        });
        const result = await new Promise((resolve) => {
          nextMessage = resolve;
          if (pendingResults.length) {
            runNextMessage();
          }
        });
        return callback(result, () => this.subscribe(callback));
      },
      async unsubscribe() {
        transport.sendMessage({ unsubscribe: message.messageId });
        delete transport.messageIdToSubscription[message.messageId];
      }
    };
  }
}

export default WebSocketTransport;
