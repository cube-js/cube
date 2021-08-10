import { getEnv } from '@cubejs-backend/shared';

const crypto = require('crypto');
const fetch = require('node-fetch');
const WebSocket = require('ws');

const trackEvents = [];
let agentInterval = null;
let lastEvent;

let transport = null;

const createWsTransport = (endpointUrl, logger) => {
  const callbacks = {};
  let wsClient = null;

  if (!wsClient) {
    const heartbeat = function heartbeat() {
      clearTimeout(this.pingTimeout);
      this.pingTimeout = setTimeout(() => {
        this.terminate();
      }, 30000 + 1000);
    };

    wsClient = new WebSocket(endpointUrl);
    
    wsClient.on('open', heartbeat);
    wsClient.on('ping', heartbeat);
    wsClient.on('close', function clear() {
      clearTimeout(this.pingTimeout);
      transport = null;
    });

    wsClient.on('error', e => {
      logger('Agent Error', { error: (e.stack || e).toString() });
    });

    wsClient.on('message', data => {
      try {
        const { method, params } = JSON.parse(data);
        if (method === 'callback' && callbacks[params.callbackId]) {
          callbacks[params.callbackId](params.result);
        }
      } catch (e) {
        logger('Agent Error', { error: (e.stack || e).toString() });
      }
    });
  }
  
  return {
    agentInterval: 100,
    ready() {
      return wsClient && wsClient.readyState === WebSocket.OPEN;
    },
    async send(data) {
      const result = await new Promise((resolve, reject) => {
        const callbackId = crypto.randomBytes(16).toString('hex');
        wsClient.send(JSON.stringify({
          method: 'agent',
          params: {
            data
          },
          callbackId
        }));

        const timeout = setTimeout(() => {
          delete callbacks[callbackId];
          reject(new Error('Timeout agent'));
        }, 30 * 1000);

        callbacks[callbackId] = res => {
          clearTimeout(timeout);
          resolve(res);
          delete callbacks[callbackId];
        };
      });

      return result;
    }
  };
};

const createHttpTransport = (endpointUrl) => ({
  agentInterval: 1000,
  ready() {
    return true;
  },
  async send(data) {
    const result = await fetch(endpointUrl, {
      method: 'post',
      body: JSON.stringify(data),
      headers: { 'Content-Type': 'application/json' },
    });
    return result.status === 200;
  }
});

export default async (event, endpointUrl, logger) => {
  trackEvents.push({
    ...event,
    id: crypto.randomBytes(16).toString('hex'),
    timestamp: new Date().toJSON()
  });
  lastEvent = new Date();

  if (!transport) {
    transport = /^http/.test(endpointUrl) ?
      createHttpTransport(endpointUrl, logger) :
      createWsTransport(endpointUrl, logger);
  }

  const flush = async (toFlush, retries) => {
    if (transport && transport.ready()) {
      if (!toFlush) toFlush = trackEvents.splice(0, getEnv('agentFrameSize'));
      if (!toFlush.length) return false;
      if (retries == null) retries = 3;

      try {
        const sentAt = new Date().toJSON();
        const result = await transport.send(toFlush.map(r => ({ ...r, sentAt })));
        if (!result && retries > 0) return flush(toFlush, retries - 1);
    
        return true;
      } catch (e) {
        if (retries > 0) return flush(toFlush, retries - 1);
        logger('Agent Error', { error: (e.stack || e).toString() });
      }
    }

    return true;
  };

  if (!agentInterval) {
    agentInterval = setInterval(async () => {
      if (trackEvents.length) {
        await flush();
      } else if (new Date().getTime() - lastEvent.getTime() > 3000) {
        clearInterval(agentInterval);
        agentInterval = null;
      }
    }, transport && transport.agentInterval || 1000);
  }
};
