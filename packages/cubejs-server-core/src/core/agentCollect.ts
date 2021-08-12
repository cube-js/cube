import { getEnv } from '@cubejs-backend/shared';
import fetch from 'node-fetch';
import crypto from 'crypto';
import WebSocket from 'ws';
import zlib from 'zlib';
import { promisify } from 'util';

interface WebSocketExtended extends WebSocket {
  pingTimeout: NodeJS.Timeout
}

interface AgentTransport {
  ready: () => Boolean,
  send: (data: any[]) => Promise<Boolean>
}

type AgentTransportFactory = (endpointUrl: string, logger?: any) => AgentTransport;

const deflate = promisify(zlib.deflate);

const trackEvents = [];
let agentInterval: NodeJS.Timeout = null;
let lastEvent: Date;
let transport: AgentTransport = null;

const createWsTransport: AgentTransportFactory = (endpointUrl, logger) => {
  const callbacks = {};

  let connectionPromiseResolve: Function;
  let connectionPromiseReject: Function;
  const connectionPromise = new Promise((resolve, reject) => {
    connectionPromiseResolve = resolve;
    connectionPromiseReject = reject;
  });

  const clearTransport = () => {
    clearInterval(agentInterval);
    transport = null;
    agentInterval = null;
  };

  const pingInterval = 30 * 1000;
  const heartbeat = function heartbeat(this: WebSocketExtended) {
    connectionPromiseResolve();
    clearTimeout(this.pingTimeout);
    this.pingTimeout = setTimeout(() => {
      this.terminate();
    }, pingInterval + 1000); // +1000 - a conservative assumption of the latency
  };
  
  const wsClient = new WebSocket(endpointUrl);
  
  wsClient.on('open', heartbeat);
  wsClient.on('ping', heartbeat);
  wsClient.on('close', function clear(this: WebSocketExtended) {
    clearTimeout(this.pingTimeout);
    clearTransport();
  });

  wsClient.on('error', e => {
    connectionPromiseReject(e);
    logger('Agent Error', { error: (e.stack || e).toString() });
  });

  wsClient.on('message', (data: WebSocket.Data) => {
    try {
      const { method, params } = JSON.parse(data.toString());
      if (method === 'callback' && callbacks[params.callbackId]) {
        callbacks[params.callbackId](params.result);
      }
    } catch (e) {
      logger('Agent Error', { error: (e.stack || e).toString() });
    }
  });

  return {
    ready: () => wsClient?.readyState === WebSocket.OPEN,
    async send(data: any[]): Promise<Boolean> {
      await connectionPromise;

      const callbackId = crypto.randomBytes(16).toString('hex');
      const message = await deflate(JSON.stringify({
        method: 'agent',
        params: {
          data
        },
        callbackId
      }));

      const result = await new Promise((resolve, reject) => {
        wsClient.send(message);

        const timeout = setTimeout(() => {
          delete callbacks[callbackId];
          reject(new Error('Timeout agent'));
        }, 30 * 1000);

        callbacks[callbackId] = () => {
          clearTimeout(timeout);
          resolve(true);
          delete callbacks[callbackId];
        };
      });

      return !!result;
    }
  };
};

const createHttpTransport: AgentTransportFactory = (endpointUrl) => ({
  ready: () => true,
  async send(data: any[]) {
    const result = await fetch(endpointUrl, {
      method: 'post',
      body: JSON.stringify(data),
      headers: { 'Content-Type': 'application/json' },
    });
    return result.status === 200;
  }
});

export default async (event: Record<string, any>, endpointUrl: string, logger: any) => {
  trackEvents.push({
    ...event,
    id: crypto.randomBytes(16).toString('hex'),
    timestamp: new Date().toJSON()
  });
  lastEvent = new Date();

  if (!transport) {
    transport = /^http/.test(endpointUrl) ?
      createHttpTransport(endpointUrl) :
      createWsTransport(endpointUrl, logger);
  }

  const flush = async (toFlush?: any[], retries?: number) => {
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
    }, getEnv('agentFlushInterval'));
  }
};
