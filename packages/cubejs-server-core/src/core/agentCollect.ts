import { getEnv } from '@cubejs-backend/shared';
import http from 'http';
import https from 'https';
import { HttpsProxyAgent } from 'https-proxy-agent';
import { HttpProxyAgent } from 'http-proxy-agent';
import fetch from 'node-fetch';
import crypto from 'crypto';
import WebSocket from 'ws';
import zlib from 'zlib';
import { promisify } from 'util';

const deflate = promisify(zlib.deflate);
interface AgentTransport {
  ready: () => Boolean,
  send: (data: any[]) => Promise<Boolean>
}
class WebSocketTransport implements AgentTransport {
  private pingTimeout: NodeJS.Timeout;

  public readonly connectionPromise: Promise<Boolean>;

  public readonly wsClient: WebSocket;

  private readonly callbacks = {};

  public constructor(
    private endpointUrl: string,
    private logger,
    private onClose: Function
  ) {
    let connectionPromiseResolve: Function;
    let connectionPromiseReject: Function;
    this.connectionPromise = new Promise((resolve, reject) => {
      connectionPromiseResolve = resolve;
      connectionPromiseReject = reject;
    });

    this.wsClient = new WebSocket(this.endpointUrl);

    const pingInterval = 30 * 1000;
    const heartbeat = () => {
      connectionPromiseResolve();
      clearTimeout(this.pingTimeout);
      this.pingTimeout = setTimeout(() => {
        this.wsClient.terminate();
      }, pingInterval + 1000); // +1000 - a conservative assumption of the latency
    };

    this.wsClient.on('open', heartbeat);
    this.wsClient.on('ping', heartbeat);
    this.wsClient.on('close', () => {
      clearTimeout(this.pingTimeout);
      this.onClose();
    });

    this.wsClient.on('error', e => {
      connectionPromiseReject(e);
      this.logger('Agent Error', { error: (e.stack || e).toString() });
    });

    this.wsClient.on('message', (data: WebSocket.Data) => {
      try {
        const { method, params } = JSON.parse(data.toString());
        if (method === 'callback' && this.callbacks[params.callbackId]) {
          this.callbacks[params.callbackId](params.result);
        }
      } catch (e: any) {
        this.logger('Agent Error', { error: (e.stack || e).toString() });
      }
    });
  }

  public ready() {
    return this?.wsClient?.readyState === WebSocket.OPEN;
  }

  public async send(data) {
    await this.connectionPromise;

    const callbackId = crypto.randomBytes(16).toString('hex');
    const message = await deflate(JSON.stringify({
      method: 'agent',
      params: {
        data
      },
      callbackId
    }));

    const result = await new Promise((resolve, reject) => {
      this.wsClient.send(message);

      const timeout = setTimeout(() => {
        delete this.callbacks[callbackId];
        reject(new Error('Timeout agent'));
      }, 30 * 1000);

      this.callbacks[callbackId] = () => {
        clearTimeout(timeout);
        resolve(true);
        delete this.callbacks[callbackId];
      };
    });

    return !!result;
  }
}

function isOnNoProxyList(url: string): boolean {
  const noProxy = process.env.NO_PROXY || process.env.no_proxy;
  if (!noProxy) {
    return false;
  }

  const parsedUrl = new URL(url);
  const { hostname } = parsedUrl;
  const noProxyList = noProxy.split(',').map((entry) => entry.trim());

  return noProxyList.some((entry) => {
    if (entry === '*') {
      return true;
    }
    if (entry.startsWith('.')) {
      return hostname.endsWith(entry);
    }

    return hostname === entry;
  });
}

class HttpTransport implements AgentTransport {
  private agent: http.Agent | https.Agent | HttpProxyAgent<string> | HttpsProxyAgent<string>;

  public constructor(
    private readonly endpointUrl: string
  ) {
    const agentParams = {
      keepAlive: true,
      maxSockets: getEnv('agentMaxSockets')
    };
    if (!isOnNoProxyList(endpointUrl) && (process.env.http_proxy || process.env.https_proxy)) {
      this.agent = endpointUrl.startsWith('https') ?
        new HttpsProxyAgent(process.env.https_proxy, agentParams) :
        new HttpProxyAgent(process.env.http_proxy, agentParams);
    } else {
      this.agent = endpointUrl.startsWith('https') ? new https.Agent(agentParams) : new http.Agent(agentParams);
    }
  }

  public ready() {
    return true;
  }

  public async send(data: any[]) {
    const result = await fetch(this.endpointUrl, {
      agent: this.agent,
      method: 'post',
      body: await deflate(JSON.stringify(data)),
      headers: {
        'Content-Type': 'application/json',
        'Content-Encoding': 'deflate'
      },
    });
    return result.status === 200;
  }
}

const trackEvents = [];
let agentInterval: NodeJS.Timeout = null;
let lastEvent: Date;
let transport: AgentTransport = null;

const clearTransport = () => {
  clearInterval(agentInterval);
  transport = null;
  agentInterval = null;
};

export default async (event: Record<string, any>, endpointUrl: string, logger: any) => {
  trackEvents.push({
    ...event,
    id: crypto.randomBytes(16).toString('hex'),
    timestamp: new Date().toJSON(),
    instanceId: getEnv('instanceId'),
  });
  lastEvent = new Date();

  const flush = async (toFlush: any[], retries?: number) => {
    if (!transport) {
      transport = /^http/.test(endpointUrl) ?
        new HttpTransport(endpointUrl) :
        new WebSocketTransport(endpointUrl, logger, clearTransport);
    }
    if (!toFlush.length) return false;
    if (retries == null) retries = 3;

    try {
      const sentAt = new Date().toJSON();
      const result = await transport.send(toFlush.map(r => ({ ...r, sentAt })));
      if (!result && retries > 0) return flush(toFlush, retries - 1);

      return true;
    } catch (e: any) {
      if (retries > 0) return flush(toFlush, retries - 1);
      logger('Agent Error', { error: (e.stack || e).toString() });
    }

    return true;
  };

  const flushAllByChunks = async () => {
    const agentFrameSize: number = getEnv('agentFrameSize');
    const toFlushArray = [];
    while (trackEvents.length > 0) {
      toFlushArray.push(trackEvents.splice(0, agentFrameSize));
    }
    await Promise.all(toFlushArray.map(toFlush => flush(toFlush)));
  };

  if (!agentInterval) {
    agentInterval = setInterval(async () => {
      if (trackEvents.length) {
        await flushAllByChunks();
      } else if (new Date().getTime() - lastEvent.getTime() > 3000) {
        clearInterval(agentInterval);
        agentInterval = null;
      }
    }, getEnv('agentFlushInterval'));
  }
};
