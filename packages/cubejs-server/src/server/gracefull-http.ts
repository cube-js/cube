import * as http from 'http';
import https from 'https';
import { Socket } from 'net';
import { TLSSocket } from 'tls';

export type GracefulHttpServer = (https.Server | http.Server) & {
  stop: (timeout?: number) => Promise<[boolean, Error|undefined]>;
};

export function gracefulHttp(server: http.Server | https.Server): GracefulHttpServer {
  const reqsPerSocket = new Map<Socket, number>();
  let stopped = false;
  let gracefully = true;

  function onConnection(socket: Socket | TLSSocket) {
    reqsPerSocket.set(socket, 0);

    socket.once('close', () => reqsPerSocket.delete(socket));
  }

  function onRequest(req: http.IncomingMessage, res: http.OutgoingMessage) {
    reqsPerSocket.set(req.socket, <number>reqsPerSocket.get(req.socket) + 1);

    res.once('finish', () => {
      const pending = <number>reqsPerSocket.get(req.socket) - 1;
      reqsPerSocket.set(req.socket, pending);

      if (stopped && pending === 0) {
        req.socket.end();
      }
    });
  }

  function destroyAll() {
    gracefully = false;

    // eslint-disable-next-line no-restricted-syntax
    for (const socket of reqsPerSocket.keys()) {
      socket.end();
    }

    // allow request handlers to update state
    setImmediate(() => {
      // eslint-disable-next-line no-restricted-syntax
      for (const socket of reqsPerSocket.keys()) {
        socket.destroy();
      }
    });
  }

  const stop: GracefulHttpServer['stop'] = (timeout) => new Promise((resolve) => {
    // allow request handlers to update state
    setImmediate(() => {
      stopped = true;

      if (timeout) {
        setTimeout(destroyAll, timeout).unref();
      } else {
        destroyAll();
      }

      server.close(e => {
        resolve([gracefully, e]);
      });

      // eslint-disable-next-line no-restricted-syntax
      for (const [socket, req] of reqsPerSocket.entries()) {
        if (req === 0) {
          socket.end();
        }
      }
    });
  });

  if (server instanceof https.Server) {
    server.on('secureConnection', onConnection);
  } else {
    server.on('connection', onConnection);
  }

  server.on('request', onRequest);
  (<GracefulHttpServer>server).stop = stop;

  return <GracefulHttpServer>server;
}
