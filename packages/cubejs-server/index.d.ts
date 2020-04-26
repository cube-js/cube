import { CreateOptions as CoreCreateOptions } from "@cubejs-backend/server-core";
import express from 'express';
import https from 'https';
import http from 'http';

export interface CreateOptions extends CoreCreateOptions {
  webSockets?: boolean;
}

declare class CubejsServer {
  constructor(options: CreateOptions);

  listen(options?: https.ServerOptions | http.ServerOptions): Promise<{ server: http.Server | https.Server, version: string, port: number, tlsPort?: number, app: express.Application }>;
  close(): Promise<void>;
  testConnections(): Promise<void>;
}

export default CubejsServer;
