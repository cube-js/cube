import { CreateOptions as CoreCreateOptions, CreateOptions } from "@cubejs-backend/server-core";
import express from 'express';
import https from 'https';
import http from 'http';

export interface CreateOptions extends CoreCreateOptions {
  webSockets?: boolean;
}

declare class CubejsServer {
  constructor(options: CreateOptions);

  listen(): Promise<{ server: http.Server | https.Server, port: number, tlsPort?: number, app: express.Application }>;
  close(): Promise<void>;
  testConnections(): Promise<void>;
}

export = CubejsServer;
