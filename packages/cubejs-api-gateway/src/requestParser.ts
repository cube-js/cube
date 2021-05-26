import uuid from 'uuid/v4';
import type { Request, Response } from 'express';

interface RequestParserResult {
  path: string;
  method: string;
  status: number;
  ip: string;
  time: string;
  contentLength?: string
  contentType?: string
}

export function getRequestIdFromRequest(req: Request): string {
  return req.get('x-request-id') || req.get('traceparent') || uuid();
}

export function requestParser(req: Request, res: Response) {
  const path = req.originalUrl || req.path || req.url;
  const httpHeader = req.header && req.header('x-forwarded-for');
  const ip: any = req.ip || httpHeader || req.connection.remoteAddress;

  const requestData: RequestParserResult = {
    path,
    method: req.method,
    status: res.statusCode,
    ip,
    time: (new Date()).toISOString(),
  };

  if (res.get) {
    requestData.contentLength = res.get('content-length');
    requestData.contentType = res.get('content-type');
  }

  return requestData;
}
