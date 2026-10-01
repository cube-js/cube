import { v4 as uuidv4 } from 'uuid';
import { isValidRequestId, REQUEST_ID_MAX_LENGTH } from '@cubejs-backend/shared';

import { UserError } from './user-error';
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

function parseRequestIdHeader(value: string): string {
  // A repeated header (e.g. one more added by a proxy) arrives joined with ", ";
  // commas are never valid in an id, so the first value is the client's
  const requestId = value.split(',')[0].trim();
  if (!isValidRequestId(requestId)) {
    throw new UserError(
      `Request id must be at most ${REQUEST_ID_MAX_LENGTH} characters from A-Z, a-z, 0-9, '+', '/', '=', '.', '_', ':' and '-'`
    );
  }

  return requestId;
}

export function getRequestIdFromRequest(req: Request): string {
  const xRequestId = req.get('x-request-id');
  if (xRequestId) {
    return parseRequestIdHeader(xRequestId);
  }

  const traceparent = req.get('traceparent');
  if (traceparent) {
    return parseRequestIdHeader(traceparent);
  }

  return `${uuidv4()}-span-1`;
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
