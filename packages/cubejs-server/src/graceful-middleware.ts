import { RequestHandler } from 'express';
import { ServerStatusHandler } from './server-status';

export function gracefulMiddleware(status: ServerStatusHandler, timeout: number): RequestHandler {
  // eslint-disable-next-line consistent-return
  return (req, res, next) => {
    if (status.isUp()) {
      return next();
    }

    // https://tools.ietf.org/html/rfc7231#section-6.6.4
    res.status(503)
      .header('Connection', 'close')
      // Timeout can be bigger then 5 sec, let's allow client to retry in 5sec
      .header('Retry-After', `${Math.max(timeout, 5)}`)
      .json({
        message: 'Server unavailable, no new requests accepted during shutdown',
      });
  };
}
