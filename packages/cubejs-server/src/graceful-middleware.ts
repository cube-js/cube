import { RequestHandler } from 'express';
import { ServerStatusHandler } from './server-status';

export function gracefulMiddleware(status: ServerStatusHandler): RequestHandler {
  // eslint-disable-next-line consistent-return
  return (req, res, next) => {
    if (status.isUp()) {
      return next();
    }

    res.status(500)
      .header('Connection', 'close')
      .json({
        message: 'Server unavailable, no new requests accepted during shutdown',
      });
  };
}
