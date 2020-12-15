import type { Handler, Response } from 'express';

type CachedRouterOptions = {
  lifetime: number,
};

interface CachedResponse {
  status: number,
  json: any,
}

export function pipeFromCache(cache: CachedResponse, res: Response) {
  res.status(cache.status)
    .json(cache.json);
}

export function cachedHandler(handler: Handler, options: CachedRouterOptions = { lifetime: 1000 }): Handler {
  let lastCache: CachedResponse = {
    status: 200,
    json: null,
  };
  let lastCacheExpr = new Date(
    new Date().getTime() - options.lifetime,
  );
  let lock = false;

  const queue: Response[] = [];

  return async (req, res, next) => {
    if (lock) {
      queue.push(res);
    } else {
      if (lastCacheExpr.getTime() > new Date().getTime()) {
        pipeFromCache(lastCache, res);

        return;
      }

      lock = true;

      try {
        const responseWrapper: any = {
          ...res,
          status(code: number) {
            res.status(code);

            lastCache.status = code;

            return responseWrapper;
          },
          json(json: any) {
            res.json(json);

            lastCache.json = json;

            return responseWrapper;
          }
        };

        await handler(
          req,
          responseWrapper,
          next
        );
      } catch (e) {
        lastCache = {
          status: 200,
          json: null,
        };
        lastCacheExpr = new Date(
          new Date().getTime() - options.lifetime
        );
      }

      lastCacheExpr = new Date(new Date().getTime() + options.lifetime);
      lock = false;

      let queuedResponse: Response | undefined;

      // eslint-disable-next-line no-cond-assign
      while (queuedResponse = queue.pop()) {
        pipeFromCache(lastCache, queuedResponse);
      }
    }
  };
}
