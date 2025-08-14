import crypto from 'crypto';

/**
 * Decorator version of asyncDebounce for methods and functions.
 * Caches promises by method arguments to prevent concurrent execution of the same operation.
 */
export function AsyncDebounce() {
  return function (
    target: any,
    propertyKey: string | symbol,
    descriptor: PropertyDescriptor
  ) {
    const originalMethod = descriptor.value;
    const cache = new Map<string, Promise<any>>();

    descriptor.value = async function (...args: any[]) {
      const key = crypto.createHash('md5')
        .update(args.map((v) => JSON.stringify(v)).join(','))
        .digest('hex');

      if (cache.has(key)) {
        return cache.get(key);
      }

      try {
        const promise = originalMethod.apply(this, args);
        cache.set(key, promise);
        return await promise;
      } finally {
        cache.delete(key);
      }
    };

    return descriptor;
  };
}
