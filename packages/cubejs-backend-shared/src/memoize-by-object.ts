/**
 * Caches `fn`'s result per object argument for that object's lifetime; non-objects aren't cached,
 * and a rejected promise is evicted so the next call retries.
 */
export function memoizeByObject<T, R>(fn: (arg: T) => R): (arg: T) => R {
  const results = new WeakMap<object, R>();

  return (arg: T): R => {
    if (!arg || typeof arg !== 'object') {
      return fn(arg);
    }

    const key = arg as unknown as object;
    if (results.has(key)) {
      return results.get(key) as R;
    }

    const result = fn(arg);
    results.set(key, result);

    if (result instanceof Promise) {
      result.catch(() => {
        if (results.get(key) === result) {
          results.delete(key);
        }
      });
    }

    return result;
  };
}
