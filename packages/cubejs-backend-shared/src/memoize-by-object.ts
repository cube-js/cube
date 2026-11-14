/**
 * Caches what `fn` returns for each object it is called with, for as long as that object lives.
 *
 * Meant for user hooks that take a request context (`contextToAppId`, `contextToGroups`, …):
 * a request reaches them through many code paths, and each hook can be costly user code
 * (a `cube.py` hook crosses into Python on every call), so it should run once per request.
 * Every request builds a new context object, so nothing is shared across requests.
 *
 * Calls with anything but an object are not cached. A promise that rejects is dropped from
 * the cache, so the next call runs `fn` again; a synchronous throw is never cached.
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
