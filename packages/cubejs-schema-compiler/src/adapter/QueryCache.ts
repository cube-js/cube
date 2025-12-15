export interface QueryCacheInterface {
  cache(key: any[], fn: Function): any;
}

/**
 * Uses string concatenation (+=) instead of JSON.stringify for performance:
 * - V8 optimizes += with ConsString (tree of string segments)
 * - JSON.stringify builds a new flat string from scratch each time
 * - For strings/numbers (common case), ~3x faster than JSON.stringify
 * - Only falls back to JSON.stringify for objects/arrays
 */
export function computeCacheKey(key: unknown): string {
  if (Array.isArray(key)) {
    let result = '';

    for (let i = 0; i < key.length; i++) {
      if (i > 0) {
        result += ':';
      }

      if (typeof key[i] === 'string' || typeof key[i] === 'number') {
        result += key[i];
      } else {
        result += JSON.stringify(key[i]);
      }
    }

    return result;
  }

  if (typeof key === 'string') {
    return key;
  }

  return JSON.stringify(key);
}

export class QueryCache implements QueryCacheInterface {
  private readonly storage: Map<string, any>;

  public constructor() {
    this.storage = new Map();
  }

  /**
   * @returns Returns the result of executing a function (Either call a function or take a value from the cache)
   */
  public cache(key: any[], fn: Function): any {
    const keyString = computeCacheKey(key);

    let result = this.storage.get(keyString);
    if (!result) {
      result = fn();
      this.storage.set(keyString, result);
    }

    return result;
  }
}
