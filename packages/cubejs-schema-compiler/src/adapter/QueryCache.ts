export class QueryCache {
  // Maps rather than objects, so no key (`__proto__` included) reaches a prototype.
  // Keys are stringified, as property names were.
  protected readonly storage: Map<string, any>;

  public constructor() {
    this.storage = new Map();
  }

  /**
   * @returns Returns the result of executing a function (Either call a function or take a value from the cache)
   */
  public cache(key: any[], fn: Function): any {
    let keyHolder: Map<string, any> = this.storage;
    const { length } = key;

    for (let i = 0; i < length - 1; i++) {
      const holderKey = String(key[i]);
      if (!keyHolder.get(holderKey)) {
        keyHolder.set(holderKey, new Map());
      }
      keyHolder = keyHolder.get(holderKey);
    }
    const lastKey = String(key[length - 1]);
    if (!keyHolder.get(lastKey)) {
      const value = fn();
      keyHolder.set(lastKey, value);
      // A rejected promise isn't kept: the next call computes the value again
      if (value instanceof Promise) {
        const holder = keyHolder;
        value.catch(() => {
          if (holder.get(lastKey) === value) {
            holder.delete(lastKey);
          }
        });
      }
    }
    return keyHolder.get(lastKey);
  }
}
