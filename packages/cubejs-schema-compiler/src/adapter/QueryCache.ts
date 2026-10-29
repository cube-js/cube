export class QueryCache {
  protected readonly storage: Record<string, any>;

  public constructor() {
    this.storage = {};
  }

  /**
   * @returns Returns the result of executing a function (Either call a function or take a value from the cache)
   */
  public cache(key: any[], fn: Function): any {
    let keyHolder: Record<string, any> = this.storage;
    const { length } = key;

    for (let i = 0; i < length - 1; i++) {
      if (!keyHolder[key[i]]) {
        keyHolder[key[i]] = {};
      }
      keyHolder = keyHolder[key[i]];
    }
    const lastKey = key[length - 1];
    if (!keyHolder[lastKey]) {
      const value = fn();
      keyHolder[lastKey] = value;
      // A rejected promise isn't kept: the next call computes the value again
      if (value instanceof Promise) {
        value.catch(() => {
          if (keyHolder[lastKey] === value) {
            delete keyHolder[lastKey];
          }
        });
      }
    }
    return keyHolder[lastKey];
  }
}
