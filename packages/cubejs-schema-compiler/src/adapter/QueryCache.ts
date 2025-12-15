export interface QueryCacheInterface {
  cache(key: any[], fn: Function): any;
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
    const keyString = JSON.stringify(key);

    let result = this.storage.get(keyString);
    if (!result) {
      result = fn();
      this.storage.set(keyString, result);
    }

    return result;
  }
}
