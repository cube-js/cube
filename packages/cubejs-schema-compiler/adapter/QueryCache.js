class QueryCache {
  constructor() {
    this.storage = {};
  }

  /**
   * @param {Array} key
   * @param {Function} fn
   * @returns Returns the result of executing a function (Either call a function or take a value from the cache)
   */
  cache(key, fn) {
    let keyHolder = this.storage;
    const { length } = key;
    for (let i = 0; i < length - 1; i++) {
      if (!keyHolder[key[i]]) {
        keyHolder[key[i]] = {};
      }
      keyHolder = keyHolder[key[i]];
    }
    const lastKey = key[length - 1];
    if (!keyHolder[lastKey]) {
      keyHolder[lastKey] = fn();
    }
    return keyHolder[lastKey];
  }
}

module.exports = QueryCache;
