class QueryCache {
  constructor() {
    this.storage = {};
  }

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
