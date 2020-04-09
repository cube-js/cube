const LRUCache = require('lru-cache');
const QueryCache = require('../adapter/QueryCache');

class CompilerCache extends QueryCache {
  constructor({ maxQueryCacheSize, maxQueryCacheAge }) {
    super();
    this.queryCache = new LRUCache({
      max: maxQueryCacheSize || 10000,
      maxAge: (maxQueryCacheAge * 1000) || 1000 * 60 * 10,
      updateAgeOnGet: true
    });
  }

  getQueryCache(key) {
    const keyString = JSON.stringify(key);
    if (!this.queryCache.get(keyString)) {
      this.queryCache.set(keyString, new QueryCache());
    }
    return this.queryCache.get(keyString);
  }
}

module.exports = CompilerCache;
