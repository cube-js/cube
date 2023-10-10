import LRUCache from 'lru-cache';
import { QueryCache } from '../adapter/QueryCache';

export class CompilerCache extends QueryCache {
  protected readonly queryCache: LRUCache<string, QueryCache>;

  public constructor({ maxQueryCacheSize, maxQueryCacheAge }) {
    super();

    this.queryCache = new LRUCache({
      max: maxQueryCacheSize || 10000,
      maxAge: (maxQueryCacheAge * 1000) || 1000 * 60 * 10,
      updateAgeOnGet: true
    });
  }

  public getQueryCache(key: unknown): QueryCache {
    const keyString = JSON.stringify(key);

    const exist = this.queryCache.get(keyString);
    if (exist) {
      return exist;
    }

    const result = new QueryCache();
    this.queryCache.set(keyString, result);

    return result;
  }
}
