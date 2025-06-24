import { LRUCache } from 'lru-cache';
import { QueryCache } from '../adapter/QueryCache';

export class CompilerCache extends QueryCache {
  protected readonly queryCache: LRUCache<string, QueryCache>;

  protected readonly rbacCache: LRUCache<string, any>;

  public constructor({ maxQueryCacheSize, maxQueryCacheAge }) {
    super();

    this.queryCache = new LRUCache({
      max: maxQueryCacheSize || 10000,
      ttl: (maxQueryCacheAge * 1000) || 1000 * 60 * 10,
      updateAgeOnGet: true
    });

    this.rbacCache = new LRUCache({
      max: 10000,
      ttl: 1000 * 60 * 5, // 5 minutes
    });
  }

  public getRbacCacheInstance(): LRUCache<string, any> {
    return this.rbacCache;
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
