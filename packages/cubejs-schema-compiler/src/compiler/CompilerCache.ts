import { LRUCache } from 'lru-cache';
import { QueryCache } from '../adapter/QueryCache';

export class CompilerCache extends QueryCache {
  protected readonly queryCache: LRUCache<string, QueryCache>;

  protected readonly rbacCache: LRUCache<string, any>;

  // Members patched with the visibility of one set of applicable access policies: few sets,
  // each a copy of every member, so the cache is small
  protected readonly visibilityCache: LRUCache<string, any>;

  public constructor(
    { maxQueryCacheSize, maxQueryCacheAge }: { maxQueryCacheSize?: number; maxQueryCacheAge?: number }
  ) {
    super();

    this.queryCache = new LRUCache({
      max: maxQueryCacheSize || 10000,
      ttl: ((maxQueryCacheAge ?? 0) * 1000) || 1000 * 60 * 10,
      updateAgeOnGet: true
    });

    this.rbacCache = new LRUCache({
      max: 10000,
      ttl: 1000 * 60 * 5, // 5 minutes
    });

    this.visibilityCache = new LRUCache({
      max: 64,
      ttl: 1000 * 60 * 5, // 5 minutes, renewed while the set of policies is in use
      updateAgeOnGet: true,
    });
  }

  public getRbacCacheInstance(): LRUCache<string, any> {
    return this.rbacCache;
  }

  public getVisibilityCacheInstance(): LRUCache<string, any> {
    return this.visibilityCache;
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
