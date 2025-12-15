import { LRUCache } from 'lru-cache';
import { QueryCacheInterface, QueryCache } from '../adapter/QueryCache';

export class CompilerCache implements QueryCacheInterface {
  protected readonly queryCache: LRUCache<string, QueryCache>;

  protected readonly rbacCache: LRUCache<string, any>;

  protected readonly cacheStorage: LRUCache<string, any>;

  public constructor({ maxQueryCacheSize, maxQueryCacheAge }) {
    this.queryCache = new LRUCache({
      max: maxQueryCacheSize || 10000,
      ttl: (maxQueryCacheAge * 1000) || 1000 * 60 * 10,
      updateAgeOnGet: true
    });

    this.rbacCache = new LRUCache({
      max: 10000,
      ttl: 1000 * 60 * 5, // 5 minutes
    });

    this.cacheStorage = new LRUCache({
      max: maxQueryCacheSize || 10000,
      ttl: (maxQueryCacheAge * 1000) || 1000 * 60 * 10,
      updateAgeOnGet: true
    });
  }

  public cache(key: any[], fn: Function): any {
    const keyString = JSON.stringify(key);

    let result = this.cacheStorage.get(keyString);
    if (!result) {
      result = fn();
      this.cacheStorage.set(keyString, result);
    }

    return result;
  }

  public getRbacCacheInstance(): LRUCache<string, any> {
    return this.rbacCache;
  }

  public getQueryCache(key: unknown): QueryCacheInterface {
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
