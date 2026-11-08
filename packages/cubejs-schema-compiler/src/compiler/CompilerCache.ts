import { LRUCache } from 'lru-cache';
import moment from 'moment-timezone';
import { QueryCache } from '../adapter/QueryCache';

export class CompilerCache extends QueryCache {
  protected readonly queryCache: LRUCache<string, QueryCache>;

  protected readonly rbacCache: LRUCache<string, any>;

  // Cubes patched with the visibility of one set of applicable access policies. An entry copies
  // the member arrays and the members it hides; the count bound caps it at 64 such copies
  protected readonly visibilityCache: LRUCache<string, any>;

  // Time zones whose UTC offset is baked into generated SQL (interval refresh keys), with the offset
  // they had when first seen. A zone whose offset has moved since joins every query cache key
  protected readonly utcOffsetTimeZones: Map<string, number> = new Map();

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

  public trackUtcOffset(timezone: string): void {
    if (this.utcOffsetTimeZones.has(timezone)) {
      return;
    }
    // A zone with one offset (UTC, an unknown zone resolving to UTC) never changes the key
    const zone = moment.tz.zone(timezone);
    if (zone && new Set(zone.offsets).size > 1) {
      this.utcOffsetTimeZones.set(timezone, moment.tz(timezone).utcOffset());
    }
  }

  // Only the zones whose offset differs from the one they were first seen with, so keys stay as
  // they were until a DST change, and a recompile (a new CompilerCache) resets them
  protected changedUtcOffsets(): Record<string, number> | undefined {
    let offsets: Record<string, number> | undefined;

    for (const [timezone, trackedOffset] of this.utcOffsetTimeZones) {
      const offset = moment.tz(timezone).utcOffset();
      if (offset !== trackedOffset) {
        offsets = offsets || {};
        offsets[timezone] = offset;
      }
    }

    return offsets;
  }

  public getQueryCache(key: unknown): QueryCache {
    // A query cache entry holds SQL with UTC offsets baked in. A process that keeps asking for the
    // same query never lets the entry expire, so a DST change has to start a new one
    const utcOffsets = this.changedUtcOffsets();
    const keyString = utcOffsets ? JSON.stringify({ key, utcOffsets }) : JSON.stringify(key);

    const exist = this.queryCache.get(keyString);
    if (exist) {
      return exist;
    }

    const result = new QueryCache();
    this.queryCache.set(keyString, result);

    return result;
  }
}
