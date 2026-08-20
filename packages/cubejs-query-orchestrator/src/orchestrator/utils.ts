import crypto from 'crypto';

import { getProcessUid } from '@cubejs-backend/shared';
import { QueryKey, QueryKeyHash } from '@cubejs-backend/base-driver';
import { CacheKey, LocalRefreshKeyDescriptor } from './QueryCache';

/**
 * Unique process ID regexp.
 */
export const processUidRE = /^[0-9a-f]{8}\b-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-\b[0-9a-f]{12}$/;

/**
 * Returns query hash by specified `queryKey`.
 */
export function getCacheHash(queryKey: QueryKey | CacheKey, processUid?: string): QueryKeyHash {
  processUid = processUid || getProcessUid();
  if (typeof queryKey === 'string' && queryKey.length < 256) {
    return queryKey as any;
  }

  if (typeof queryKey === 'object' && 'persistent' in queryKey && queryKey.persistent) {
    return `${crypto
      .createHash('md5')
      .update(JSON.stringify(queryKey))
      .digest('hex')
    }@${processUid}` as any;
  } else {
    return crypto
      .createHash('md5')
      .update(JSON.stringify(queryKey))
      .digest('hex') as any;
  }
}

/**
 * Evaluates an `every` based refreshKey from the local clock, producing the same
 * row shape the equivalent `SELECT FLOOR(...) as refresh_key` would return.
 *
 * `nowMs` is not floored to whole seconds first: for integer `x`, fractional
 * `f` in [0, 1) and `interval >= 1`, floor((x + f) / interval) === floor(x / interval),
 * which is also why the fractional seconds of `EXTRACT(EPOCH FROM NOW())` never
 * mattered on the SQL path.
 */
export function evaluateLocalRefreshKey(
  descriptor: LocalRefreshKeyDescriptor,
  nowMs: number = Date.now(),
): [{ refresh_key: number }] {
  const { utcOffset, interval, dayOffset } = descriptor;

  return [{ refresh_key: Math.floor((utcOffset + nowMs / 1000 - dayOffset) / interval) }];
}

/**
 * A malformed descriptor must fall back to the SQL path rather than produce a
 * garbage refresh key, which would silently invalidate everything downstream.
 */
export function isValidLocalRefreshKey(descriptor?: LocalRefreshKeyDescriptor): boolean {
  return !!descriptor &&
    Number.isFinite(descriptor.interval) && descriptor.interval > 0 &&
    Number.isFinite(descriptor.utcOffset) &&
    Number.isFinite(descriptor.dayOffset);
}

/**
 * Extracts the UUID prefix from a request ID by stripping the `-span-N` suffix.
 */
export function extractRequestUUID(requestId: string): string {
  const idx = requestId.lastIndexOf('-span-');
  return idx !== -1 ? requestId.substring(0, idx) : requestId;
}
