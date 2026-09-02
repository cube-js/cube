import crypto from 'crypto';

import { getProcessUid } from '@cubejs-backend/shared';
import { QueryKey, QueryKeyHash } from '@cubejs-backend/base-driver';
import { CacheKey, LocalRefreshKeyDescriptor } from './QueryCache';

export { extractRequestUUID } from '@cubejs-backend/shared';

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
 * `nowMs` is not floored to whole seconds first: for an integer `interval >= 1` the
 * fractional part cannot cross a boundary, which is also why the fractional seconds of
 * `EXTRACT(EPOCH FROM NOW())` never mattered on the SQL path.
 *
 * The key is a string because `contentVersion` and the query cache `renewalKey` hash it
 * through `JSON.stringify`: `2980310` and `"2980310"` are different keys, so a number here
 * would rebuild every pre-aggregation the first time the flag is switched on. A string is
 * also what the SQL path yields — an `every` key runs against Cube Store whenever an
 * external store is configured, and Cube Store carries every column as a string.
 */
export function evaluateLocalRefreshKey(
  descriptor: LocalRefreshKeyDescriptor,
  nowMs: number = Date.now(),
): [{ refresh_key: string }] {
  const { utcOffset, interval, dayOffset } = descriptor;

  return [{ refresh_key: String(Math.floor((utcOffset + nowMs / 1000 - dayOffset) / interval)) }];
}

export function isValidLocalRefreshKey(descriptor?: LocalRefreshKeyDescriptor): boolean {
  return !!descriptor &&
    Number.isFinite(descriptor.interval) && descriptor.interval > 0 &&
    Number.isFinite(descriptor.utcOffset) &&
    Number.isFinite(descriptor.dayOffset);
}
