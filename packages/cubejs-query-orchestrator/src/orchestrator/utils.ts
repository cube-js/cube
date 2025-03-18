import crypto from 'crypto';

import { getProcessUid } from '@cubejs-backend/shared';
import { QueryKey, QueryKeyHash } from '@cubejs-backend/base-driver';
import { CacheKey } from './QueryCache';

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
