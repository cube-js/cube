import { getCacheHash } from './utils';

export abstract class BaseQueueDriver {
  public redisHash(queryKey) {
    return getCacheHash(queryKey);
  }
}
