import { LocalQueueDriverConnectionInterface, QueueDriverInterface } from '@cubejs-backend/base-driver';
import { getCacheHash } from './utils';

export abstract class BaseQueueDriver implements QueueDriverInterface {
  public redisHash(queryKey) {
    return getCacheHash(queryKey);
  }

  abstract createConnection(): Promise<LocalQueueDriverConnectionInterface>;

  abstract release(connection: LocalQueueDriverConnectionInterface): Promise<void>;
}
