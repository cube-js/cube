import {
  QueryKey,
  QueryKeyHash,
  QueueDriverConnectionInterface,
  QueueDriverInterface,
} from '@cubejs-backend/base-driver';
import { getCacheHash } from './utils';

export abstract class BaseQueueDriver implements QueueDriverInterface {
  public constructor(protected processUid: string) {
  }

  private counter = 0;

  public generateQueueId = (): number => this.counter++;

  public redisHash(queryKey: QueryKey): QueryKeyHash {
    return getCacheHash(queryKey, this.processUid);
  }

  abstract createConnection(): Promise<QueueDriverConnectionInterface>;

  abstract release(connection: QueueDriverConnectionInterface): void;
}
