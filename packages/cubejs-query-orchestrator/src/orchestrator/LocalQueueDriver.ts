import {
  QueueDriverConnectionInterface,
  QueueDriverOptions
} from '@cubejs-backend/base-driver';
import { BaseQueueDriver } from './BaseQueueDriver';
import { LocalQueueDriverConnection, LocalQueueDriverConnectionState } from './LocalQueueDriverConnection';

const sharedState: Record<string, LocalQueueDriverConnectionState> = {};

function getState(prefix: string): LocalQueueDriverConnectionState {
  if (!sharedState[prefix]) {
    sharedState[prefix] = new LocalQueueDriverConnectionState();
  }

  return sharedState[prefix];
}

export class LocalQueueDriver extends BaseQueueDriver {
  private readonly options: QueueDriverOptions;

  public constructor(options: QueueDriverOptions) {
    super(options.processUid || 'local');
    this.options = options;
  }

  public async createConnection(): Promise<QueueDriverConnectionInterface> {
    const state = getState(this.options.redisQueuePrefix);
    return new LocalQueueDriverConnection(this, state, this.options);
  }

  public release(client: QueueDriverConnectionInterface): void {
    client.release();
  }
}
