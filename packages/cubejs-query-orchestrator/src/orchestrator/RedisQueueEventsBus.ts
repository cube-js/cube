import { RedisPool } from './RedisPool';
import { BaseQueueEventsBus } from './BaseQueueEventsBus';

export class RedisQueueEventsBus extends BaseQueueEventsBus {
  protected readonly redisPool: RedisPool|undefined;

  protected readonly subscribers: Record<string, any>;

  public eventsChannel: string;

  public constructor(options) {
    super();
    this.redisPool = options.redisPool;
    this.eventsChannel = 'queue:events';
    this.initSubscriber();
  }

  public async initSubscriber() {
    const redisClientSubscriber = await this.redisPool.getClient();
    
    redisClientSubscriber.subscribe('queue:events', (err) => {
      if (err) {
        console.error('Failed to subscribe: %s', err.message);
      }
    });

    redisClientSubscriber.on('message', async (channel, message) => {
      try {
        message = JSON.parse(message);
        await Promise.all(this.subscribers.map(subscriber => subscriber(message)));
      } catch (error) {
        console.error(error.stack || error);
      }
    });
  }
}
