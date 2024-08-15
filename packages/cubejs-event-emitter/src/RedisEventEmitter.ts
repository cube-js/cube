import { ClientOpts, RedisClient } from 'redis';
import { EventEmitterInterface, EventEmitterOptions } from './EventEmitter.interface';
import { createRedisClient } from './RedisFactory';

export interface RedisEventEmitterOptions extends EventEmitterOptions, ClientOpts {
  type: 'redis';
  url: string;
}

export class RedisEventEmitter implements EventEmitterInterface {
    #sub: RedisClient | null = null;

    #pub: RedisClient | null = null;

    readonly #subscriptions = new Map<string, ((args: any) => void)[]>();

    readonly #options: RedisEventEmitterOptions;

    public constructor(options: RedisEventEmitterOptions) {
      this.#options = options;
      this.init().then(() => {
        console.log('Redis client is initialized');
      });
    }

    public async init() {
      this.#sub = await createRedisClient(this.#options);
      this.#pub = await createRedisClient(this.#options);
    }

    public on(event: string, listener: (...args: any[]) => void): this {
      if (!this.#sub) {
        throw new Error('Redis client is not initialized');
      }

      if (!this.#subscriptions.has(event)) {
        this.#subscriptions.set(event, []);
        this.#sub.subscribe(event);
        this.#sub.on('message', (channel, message) => {
          const subscribers = this.#subscriptions.get(channel);
          if (subscribers) {
            subscribers.forEach((l) => l(JSON.parse(message)));
          }
        });
      }
      this.#subscriptions.get(event)!.push(listener);

      return this;
    }

    public emit(event: string, ...args: any[]): boolean {
      if (!this.#pub) {
        throw new Error('Redis client is not initialized');
      }

      this.#pub.publish(event, JSON.stringify(args));
      return true;
    }
}
