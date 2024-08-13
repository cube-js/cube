import RedisClient, { createClient } from 'redis';
import { EventEmitterInterface } from './EventEmitter.interface';

export interface RedisEventEmitterOptions {
    url: string;
    prefix: string;
}

export class RedisEventEmitter implements EventEmitterInterface {
    readonly #client: RedisClient.RedisClientType;

    readonly #options: RedisEventEmitterOptions;

    readonly #subscriber: RedisClient.RedisClientType;

    public constructor(options: RedisEventEmitterOptions) {
      this.#client = createClient({
        url: options.url
      });
      this.#options = options;
      this.#subscriber = this.#client.duplicate();
    }

    public on(event: string, listener: (args: any) => void): this {
      this.#subscriber.subscribe(`${this.#options.prefix}.${event}`, (val) => {
        console.log('Message received from to', val);
        listener(JSON.parse(val));
      });
      return this;
    }

    public emit(event: string, ...args: any): boolean {
      const channel = `${this.#options.prefix}.${event}`;
      this.#client.publish(channel, JSON.stringify(args));
      // Assume that there are always listeners
      return true;
    }
}
