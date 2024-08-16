import { Subject } from 'rxjs';
import { take } from 'rxjs/operators';
import { RedisClient } from 'redis';
import { EventEmitterInterface, EventEmitterOptions } from './EventEmitter.interface';
import { createRedisClient } from './RedisFactory';

export interface RedisEventEmitterOptions extends EventEmitterOptions {
  type: 'redis';
  url: string;
}

export class RedisEventEmitter implements EventEmitterInterface {
    #sub: RedisClient | null = null;

    #pub: RedisClient | null = null;

    readonly #subscriptions = new Map<string, ((args: any) => void)[]>();

    readonly #url: string;

    readonly #initSubject = new Subject<null>();

    public constructor(url: string) {
      this.#url = url;
      this.init().then(() => {
        console.log('Redis client is initialized');
      });
    }

    public async init() {
      this.#sub = await createRedisClient(this.#url);
      this.#pub = await createRedisClient(this.#url);

      this.#sub.on('message', (channel, message) => {
        const subscribers = this.#subscriptions.get(channel);
        if (subscribers) {
          subscribers.forEach((l) => l(JSON.parse(message)));
        }
      });

      this.#initSubject.next(null);
    }

    public on(event: string, listener: (...args: any[]) => void) {
      if (this.#sub) {
        console.log('Subscribing to', event);
        this.#sub.subscribe(event);
      } else {
        this.#initSubject
          .pipe(
            take(1)
          )
          .subscribe(() => {
          console.log('Subscribing to', event);
          this.#sub!.subscribe(event);
        });
      }

      if (!this.#subscriptions.has(event)) {
        this.#subscriptions.set(event, []);
      }
      this.#subscriptions.get(event)!.push(listener);
    }

    public emit(event: string, ...args: any[]): boolean {
      if (!this.#pub) {
        return false;
      }

      this.#pub.publish(event, JSON.stringify(args));
      return true;
    }
}
