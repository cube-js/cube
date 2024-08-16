import {createClient, RedisClientType} from 'redis';
import { Subject } from 'rxjs';
import { take } from 'rxjs/operators';
import { EventEmitterInterface, EventEmitterOptions } from './EventEmitter.interface';

export interface RedisEventEmitterOptions extends EventEmitterOptions {
  type: 'redis';
  url: string;
}

export class RedisEventEmitter implements EventEmitterInterface {
    #sub: RedisClientType | null = null;

    #pub: RedisClientType | null = null;

    #initialized = false;

    readonly #url: string;

    readonly #initSubject = new Subject<null>();

    public constructor(url: string) {
      this.#url = url;
      this.init().then(() => {
        console.log('Redis client is initialized');
      });
    }

    public async init() {
      const options = { url: this.#url };
      this.#sub = createClient(options);
      this.#pub = createClient(options);

      await this.#sub.connect();
      await this.#pub.connect();

      this.#initialized = true;
      this.#initSubject.next(null);
      this.#initSubject.complete();
    }

    public on(event: string, listener: (...args: any[]) => void) {
      if (!this.#initialized || !this.#sub) {
        this.#initSubject
          .pipe(
            take(1)
          )
          .subscribe(() => {
            this.on(event, listener);
          });
        return;
      }

      console.log('Subscribing to', event);
      this.#sub.subscribe(event, (message) => {
        listener(JSON.parse(message));
      });
    }

    public emit(event: string, ...args: any[]): boolean {
      if (!this.#initialized || !this.#pub) {
        return false;
      }

      this.#pub.publish(event, JSON.stringify(args));
      return true;
    }
}
