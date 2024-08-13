import { RedisClient } from 'redis';
import { EventEmitterInterface } from './EventEmitter.interface';

export class RedisEventEmitter implements EventEmitterInterface {
    // readonly #redisPool: RedisPool;

    #client: RedisClient | null = null;

    // public constructor({ pool }: RedisEventEmitterOptions) {
    //   this.#redisPool = pool;
    //   this.getClient();
    // }

    // protected async getClient() {
    //   this.#client = await this.#redisPool.getClient();
    // }

    public on(event: string, listener: (...args: any[]) => void): this {
      if (!this.#client) {
        throw new Error('Redis client is not initialized');
      }
      this.#client.on(event, (val) => listener(JSON.parse(val)));

      return this;
    }

    public emit(event: string, ...args: any[]): boolean {
      if (!this.#client) {
        throw new Error('Redis client is not initialized');
      }
      this.#client.publish(event, JSON.stringify(args));

      return true;
    }
}
