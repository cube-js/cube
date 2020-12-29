import { CacheDriverInterface } from './cache-driver.interface';

const store = {};

export class LocalCacheDriver implements CacheDriverInterface {
  protected readonly store: Record<string, any>;

  public constructor() {
    this.store = store;
  }

  public async get(key: string) {
    if (this.store[key] && this.store[key].exp < new Date().getTime()) {
      delete this.store[key];
    }
    return this.store[key] && this.store[key].value;
  }

  public async set(key: string, value, expiration) {
    this.store[key] = {
      value,
      exp: new Date().getTime() + expiration * 1000
    };
  }

  public async remove(key: string) {
    delete this.store[key];
  }

  public async keysStartingWith(prefix: string) {
    return Object.keys(this.store)
      .filter(k => k.indexOf(prefix) === 0 && this.store[k].exp > new Date().getTime());
  }

  public async cleanup(): Promise<void> {
    // Nothing to do
  }

  public async testConnection(): Promise<void> {
    // Nothing to do
  }
}
