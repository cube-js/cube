export interface CacheDriverInterface {
  get(key: string): Promise<any>;
  set(key: string, value, expiration): Promise<void>;
  remove(key: string): Promise<void>;
  keysStartingWith(prefix: string): Promise<any[]>;
}
