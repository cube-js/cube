/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview Named pool wrapper around generic-pool with enhanced error messages.
 */

import genericPool, { Pool as GenericPool, Factory, Options } from 'generic-pool';

export { Factory, Options as PoolOptions } from 'generic-pool';

export class PoolTimeoutError extends Error {
  public readonly poolName: string;

  public constructor(poolName: string) {
    super(`ResourceRequest timed out (pool: ${poolName})`);
    this.name = 'PoolTimeoutError';
    this.poolName = poolName;
  }
}

/**
 * Uses composition instead of inheritance because generic-pool doesn't export
 * a Pool class, the Pool type is an interface, not an extendable class.
 */
export class Pool<T> {
  private readonly pool: GenericPool<T>;

  private readonly name: string;

  public constructor(name: string, factory: Factory<T>, options?: Options) {
    this.name = name;
    this.pool = genericPool.createPool<T>(factory, options);
  }

  public async acquire(priority?: number): Promise<T> {
    try {
      return await this.pool.acquire(priority);
    } catch (error) {
      if (error instanceof Error && error.name === 'TimeoutError') {
        throw new PoolTimeoutError(this.name);
      }

      throw error;
    }
  }

  public async release(resource: T): Promise<void> {
    return this.pool.release(resource);
  }

  public async destroy(resource: T): Promise<void> {
    return this.pool.destroy(resource);
  }

  public async drain(): Promise<void> {
    return this.pool.drain();
  }

  public async clear(): Promise<void> {
    return this.pool.clear();
  }

  public async use<U>(cb: (resource: T) => U | Promise<U>): Promise<U> {
    try {
      return await this.pool.use(cb);
    } catch (error) {
      if (error instanceof Error && error.name === 'TimeoutError') {
        throw new PoolTimeoutError(this.name);
      }
      throw error;
    }
  }

  // State accessors
  public get size(): number {
    return this.pool.size;
  }

  public get available(): number {
    return this.pool.available;
  }

  public get borrowed(): number {
    return this.pool.borrowed;
  }

  public get pending(): number {
    return this.pool.pending;
  }

  public get max(): number {
    return this.pool.max;
  }

  public get min(): number {
    return this.pool.min;
  }

  // Event handling
  public on(event: 'factoryCreateError' | 'factoryDestroyError', listener: (err: Error) => void): this {
    this.pool.on(event, listener);
    return this;
  }

  // For backward compatibility (drivers use pool._factory for testConnection)
  public get _factory(): Factory<T> {
    // eslint-disable-next-line no-underscore-dangle
    return (this.pool as any)._factory;
  }
}
