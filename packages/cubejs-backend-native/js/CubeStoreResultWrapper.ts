import { getCubestoreResult, ResultRow } from './index';

export class CubeStoreResultWrapper {
  private readonly proxy: any;

  private cache: any;

  public cached: Boolean = false;

  public constructor(private readonly nativeReference: any) {
    this.proxy = new Proxy(this, {
      get: (target, prop: string | symbol) => {
        // intercept indexes
        if (typeof prop === 'string' && !Number.isNaN(Number(prop))) {
          const array = this.getArray();
          return array[Number(prop)];
        }

        // intercept array methods
        if (typeof prop === 'string' && prop in Array.prototype) {
          const arrayMethod = (Array.prototype as any)[prop];
          if (typeof arrayMethod === 'function') {
            return (...args: any[]) => this.invokeArrayMethod(prop, ...args);
          }
        }

        // intercept isNative
        if (prop === 'isNative') {
          return true;
        }

        // intercept array length
        if (prop === 'length') {
          return this.getArray().length;
        }

        // intercept JSON.stringify or toJSON()
        if (prop === 'toJSON') {
          return () => this.getArray();
        }

        return (target as any)[prop];
      },

      // intercept array length
      getOwnPropertyDescriptor: (target, prop) => {
        if (prop === 'length') {
          const array = this.getArray();
          return {
            configurable: true,
            enumerable: true,
            value: array.length,
            writable: false
          };
        }
        return Object.getOwnPropertyDescriptor(target, prop);
      },

      ownKeys: (target) => {
        const array = this.getArray();
        return [...Object.keys(target), ...Object.keys(array), 'length', 'isNative'];
      }
    });

    return this.proxy;
  }

  private getArray(): ResultRow[] {
    if (!this.cache) {
      this.cache = getCubestoreResult(this.nativeReference);
      this.cached = true;
    }
    return this.cache;
  }

  private invokeArrayMethod(method: string, ...args: any[]): any {
    const array = this.getArray();
    return (array as any)[method](...args);
  }

  public getNativeRef() {
    return this.nativeReference;
  }
}
