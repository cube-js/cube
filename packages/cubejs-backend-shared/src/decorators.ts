import { asyncDebounceFn, AsyncDebounceOptions } from './promises';

export function AsyncDebounce(options: AsyncDebounceOptions = {}) {
  return (
    target: any,
    propertyKey: string | symbol,
    descriptor: PropertyDescriptor
  ): PropertyDescriptor => {
    const originalMethod = descriptor.value;

    return {
      configurable: true,
      get() {
        const debouncedMethod = asyncDebounceFn(originalMethod.bind(this), options);

        Object.defineProperty(this, propertyKey, {
          value: debouncedMethod,
          configurable: true,
          writable: false
        });

        return debouncedMethod;
      }
    };
  };
}
