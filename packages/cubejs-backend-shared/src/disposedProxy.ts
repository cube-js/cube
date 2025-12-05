/**
 * Creates a proxy object that throws an error on any property access.
 * Used as a safeguard after disposal to catch dangling references.
 */
export function disposedProxy(name: string, instanceName: string): any {
  return new Proxy({}, {
    get(_target: object, prop: string | symbol): never {
      throw new Error(
        `Cannot access property '${String(prop)}' on ${instanceName}. ` +
        `The '${name}' has been cleaned up and is no longer available.`
      );
    },
    set(_target: object, prop: string | symbol): never {
      throw new Error(
        `Cannot set property '${String(prop)}' on ${instanceName}. ` +
        `The '${name}' has been cleaned up and is no longer available.`
      );
    },
    apply(): never {
      throw new Error(
        `Cannot call method on ${instanceName}. ` +
        `The '${name}' has been cleaned up and is no longer available.`
      );
    },
    has(_target: object, _prop: string | symbol): never {
      throw new Error(
        `Cannot check property existence on ${instanceName}. ` +
        `The '${name}' has been cleaned up and is no longer available.`
      );
    },
    ownKeys(): never {
      throw new Error(
        `Cannot enumerate properties on ${instanceName}. ` +
        `The '${name}' has been cleaned up and is no longer available.`
      );
    },
    getPrototypeOf(): never {
      throw new Error(
        `Cannot get prototype of ${instanceName}. ` +
        `The '${name}' has been cleaned up and is no longer available.`
      );
    }
  });
}
