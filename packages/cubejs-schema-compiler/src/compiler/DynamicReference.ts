export class DynamicReference<T> {
  public constructor(public memberNames: Array<string>, public fn: (...args: Array<unknown>) => T) {
  }
}
