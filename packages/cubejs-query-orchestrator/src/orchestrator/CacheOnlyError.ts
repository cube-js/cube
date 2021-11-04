export class CacheOnlyError extends Error {
  public constructor() {
    super('Cache only');
  }
}
