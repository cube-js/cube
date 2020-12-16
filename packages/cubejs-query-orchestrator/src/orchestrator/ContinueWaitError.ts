export class ContinueWaitError extends Error {
  public constructor() {
    super('Continue wait');
  }
}
