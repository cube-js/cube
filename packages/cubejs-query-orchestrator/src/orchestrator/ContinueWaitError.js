export class ContinueWaitError extends Error {
  constructor() {
    super('Continue wait');
  }
}
