class ContinueWaitError extends Error {
  constructor() {
    super('Continue wait');
  }
}

module.exports = ContinueWaitError;
