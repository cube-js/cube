class UserError extends Error {
  constructor(message) {
    super(message);
    this.type = 'UserError';
    console.trace(message)
  }
}

module.exports = UserError;
