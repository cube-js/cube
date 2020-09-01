class UserError extends Error {
  constructor(message) {
    super(message);
    this.type = 'UserError';
  }
}

module.exports = UserError;
