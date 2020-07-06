class UserError extends Error {
  constructor(message, code) {
    super(message);
    this.code = code;
    this.type = 'UserError';
  }
}

module.exports = UserError;
