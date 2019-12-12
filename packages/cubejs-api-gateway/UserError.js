const CubejsHandlerError = require('./CubejsHandlerError');

class UserError extends CubejsHandlerError {
  constructor(message) {
    super(400, 'User Error', message);
  }
}

module.exports = UserError;
