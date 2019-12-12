class CubejsHandlerError extends Error {
  constructor(status, type, message) {
    super(message || type);
    this.status = status;
    this.type = type;
  }
}

module.exports = CubejsHandlerError;
