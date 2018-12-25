class CompileError extends Error {
  constructor(messages) {
    super(`Compile errors:\n${messages.join('\n')}`);
    this.messages = messages;
  }
}

module.exports = CompileError;