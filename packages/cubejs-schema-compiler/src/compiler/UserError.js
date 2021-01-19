export class UserError extends Error {
  constructor(message) {
    super(message);
    this.type = 'UserError';
  }
}
