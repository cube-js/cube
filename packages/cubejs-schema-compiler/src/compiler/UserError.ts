export class UserError extends Error {
  protected readonly type: string = 'UserError';

  public constructor(message: string) {
    super(message);
  }
}
