export class UserError extends Error {
  protected readonly type: string = 'UserError';

  public constructor(message: string) {
    super(message);
  }
}

/**
 * The join graph has no path covering the cubes to join.
 */
export class JoinPathNotFoundError extends UserError {}
