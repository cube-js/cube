export class UserError extends Error {
  protected readonly type: string = 'UserError';

  public constructor(message: string) {
    super(message);
  }
}

/**
 * `BaseQuery.tryJoinTreeForHints` turns exactly this error into "no join", so throw it only
 * when the join graph has no path covering the cubes to join.
 */
export class JoinPathNotFoundError extends UserError {}
