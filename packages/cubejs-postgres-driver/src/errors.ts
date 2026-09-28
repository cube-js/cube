export class PostgresError extends Error {
  public name = 'PostgresError';

  public constructor(message: string, options?: ErrorOptions) {
    super(message, options);
  }
}

export class ConnectionError extends PostgresError {
  public readonly name = 'ConnectionError';

  public constructor(cause: Error, poolName: string) {
    const message = cause instanceof AggregateError
      ? cause.errors.map((e: Error) => e.message).join(', ')
      : cause.message;

    super(`Unable to connect to the database (${poolName}): ${message}`, { cause });
  }
}
