export class SnowflakeError extends Error {
  public name = 'SnowflakeError';

  public constructor(message: string, options?: ErrorOptions) {
    super(message, options);
  }
}

export class ConnectionError extends SnowflakeError {
  public readonly name = 'ConnectionError';

  public constructor(cause: Error) {
    super(`Unable to connect to the Snowflake instance: ${cause.message}`, { cause });
  }
}
