abstract class CubeStoreError extends Error {

}

export class ConnectionError extends CubeStoreError {
  public readonly cause?: Error;

  public constructor(message: string, cause?: Error) {
    super(message);

    this.name = 'ConnectionError';
    this.cause = cause;
  }
}

/**
 * A message didn't fit into the size limit of the connection. Unlike other
 * connection errors this one is not worth retrying: the same message would be
 * rejected again.
 */
export class MessageTooLargeError extends ConnectionError {
  public constructor(message: string, cause?: Error) {
    super(message, cause);

    this.name = 'MessageTooLargeError';
  }
}

/**
 * Cube Store refused the connection, not the query, and closes it after answering.
 * The query never ran, so it is safe to re-send over a new connection.
 */
export class WrongConnectionError extends ConnectionError {
  public constructor(message: string) {
    super(message);

    this.name = 'WrongConnectionError';
  }
}

export class QueryError extends CubeStoreError {
  public constructor(message: string) {
    super(message);

    this.name = 'QueryError';
  }
}
