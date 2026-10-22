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
 * Cube Store refused the connection itself rather than the query: the node it
 * reached can't serve this client (a node starting on another release channel
 * than the tenant's, for instance) and closes the connection right after
 * answering. The query was never executed, so it is safe to send it again over a
 * new connection, which usually reaches a node that can serve it.
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
