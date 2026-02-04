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

export class QueryError extends CubeStoreError {
  public constructor(message: string) {
    super(message);
    this.name = 'QueryError';
  }
}
