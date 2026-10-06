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

export type MessageTooLargeDetails = {
  // The limit in bytes that refused a message, when the side that refused it reported one.
  limit?: number,
  // The setting behind `limit`, when the side that refused the message named it.
  limitSetting?: string,
  // The size in bytes of the message this error rejected.
  size?: number,
  // Whether this message is the one refused for its size, unset when that is not known.
  offender?: boolean,
};

/**
 * A message didn't fit into the size limit of the connection. Unlike other
 * connection errors this one is not worth retrying: the same message would be
 * rejected again.
 */
export class MessageTooLargeError extends ConnectionError {
  public readonly limit?: number;

  public readonly limitSetting?: string;

  public readonly size?: number;

  public readonly offender?: boolean;

  public constructor(message: string, cause?: Error, details: MessageTooLargeDetails = {}) {
    super(message, cause);

    this.name = 'MessageTooLargeError';
    this.limit = details.limit;
    this.limitSetting = details.limitSetting;
    this.size = details.size;
    this.offender = details.offender;
  }
}

/**
 * A message over CUBEJS_CUBESTORE_MAX_MESSAGE_SIZE, refused by this side before it was sent.
 */
export class ClientMessageTooLargeError extends MessageTooLargeError {
  public constructor(message: string, limit: number, size: number) {
    super(message, undefined, { limit, size, offender: true });

    this.name = 'ClientMessageTooLargeError';
  }
}

/**
 * A queue result Cube Store refused for its own size, as opposed to a message rejected alongside
 * an oversized one on the shared connection.
 */
export class ResultTooLargeError extends MessageTooLargeError {
  public constructor(message: string, cause?: Error) {
    super(message, cause);

    this.name = 'ResultTooLargeError';
  }
}

export class QueryError extends CubeStoreError {
  public constructor(message: string) {
    super(message);

    this.name = 'QueryError';
  }
}
