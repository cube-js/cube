import { CubeStoreDriver } from '../src/CubeStoreDriver';
import { CubestoreQueueDriverConnection } from '../src/CubeStoreQueueDriver';
import { ClientMessageTooLargeError, MessageTooLargeError, QueryError, ResultTooLargeError } from '../src/errors';

jest.mock('@cubejs-backend/native', () => ({
  parseCubestoreResultMessage: jest.fn(),
}));

class RefusingCubeStoreDriver extends CubeStoreDriver {
  public error: Error = new Error('not set');

  public async query<R = any>(): Promise<R[]> {
    throw this.error;
  }

  public async hasCapability(): Promise<boolean> {
    return false;
  }
}

describe('CubestoreQueueDriverConnection.setResultAndRemoveQuery', () => {
  const driver = new RefusingCubeStoreDriver();
  const connection = new CubestoreQueueDriverConnection(driver, {} as any);

  const ack = (result: string) => connection.setResultAndRemoveQuery('hash' as any, { result }, '1' as any).catch(e => e);

  it('reports a result Cube Store refused on its own as too large', async () => {
    driver.error = new QueryError(
      'Request of 55000000 bytes exceeds the maximum message size of 50331648 bytes. Reduce the size of the query, ' +
      'e.g. by sending fewer or smaller inline tables, or raise CUBESTORE_TRANSPORT_MAX_MESSAGE_SIZE.'
    );

    const error = await ack('small');

    expect(error).toBeInstanceOf(ResultTooLargeError);
    expect(error.message).toEqual(
      'Query result message of 52.5 MB exceeds the limit of 48 MB set by CUBESTORE_TRANSPORT_MAX_MESSAGE_SIZE on the Cube Store side'
    );
  });

  it('keeps any other query error as it is', async () => {
    driver.error = new QueryError('Internal: queue is gone');

    const error = await ack('small');

    expect(error).toBeInstanceOf(QueryError);
    expect(error.message).toEqual('Internal: queue is gone');
  });

  it('reports the result message a closed connection named as too large', async () => {
    driver.error = new MessageTooLargeError('Cube Store closed the connection', undefined, {
      limit: 1024,
      size: 1536,
      offender: true,
    });

    const error = await ack('small');

    expect(error).toBeInstanceOf(ResultTooLargeError);
    expect(error.message).toEqual(
      'Query result message of 1.5 KB exceeds the limit of 1 KB set by CUBESTORE_TRANSPORT_MAX_MESSAGE_SIZE on the ' +
      'Cube Store side'
    );
  });

  it('names the limit a closed connection reported', async () => {
    driver.error = new MessageTooLargeError('Cube Store closed the connection', undefined, {
      limit: 8 * 1024 * 1024,
      limitSetting: 'CUBESTORE_TRANSPORT_MAX_FRAME_SIZE',
      size: 20 * 1024 * 1024,
      offender: true,
    });

    const error = await ack('small');

    expect(error).toBeInstanceOf(ResultTooLargeError);
    expect(error.message).toEqual(
      'Query result message of 20 MB exceeds the limit of 8 MB set by CUBESTORE_TRANSPORT_MAX_FRAME_SIZE on the ' +
      'Cube Store side'
    );
  });

  it('names the client limit for a result refused before it was sent', async () => {
    driver.error = new ClientMessageTooLargeError('Cube Store request size exceeds the maximum', 1024, 1536);

    const error = await ack('small');

    expect(error).toBeInstanceOf(ResultTooLargeError);
    expect(error.message).toEqual(
      'Query result message of 1.5 KB exceeds the limit of 1 KB set by CUBEJS_CUBESTORE_MAX_MESSAGE_SIZE, which has ' +
      'to be raised together with CUBESTORE_TRANSPORT_MAX_MESSAGE_SIZE on the Cube Store side'
    );
  });

  it('keeps a result message a closed connection did not name a plain oversized message error', async () => {
    driver.error = new MessageTooLargeError('Cube Store closed the connection', undefined, {
      limit: 1024,
      size: 1536,
      offender: false,
    });

    const error = await ack('small');

    expect(error).toBeInstanceOf(MessageTooLargeError);
    expect(error).not.toBeInstanceOf(ResultTooLargeError);
  });

  it('keeps a closed connection without a named offender a plain oversized message error', async () => {
    driver.error = new MessageTooLargeError('Cube Store closed the connection', undefined, { size: 2048 });

    const error = await ack('small');

    expect(error).toBeInstanceOf(MessageTooLargeError);
    expect(error).not.toBeInstanceOf(ResultTooLargeError);
  });
});
