import jwt from 'jsonwebtoken';
import { testLogger } from '@cubejs-backend/shared';

import { ApiGateway } from '../../src';
import type { SQLServer } from '../../src/sql-server';
import { compilerApi, AdapterApiMock, DataSourceStorageMock } from '../mocks';

class TestApiGateway extends ApiGateway {
  // The SQL server needs the native binary, which the WebSocket flow doesn't touch.
  protected createSQLServerInstance(): SQLServer {
    return {} as unknown as SQLServer;
  }
}

describe('SubscriptionServer JWT expiry', () => {
  const SECRET = 'secret';
  const loadMessage = (messageId: string) => JSON.stringify({ method: 'load', messageId, params: { query: {} } });

  let gw: TestApiGateway;
  let load: jest.SpyInstance;
  let sent: any[];
  let closed: any[];

  const createServer = () => gw.initSubscriptionServer(
    async (_connectionId, message) => {
      sent.push(message);
    },
    (...args) => {
      closed.push(args);
    },
  );

  beforeEach(() => {
    jest.useFakeTimers();
    jest.setSystemTime(new Date('2026-01-01T00:00:00.000Z'));

    sent = [];
    closed = [];
    gw = new TestApiGateway(SECRET, compilerApi, async () => new AdapterApiMock(), testLogger, {
      standalone: true,
      dataSourceStorage: new DataSourceStorageMock(),
      basePath: '/cubejs-api',
      refreshScheduler: {},
      enforceSecurityChecks: true,
    });
    load = jest.spyOn(gw, 'load').mockImplementation(async ({ res }: any) => {
      await res({ data: [] });
    });
  });

  afterEach(() => {
    jest.restoreAllMocks();
    jest.useRealTimers();
  });

  test('an idle socket cannot issue its first query after the token expired', async () => {
    const server = createServer();
    const token = jwt.sign({ tenant: 'a' }, SECRET, { expiresIn: '10s' });

    await server.processMessage('conn-1', JSON.stringify({ authorization: token }));
    expect(sent).toEqual([{ handshake: true }]);

    jest.setSystemTime(Date.now() + 11 * 1000);
    await server.processMessage('conn-1', loadMessage('1'));

    expect(load).not.toHaveBeenCalled();
    expect(sent[1]).toMatchObject({ messageId: '1', message: { error: 'Invalid token' }, status: 403 });
    expect(closed).toEqual([['conn-1', 1008, 'Authorization expired']]);

    await server.processMessage('conn-1', loadMessage('2'));
    expect(load).not.toHaveBeenCalled();
    expect(sent[2]).toEqual({ messageId: '2', message: { error: 'Not authorized' }, status: 403 });
  });

  test('queries are served until the token expires', async () => {
    const server = createServer();
    const token = jwt.sign({ tenant: 'a' }, SECRET, { expiresIn: '10s' });

    await server.processMessage('conn-1', JSON.stringify({ authorization: token }));
    jest.setSystemTime(Date.now() + 9 * 1000);
    await server.processMessage('conn-1', loadMessage('1'));

    expect(load).toHaveBeenCalledTimes(1);
    expect(load.mock.calls[0][0].context.securityContext).toMatchObject({ tenant: 'a' });
    expect(closed).toEqual([]);
  });

  test('a failed re-authentication with an expired token revokes the previous identity', async () => {
    const server = createServer();
    const token = jwt.sign({ tenant: 'a' }, SECRET, { expiresIn: '1h' });
    const expiredToken = jwt.sign({ tenant: 'a' }, SECRET, { expiresIn: '-1s' });

    await server.processMessage('conn-1', JSON.stringify({ authorization: token }));
    await server.processMessage('conn-1', JSON.stringify({ authorization: expiredToken }));

    expect(sent[1]).toMatchObject({ message: { error: 'Invalid token' }, status: 403 });
    expect(closed).toEqual([['conn-1', 1008, 'Authorization failed']]);

    await server.processMessage('conn-1', loadMessage('1'));
    expect(load).not.toHaveBeenCalled();
    expect(sent[2]).toEqual({ messageId: '1', message: { error: 'Not authorized' }, status: 403 });
  });

  test('a downgrade to another identity replaces the previous one', async () => {
    const server = createServer();

    await server.processMessage('conn-1', JSON.stringify({ authorization: jwt.sign({ tenant: 'a' }, SECRET, { expiresIn: '1h' }) }));
    await server.processMessage('conn-1', JSON.stringify({ authorization: jwt.sign({ tenant: 'meta' }, SECRET, { expiresIn: '1h' }) }));
    await server.processMessage('conn-1', loadMessage('1'));

    expect(load.mock.calls[0][0].context.securityContext).toMatchObject({ tenant: 'meta' });
    expect(closed).toEqual([]);
  });
});
