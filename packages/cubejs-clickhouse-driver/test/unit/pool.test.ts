import { createClient } from '@clickhouse/client';

import { ClickHouseDriver } from '../../src';

jest.mock('@clickhouse/client', () => ({
  ...jest.requireActual('@clickhouse/client'),
  createClient: jest.fn(() => ({ close: jest.fn() })),
}));

const createClientMock = createClient as unknown as jest.Mock;

describe('ClickHouseDriver pool size', () => {
  const originalMaxPool = process.env.CUBEJS_DB_MAX_POOL;

  beforeEach(() => {
    createClientMock.mockClear();
    delete process.env.CUBEJS_DB_MAX_POOL;
  });

  afterAll(() => {
    if (originalMaxPool === undefined) {
      delete process.env.CUBEJS_DB_MAX_POOL;
    } else {
      process.env.CUBEJS_DB_MAX_POOL = originalMaxPool;
    }
  });

  const createDriver = (options = {}) => new ClickHouseDriver({
    host: 'localhost',
    port: '8123',
    dataSource: 'default',
    ...options,
  });

  const maxOpenConnections = () => createClientMock.mock.calls[0][0].max_open_connections;

  it('defaults to the driver concurrency, so statements do not queue on sockets', () => {
    createDriver();

    expect(maxOpenConnections()).toEqual(ClickHouseDriver.getDefaultConcurrency());
  });

  it('follows CUBEJS_DB_MAX_POOL', () => {
    process.env.CUBEJS_DB_MAX_POOL = '3';
    createDriver();

    expect(maxOpenConnections()).toEqual(3);
  });

  it('prefers an explicit maxPoolSize over CUBEJS_DB_MAX_POOL', () => {
    process.env.CUBEJS_DB_MAX_POOL = '3';
    createDriver({ maxPoolSize: 42 });

    expect(maxOpenConnections()).toEqual(42);
  });
});
