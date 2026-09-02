import { ClickHouseLogLevel, createClient } from '@clickhouse/client';

import { ClickHouseDriver } from '../../src';

jest.mock('@clickhouse/client', () => ({
  ...jest.requireActual('@clickhouse/client'),
  createClient: jest.fn(() => ({
    close: jest.fn(),
  })),
}));

const createClientMock = createClient as unknown as jest.Mock;

describe('ClickHouseDriver client options', () => {
  beforeEach(() => {
    createClientMock.mockClear();
  });

  it('forwards custom headers to the underlying client as http_headers', () => {
    const headers = {
      'X-Custom-Header': 'custom-value',
    };

    // eslint-disable-next-line no-new
    new ClickHouseDriver({
      host: 'localhost',
      port: '8123',
      dataSource: 'default',
      headers,
    });

    expect(createClientMock).toHaveBeenCalled();
    expect(createClientMock.mock.calls[0][0]).toMatchObject({ http_headers: headers });
  });

  it('defaults http_headers to an empty object when no headers are configured', () => {
    // eslint-disable-next-line no-new
    new ClickHouseDriver({
      host: 'localhost',
      port: '8123',
      dataSource: 'default',
    });

    expect(createClientMock).toHaveBeenCalled();
    expect(createClientMock.mock.calls[0][0]).toMatchObject({ http_headers: {} });
  });

  it('defaults the client log level to ERROR', () => {
    // eslint-disable-next-line no-new
    new ClickHouseDriver({
      host: 'localhost',
      port: '8123',
      dataSource: 'default',
    });

    expect(createClientMock.mock.calls[0][0]).toMatchObject({
      log: { level: ClickHouseLogLevel.ERROR },
    });
  });

  it('forwards a custom log level', () => {
    // eslint-disable-next-line no-new
    new ClickHouseDriver({
      host: 'localhost',
      port: '8123',
      dataSource: 'default',
      logLevel: ClickHouseLogLevel.TRACE,
    });

    expect(createClientMock.mock.calls[0][0]).toMatchObject({
      log: { level: ClickHouseLogLevel.TRACE },
    });
  });

  it('routes client logs to the logger set on the driver', () => {
    const driver = new ClickHouseDriver({
      host: 'localhost',
      port: '8123',
      dataSource: 'default',
    });
    const logger = jest.fn();
    driver.setLogger(logger);

    const { LoggerClass } = createClientMock.mock.calls[0][0].log;
    const err = new Error('boom');
    new LoggerClass().error({ module: 'Connection', message: 'Request failed', args: { query_id: '42' }, err });

    expect(logger).toHaveBeenCalledWith('ClickHouse Client Log', expect.objectContaining({
      level: 'error',
      module: 'Connection',
      message: 'Request failed',
      query_id: '42',
      error: expect.stringContaining('boom'),
    }));
  });

  it('does not throw when no logger is set on the driver', () => {
    // eslint-disable-next-line no-new
    new ClickHouseDriver({
      host: 'localhost',
      port: '8123',
      dataSource: 'default',
    });

    const { LoggerClass } = createClientMock.mock.calls[0][0].log;
    expect(() => new LoggerClass().warn({ module: 'Config', message: 'nobody listens' })).not.toThrow();
  });
});
