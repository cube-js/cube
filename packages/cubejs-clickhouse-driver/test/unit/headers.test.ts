import { createClient } from '@clickhouse/client';

import { ClickHouseDriver } from '../../src';

jest.mock('@clickhouse/client', () => ({
  createClient: jest.fn(() => ({
    close: jest.fn(),
  })),
}));

const createClientMock = createClient as unknown as jest.Mock;

describe('ClickHouseDriver custom headers', () => {
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
});
