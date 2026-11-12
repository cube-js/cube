import { ClickHouseDriver } from '../../src';

// Never resolves, like a long-running ClickHouse query
const mockQuery = jest.fn((_params: any) => new Promise(() => undefined));
const mockCommand = jest.fn(async (_params: any) => ({}));

jest.mock('@clickhouse/client', () => ({
  ...jest.requireActual('@clickhouse/client'),
  createClient: jest.fn(() => ({
    ping: jest.fn(async () => ({ success: true })),
    query: mockQuery,
    command: mockCommand,
    insert: jest.fn(async () => ({})),
    close: jest.fn(async () => undefined),
  })),
}));

function createDriver(): ClickHouseDriver {
  return new ClickHouseDriver({
    host: 'localhost',
    port: '8123',
    dataSource: 'default',
  });
}

// https://github.com/cube-js/cube/issues/12310
describe('ClickHouseDriver cancellation', () => {
  beforeEach(() => {
    mockQuery.mockClear();
    mockCommand.mockClear();
  });

  it('query() exposes the cancel handler QueryCache registers', async () => {
    const inFlight = createDriver().query('SELECT sleep(10)', []);

    expect(typeof (inFlight as any).cancel).toBe('function');
  });

  it('cancelling query() kills the running query on the server', async () => {
    const inFlight = createDriver().query('SELECT sleep(10)', []);
    inFlight.catch(() => undefined);

    // Let the driver send the query
    await new Promise((resolve) => setImmediate(resolve));
    const queryId = mockQuery.mock.calls[0][0].query_id;

    await (inFlight as any).cancel?.();

    expect(mockCommand).toHaveBeenCalledWith(expect.objectContaining({
      query: `KILL QUERY WHERE query_id = '${queryId}'`,
    }));
  });
});
