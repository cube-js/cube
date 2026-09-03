import { createClient } from '@clickhouse/client';

import { ClickHouseDriver } from '../../src';

const FORMAT = 'JSONCompactEachRowWithNamesAndTypes';

const mockStream = jest.fn();
const mockQuery = jest.fn(async (_params: any) => ({
  response_headers: { 'x-clickhouse-format': FORMAT } as Record<string, string>,
  stream: mockStream,
}));

jest.mock('@clickhouse/client', () => ({
  ...jest.requireActual('@clickhouse/client'),
  createClient: jest.fn(() => ({
    ping: jest.fn(async () => ({ success: true })),
    query: mockQuery,
    close: jest.fn(),
  })),
}));

const createClientMock = createClient as unknown as jest.Mock;

/**
 * A batch as produced by ResultSet.stream().
 */
function batch(...rows: Array<Array<unknown>>) {
  return rows.map((row) => ({ json: () => row }));
}

function createDriver(): ClickHouseDriver {
  return new ClickHouseDriver({ host: 'localhost', port: '8123', dataSource: 'default' });
}

async function drain(stream: NodeJS.ReadableStream): Promise<Array<unknown>> {
  const rows: Array<unknown> = [];
  for await (const row of stream) {
    rows.push(row);
  }
  return rows;
}

describe('ClickHouseDriver mid-stream exceptions', () => {
  beforeEach(() => {
    createClientMock.mockClear();
    mockQuery.mockClear();
    mockStream.mockReset();
  });

  it('rejects the row stream when the server raises after the first block', async () => {
    mockStream.mockImplementation(async function* rows() {
      yield batch(['id']);
      yield batch(['UInt8']);
      yield batch([1]);
      throw new Error('Code: 395. DB::Exception: boom');
    });

    const driver = createDriver();
    // The names and types rows arrive before the exception, so stream() itself resolves
    const tableData = await driver.stream('SELECT 1 AS id', [], {} as any);
    expect(tableData.types).toEqual([{ name: 'id', type: 'int' }]);

    await expect(drain(tableData.rowStream)).rejects.toThrow(/Stream query failed.*boom.*query id/s);
  });

  it('yields all rows when the stream completes', async () => {
    mockStream.mockImplementation(async function* rows() {
      yield batch(['id']);
      yield batch(['UInt8']);
      yield batch([1], [2]);
    });

    const driver = createDriver();
    const tableData = await driver.stream('SELECT 1 AS id', [], {} as any);

    await expect(drain(tableData.rowStream)).resolves.toEqual([{ id: '1' }, { id: '2' }]);
  });
});
