import { createClient } from '@clickhouse/client';

import { ClickHouseDriver } from '../../src';
import { formatError } from '../../src/utils';

jest.mock('@clickhouse/client', () => ({
  createClient: jest.fn(),
}));

const createClientMock = createClient as unknown as jest.Mock;

describe('ClickHouseDriver statement path', () => {
  let client: Record<'ping' | 'query' | 'command' | 'insert' | 'close', jest.Mock>;

  beforeEach(() => {
    client = {
      ping: jest.fn(async () => ({ success: true })),
      query: jest.fn(async () => ({
        response_headers: {},
        json: async () => ({ meta: [{ name: 'one', type: 'UInt8' }], data: [{ one: 1 }] }),
      })),
      command: jest.fn(async () => ({ query_id: 'test' })),
      insert: jest.fn(async () => ({ executed: true })),
      close: jest.fn(async () => undefined),
    };
    createClientMock.mockReset();
    createClientMock.mockImplementation(() => client);
  });

  const createDriver = () => new ClickHouseDriver({
    host: 'localhost',
    port: '8123',
    dataSource: 'default',
  });

  it('does not run a health check per query', async () => {
    const driver = createDriver();

    expect(await driver.query('SELECT 1 AS one', [])).toEqual([{ one: '1' }]);
    await driver.query('SELECT 1 AS one', []);

    expect(client.query).toHaveBeenCalledTimes(2);
    expect(client.ping).not.toHaveBeenCalled();
    // A single long-lived client, so no extra socket pool per statement
    expect(createClientMock).toHaveBeenCalledTimes(1);
  });

  it('does not run a health check per command or insert', async () => {
    const driver = createDriver();

    await driver.command('CREATE DATABASE IF NOT EXISTS test');
    await driver.insert('test.t', [[1]]);

    expect(client.command).toHaveBeenCalledTimes(1);
    expect(client.insert).toHaveBeenCalledTimes(1);
    expect(client.ping).not.toHaveBeenCalled();
  });

  it('reports the reason of a failed query in the message', async () => {
    client.query.mockRejectedValue(new AggregateError(
      [new Error('connect ECONNREFUSED ::1:8123'), new Error('connect ECONNREFUSED 127.0.0.1:8123')],
      'All promises were rejected',
    ));

    await expect(createDriver().query('SELECT 1', [])).rejects.toThrow(
      /Query failed: Aggregate error: All promises were rejected; errors: Error: connect ECONNREFUSED ::1:8123; Error: connect ECONNREFUSED 127\.0\.0\.1:8123; query id: /
    );
  });

  it('reports the reason of a failed command in the message', async () => {
    client.command.mockRejectedValue(new Error('Timeout error.'));

    await expect(createDriver().command('DROP TABLE test.t')).rejects.toThrow(
      /Command failed: Error: Timeout error\.; query id: /
    );
  });
});

describe('formatError', () => {
  it('flattens nested aggregate errors', () => {
    const error = new AggregateError(
      [new AggregateError([new Error('inner')], 'nested'), new Error('outer')],
      'All promises were rejected',
    );

    expect(formatError(error)).toEqual(
      'Aggregate error: All promises were rejected; errors: Aggregate error: nested; errors: Error: inner; Error: outer'
    );
  });

  it('omits the prefix message when the aggregate error carries none', () => {
    const error = new AggregateError([new Error('connect ECONNREFUSED ::1:1')]);

    expect(formatError(error)).toEqual('Aggregate error; errors: Error: connect ECONNREFUSED ::1:1');
  });

  it('stringifies plain errors', () => {
    expect(formatError(new Error('boom'))).toEqual('Error: boom');
  });
});
