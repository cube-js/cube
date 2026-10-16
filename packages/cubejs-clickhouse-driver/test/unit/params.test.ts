import { createClient } from '@clickhouse/client';

import { ClickHouseDriver } from '../../src';
import { formatParams } from '../../src/utils';

jest.mock('@clickhouse/client', () => ({
  ...jest.requireActual('@clickhouse/client'),
  createClient: jest.fn(),
}));

const createClientMock = createClient as unknown as jest.Mock;

// A regex with question marks, as used in the `sql` of cube members
const REGEX = String.raw`'(.*?)(?:-?\d{2})'`;

describe('formatParams', () => {
  it('substitutes tokens and keeps literal question marks', () => {
    expect(formatParams(`SELECT extract(s, ${REGEX}) WHERE x = ___ClickHouseParam_0___`, ['a\'b']))
      .toEqual(`SELECT extract(s, ${REGEX}) WHERE x = 'a''b'`);
  });

  it('keeps the ternary operator', () => {
    expect(formatParams('SELECT x > 0 ? ___ClickHouseParam_0___ : ___ClickHouseParam_1___', [1, 'neg']))
      .toEqual('SELECT x > 0 ? 1 : \'neg\'');
  });

  it('substitutes tokens by index, including repeated ones', () => {
    expect(formatParams('SELECT ___ClickHouseParam_1___, ___ClickHouseParam_0___, ___ClickHouseParam_1___', ['a', null]))
      .toEqual('SELECT NULL, \'a\', NULL');
  });

  it('escapes backslashes and does not rescan substituted values', () => {
    expect(formatParams('SELECT ___ClickHouseParam_0___, ___ClickHouseParam_1___', ['___ClickHouseParam_1___\\', 2]))
      .toEqual('SELECT \'___ClickHouseParam_1___\\\\\', 2');
  });

  it('leaves SQL without tokens untouched', () => {
    expect(formatParams(`SELECT ${REGEX}`)).toEqual(`SELECT ${REGEX}`);
  });

  it('throws on a token without a value', () => {
    expect(() => formatParams('SELECT ___ClickHouseParam_0___, ___ClickHouseParam_1___', ['a']))
      .toThrow('Missing value for ClickHouse query parameter 1 (1 provided)');
  });
});

describe('ClickHouseDriver params', () => {
  let client: Record<'query' | 'close', jest.Mock>;

  beforeEach(() => {
    client = {
      query: jest.fn(async () => ({
        response_headers: {},
        json: async () => ({ meta: [{ name: 'one', type: 'UInt8' }], data: [[1]] }),
      })),
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

  it('emits tokens from param()', () => {
    expect(createDriver().param(3)).toEqual('___ClickHouseParam_3___');
  });

  it('formats the query sent by query()', async () => {
    const driver = createDriver();
    await driver.query(`SELECT extract(s, ${REGEX}) AS one WHERE x = ${driver.param(0)}`, ['a\'b']);

    expect(client.query).toHaveBeenCalledWith(expect.objectContaining({
      query: `SELECT extract(s, ${REGEX}) AS one WHERE x = 'a''b'`,
    }));
  });

  it('formats the query sent by stream()', async () => {
    client.query.mockRejectedValue(new Error('boom'));

    const driver = createDriver();
    await expect(driver.stream(`SELECT extract(s, ${REGEX}) WHERE x = ${driver.param(0)}`, [42], { highWaterMark: 100 }))
      .rejects.toThrow('boom');

    expect(client.query).toHaveBeenCalledWith(expect.objectContaining({
      query: `SELECT extract(s, ${REGEX}) WHERE x = 42`,
    }));
  });
});
