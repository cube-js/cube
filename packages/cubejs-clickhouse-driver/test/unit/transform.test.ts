import * as moment from 'moment';

import {
  buildTransformFromMeta,
  buildTransformFromNamesAndTypes,
  formatCanonicalDateTime,
  formatDateTime,
  getColumnConverter,
  transformRow,
} from '../../src/Transform';

const convert = (type: string, value: unknown) => {
  const converter = getColumnConverter(type);
  return converter === null ? value : converter(value);
};

const viaMoment = (value: unknown) => moment.utc(value as any).format(moment.HTML5_FMT.DATETIME_LOCAL_MS);

describe('getColumnConverter', () => {
  it('leaves non-convertible types alone', () => {
    for (const type of [
      'String', 'FixedString(8)', 'UUID', 'Bool', 'IPv4', 'IPv6', 'JSON', 'Nothing',
      'Enum8(\'hello\' = 1, \'world\' = 2)', 'Enum16(\'hello\' = 1, \'world\' = 1000)',
      'LowCardinality(String)', 'Nullable(String)',
    ]) {
      expect(getColumnConverter(type)).toBeNull();
    }
  });

  it('maps Date types to midnight', () => {
    expect(convert('Date', '2020-01-01')).toEqual('2020-01-01T00:00:00.000');
    expect(convert('Date32', '2020-01-01')).toEqual('2020-01-01T00:00:00.000');
    expect(convert('Nullable(Date)', '2020-01-01')).toEqual('2020-01-01T00:00:00.000');
  });

  it('maps DateTime types, with and without a timezone', () => {
    expect(convert('DateTime', '2020-01-01 00:00:00')).toEqual('2020-01-01T00:00:00.000');
    expect(convert('DateTime(\'Asia/Istanbul\')', '2020-01-01 00:00:00')).toEqual('2020-01-01T00:00:00.000');
    expect(convert('Nullable(DateTime(\'UTC\'))', '2020-01-01 00:00:00')).toEqual('2020-01-01T00:00:00.000');
  });

  it('maps every DateTime64 precision, truncating to millis', () => {
    expect(convert('DateTime64(0, \'UTC\')', '2020-01-02 00:00:00')).toEqual('2020-01-02T00:00:00.000');
    expect(convert('DateTime64(3, \'UTC\')', '2020-01-02 00:00:00.234')).toEqual('2020-01-02T00:00:00.234');
    expect(convert('DateTime64(6, \'UTC\')', '2020-01-02 00:00:00.234567')).toEqual('2020-01-02T00:00:00.234');
    expect(convert('DateTime64(9, \'UTC\')', '2020-01-02 00:00:00.234567890')).toEqual('2020-01-02T00:00:00.234');
    expect(convert('Nullable(DateTime64(3))', '2020-01-02 00:00:00.234')).toEqual('2020-01-02T00:00:00.234');

    expect(convert('DateTime64(9)', '2020-01-02 00:00:00.999999999')).toEqual('2020-01-02T00:00:00.999');
    expect(convert('DateTime64(6)', '2020-01-02 00:00:00.000999')).toEqual('2020-01-02T00:00:00.000');
  });

  it('stringifies every numeric type, including through Nullable', () => {
    for (const type of [
      'Int8', 'Int16', 'Int32', 'Int64', 'Int128', 'Int256',
      'UInt8', 'UInt16', 'UInt32', 'UInt64', 'Float32', 'Float64',
    ]) {
      expect(convert(type, 1)).toEqual('1');
      expect(convert(`Nullable(${type})`, 1)).toEqual('1');
    }

    expect(convert('Int64', '9223372036854775807')).toEqual('9223372036854775807');
    expect(convert('Decimal(38, 2)', '1.01')).toEqual('1.01');
    expect(convert('Decimal32(2)', 1.01)).toEqual('1.01');
    expect(convert('Nullable(Decimal64(2))', 1.01)).toEqual('1.01');
  });

  it('passes null through for every branch', () => {
    for (const type of [
      'Nullable(Date)', 'Nullable(DateTime)', 'Nullable(DateTime64(3, \'UTC\'))',
      'Nullable(Int64)', 'Nullable(Float64)', 'Nullable(Decimal(38, 2))', 'Nullable(String)',
    ]) {
      expect(convert(type, null)).toBeNull();
    }
  });

  it('leaves container types alone', () => {
    for (const type of [
      'Array(Int64)', 'Array(Nullable(Int64))', 'Array(Array(Float64))', 'Array(Date)',
      'Array(DateTime)', 'Array(DateTime64(3, \'UTC\'))', 'Array(String)',
      'Map(String, Int64)', 'Map(String, DateTime)', 'Map(Int64, Array(Decimal(38, 2)))',
      'Tuple(Int64, String)', 'Tuple(d DateTime, n Int32)',
      'Nested(n Int64, d Date)',
      'AggregateFunction(sum, Int64)', 'AggregateFunction(quantiles(0.5), Float64)',
      'JSON',
    ]) {
      expect(getColumnConverter(type)).toBeNull();
    }
  });

  it('leaves an enum whose members are named after types alone', () => {
    for (const type of [
      'Enum(\'Date\' = 1, \'Int\' = 2)',
      'Enum8(\'Date\' = 1)',
      'Enum16(\'DateTime\' = 1, \'Decimal\' = 2)',
      'Nullable(Enum8(\'Float\' = 1))',
    ]) {
      expect(getColumnConverter(type)).toBeNull();
    }

    expect(convert('Enum8(\'Date\' = 1, \'Int\' = 2)', 'Date')).toEqual('Date');
  });

  it('passes container values through as they arrive', () => {
    expect(convert('Array(Int64)', ['1', '2'])).toEqual(['1', '2']);
    expect(convert('Map(String, Int64)', { a: '1' })).toEqual({ a: '1' });
    expect(convert('Tuple(Int64, String)', ['1', 'a'])).toEqual(['1', 'a']);
    expect(convert('Array(Nullable(Int64))', [null, '3'])).toEqual([null, '3']);
  });

  it('sees through nested wrappers', () => {
    expect(convert('LowCardinality(Nullable(String))', 'x')).toEqual('x');
    expect(convert('LowCardinality(Nullable(Int64))', 1)).toEqual('1');
    expect(convert('LowCardinality(Nullable(Date))', '2020-01-01')).toEqual('2020-01-01T00:00:00.000');
  });

  // Not an AggregateFunction state: it reads back as a plain value of its argument type
  it('still stringifies a SimpleAggregateFunction', () => {
    expect(convert('SimpleAggregateFunction(sum, Int64)', 1)).toEqual('1');
    expect(convert('SimpleAggregateFunction(max, Float64)', 1.5)).toEqual('1.5');
  });
});

describe('formatCanonicalDateTime', () => {
  it('handles the simple output format', () => {
    expect(formatCanonicalDateTime('2020-01-02 00:00:00')).toEqual('2020-01-02T00:00:00.000');
    expect(formatCanonicalDateTime('2020-01-02 03:04:05.234567890')).toEqual('2020-01-02T03:04:05.234');
  });

  it('handles the iso output format', () => {
    expect(formatCanonicalDateTime('2020-01-02T00:00:00Z')).toEqual('2020-01-02T00:00:00.000');
    expect(formatCanonicalDateTime('2020-01-02T00:00:00.234Z')).toEqual('2020-01-02T00:00:00.234');
    expect(formatCanonicalDateTime('2020-01-02T00:00:00.234567890Z')).toEqual('2020-01-02T00:00:00.234');
  });

  it('right-pads a short fraction the way moment does', () => {
    expect(formatCanonicalDateTime('2020-01-02 00:00:00.2')).toEqual('2020-01-02T00:00:00.200');
    expect(formatCanonicalDateTime('2020-01-02 00:00:00.23')).toEqual('2020-01-02T00:00:00.230');
  });

  it('declines anything it cannot prove is equivalent to moment', () => {
    expect(formatCanonicalDateTime('2020-01-02T00:00:00+03:00')).toBeNull();
    expect(formatCanonicalDateTime('1577923200')).toBeNull();
    expect(formatCanonicalDateTime('2020-01-02 00:00:00.')).toBeNull();
    expect(formatCanonicalDateTime('2020-01-02 00:00:00 extra')).toBeNull();
    expect(formatCanonicalDateTime('2020-01-02')).toBeNull();
    expect(formatCanonicalDateTime('')).toBeNull();
    expect(formatCanonicalDateTime('not-a-date-at-all')).toBeNull();
  });
});

describe('formatDateTime', () => {
  it.each([
    ['2020-01-02T00:00:00+03:00'],
    ['1577923200'],
    ['garbage'],
  ])('delegates %s to moment', (value) => {
    expect(formatDateTime(value)).toEqual(viaMoment(value));
  });

  it('delegates non-string values to moment', () => {
    expect(formatDateTime(1577923200)).toEqual(viaMoment(1577923200));
  });

  it('agrees with moment on every canonical shape', () => {
    for (const value of [
      '2020-01-02 00:00:00',
      '2020-01-02 00:00:00.234',
      '2020-01-02 00:00:00.234567',
      '2020-01-02 00:00:00.234567890',
      '2020-01-02 00:00:00.2',
      '2020-01-02T00:00:00Z',
      '2020-01-02T23:59:59.999999Z',
    ]) {
      expect(formatCanonicalDateTime(value)).not.toBeNull();
      expect(formatDateTime(value)).toEqual(viaMoment(value));
    }
  });
});

describe('buildTransformFromMeta', () => {
  it('resolves one converter slot per column, null for pass-through', () => {
    const transform = buildTransformFromMeta([
      { name: 's', type: 'String' },
      { name: 'd', type: 'Date' },
      { name: 'e', type: 'Enum8(\'a\' = 1)' },
      { name: 'n', type: 'Int64' },
    ]);

    expect(transform.names).toEqual(['s', 'd', 'e', 'n']);
    expect(transform.converters.map((c) => c === null)).toEqual([true, false, true, false]);
  });

  it('builds a plain object with every column, in meta order', () => {
    const transform = buildTransformFromMeta([
      { name: 's', type: 'String' },
      { name: 'd', type: 'Date' },
      { name: 'n', type: 'Int32' },
    ]);
    const row = transformRow(['hello', '2020-01-01', 1], transform);

    expect(row).toEqual({ s: 'hello', d: '2020-01-01T00:00:00.000', n: '1' });
    expect(Object.keys(row)).toEqual(['s', 'd', 'n']);
    expect(Object.getPrototypeOf(row)).toBe(Object.prototype);
  });

  it('handles a result set where nothing needs converting', () => {
    const transform = buildTransformFromMeta([{ name: 's', type: 'String' }]);

    expect(transform.converters).toEqual([null]);
    expect(transformRow(['hello'], transform)).toEqual({ s: 'hello' });
  });

  it('matches what JSON.parse did to a format JSON row with duplicate column names', () => {
    const transform = buildTransformFromMeta([
      { name: 'x', type: 'Int64' },
      { name: 'y', type: 'String' },
      { name: 'x', type: 'Int64' },
    ]);
    const row = transformRow(['1', 'a', '2'], transform);

    expect(Object.keys(row)).toEqual(['x', 'y']);
    expect(JSON.stringify(row)).toEqual(JSON.stringify(JSON.parse('{"x":"1","y":"a","x":"2"}')));
  });

  it('orders integer-like column names the way JSON.parse did', () => {
    const transform = buildTransformFromMeta([
      { name: 'b', type: 'String' },
      { name: '0', type: 'String' },
    ]);
    const row = transformRow(['b', 'zero'], transform);

    expect(JSON.stringify(row)).toEqual(JSON.stringify(JSON.parse('{"b":"b","0":"zero"}')));
  });

  // SELECT 1 AS `__proto__` is legal ClickHouse
  it('does not let a __proto__ column reach Object.prototype', () => {
    const transform = buildTransformFromMeta([
      { name: '__proto__', type: 'String' },
      { name: 'n', type: 'Int32' },
    ]);

    expect(transform.nullPrototype).toBe(true);

    const row = transformRow(['pwned', 1], transform);

    expect(Object.keys(row)).toEqual(['__proto__', 'n']);
    expect(Object.getOwnPropertyDescriptor(row, '__proto__')?.value).toEqual('pwned');
    expect(({} as any).pwned).toBeUndefined();
    expect(Object.getPrototypeOf({})).toBe(Object.prototype);
  });

  it('rejects a row of the wrong width', () => {
    const transform = buildTransformFromMeta([
      { name: 'a', type: 'String' },
      { name: 'b', type: 'String' },
    ]);

    expect(() => transformRow(['only-one'], transform))
      .toThrow('Unexpected row and names/types length mismatch; row 1 vs names 2');
  });
});

describe('buildTransformFromNamesAndTypes', () => {
  const names = ['s', 'd', 'dt', 'dt64', 'n', 'nullable'];
  const types = ['String', 'Date', 'DateTime', 'DateTime64(9, \'UTC\')', 'Int64', 'Nullable(Float64)'];

  it('produces the same plan as the meta-shaped builder', () => {
    const fromNames = buildTransformFromNamesAndTypes(names, types);
    const fromMeta = buildTransformFromMeta(names.map((name, i) => ({ name, type: types[i] })));

    expect(fromNames).toEqual(fromMeta);
  });

  it('transforms a full row', () => {
    const transform = buildTransformFromNamesAndTypes(names, types);
    const row = transformRow(
      ['hello', '2020-01-01', '2020-01-01 00:00:00', '2020-01-02 00:00:00.234567890', 1, null],
      transform,
    );

    expect(row).toEqual({
      s: 'hello',
      d: '2020-01-01T00:00:00.000',
      dt: '2020-01-01T00:00:00.000',
      dt64: '2020-01-02T00:00:00.234',
      n: '1',
      nullable: null,
    });
  });

  it('rejects a names/types length mismatch at build time', () => {
    expect(() => buildTransformFromNamesAndTypes(['a', 'b'], ['String']))
      .toThrow('Unexpected names and types length mismatch; names 2 vs types 1');
  });
});
