import { ClickHouseDriver } from '../../src';
import { getColumnConverter } from '../../src/Transform';

class TypeProbe extends ClickHouseDriver {
  public genericType(columnType: string): string {
    return this.toGenericType(columnType);
  }
}

// Converters are identified by instance, because behaviour cannot tell the DateTime64 precisions
// apart on a well formed value — they all agree with the moment formatter by design.
const CONVERTERS: Record<string, unknown> = {
  none: null,
  json: getColumnConverter('Map(String, Int32)'),
  date: getColumnConverter('Date'),
  number: getColumnConverter('Int64'),
  moment: getColumnConverter('DateTime64(10)'),
  dt0: getColumnConverter('DateTime'),
  dt1: getColumnConverter('DateTime64(1)'),
  dt2: getColumnConverter('DateTime64(2)'),
  dt3: getColumnConverter('DateTime64(3)'),
  dt6: getColumnConverter('DateTime64(6)'),
  dt9: getColumnConverter('DateTime64(9)'),
};

function converterLabel(type: string): string {
  const converter = getColumnConverter(type);
  const label = Object.keys(CONVERTERS).find((key) => CONVERTERS[key] === converter);

  return label ?? 'unknown';
}

// Written out inline rather than snapshotted, so a change to either seam shows up as a reviewable
// diff.
const TYPES: Array<[type: string, converter: string, generic: string]> = [
  ['Int8', 'number', 'int'],
  ['Int16', 'number', 'int'],
  ['Int32', 'number', 'int'],
  ['Int64', 'number', 'bigint'],
  ['Int128', 'number', 'decimal'],
  ['Int256', 'number', 'decimal'],
  ['UInt8', 'number', 'int'],
  ['UInt16', 'number', 'int'],
  ['UInt32', 'number', 'int'],
  ['UInt64', 'number', 'bigint'],
  ['UInt128', 'number', 'decimal'],
  ['UInt256', 'number', 'decimal'],
  ['Float32', 'number', 'float'],
  ['Float64', 'number', 'double'],
  ['BFloat16', 'number', 'float'],
  ['Decimal', 'number', 'decimal'],
  ['Decimal(9, 2)', 'number', 'decimal'],
  ['Decimal32(2)', 'number', 'decimal'],
  ['Decimal64(2)', 'number', 'decimal'],
  ['Decimal128(2)', 'number', 'decimal'],
  ['Decimal256(2)', 'number', 'decimal'],
  ['String', 'none', 'text'],
  ['FixedString(16)', 'none', 'text'],
  ['UUID', 'none', 'uuid'],
  ['Bool', 'none', 'boolean'],
  ['Boolean', 'none', 'boolean'],
  ['IPv4', 'none', 'text'],
  ['IPv6', 'none', 'text'],
  ['Nothing', 'none', 'text'],
  ['Date', 'date', 'date'],
  ['Date32', 'date', 'date'],
  ['DateTime', 'dt0', 'timestamp'],
  ['DateTime(\'UTC\')', 'dt0', 'timestamp'],
  ['DateTime64', 'dt3', 'timestamp'],
  ['DateTime64(0)', 'dt0', 'timestamp'],
  ['DateTime64(1)', 'dt1', 'timestamp'],
  ['DateTime64(2)', 'dt2', 'timestamp'],
  ['DateTime64(3, \'UTC\')', 'dt3', 'timestamp'],
  ['DateTime64(6, \'UTC\')', 'dt6', 'timestamp'],
  ['DateTime64(9)', 'dt9', 'timestamp'],
  ['DateTime64(10)', 'moment', 'timestamp'],
  ['Time', 'none', 'string'],
  ['Time64(3)', 'none', 'string'],
  // enums, including one naming other types in its values
  ['Enum(\'hello\' = 1, \'world\' = 2)', 'none', 'text'],
  ['Enum8(\'hello\' = 1)', 'none', 'text'],
  ['Enum16(\'hello\' = 1000)', 'none', 'text'],
  ['Enum8(\'Date\' = 1, \'Int\' = 2)', 'none', 'text'],
  ['Nullable(Int64)', 'number', 'bigint'],
  ['Nullable(String)', 'none', 'text'],
  ['Nullable(DateTime(\'UTC\'))', 'dt0', 'timestamp'],
  ['LowCardinality(String)', 'none', 'text'],
  ['LowCardinality(Nullable(String))', 'none', 'text'],
  ['LowCardinality(Nullable(DateTime64(6)))', 'dt6', 'timestamp'],
  // containers keep their element type in the generic type, but pass values through untouched
  ['Array(Int32)', 'none', 'int[]'],
  ['Array(DateTime)', 'none', 'timestamp[]'],
  ['Array(Nullable(String))', 'none', 'text[]'],
  ['Array(Array(Int32))', 'none', 'int[][]'],
  ['Array(Decimal(9, 2))', 'none', 'decimal[]'],
  ['Map(String, Int32)', 'json', 'text'],
  ['Map(String, Enum8(\'a,b\' = 1))', 'json', 'text'],
  ['Tuple(Int32, String)', 'json', 'text'],
  ['Tuple(a Int32, b String)', 'json', 'text'],
  ['Nested(a UInt8, b String)', 'json', 'text'],
  ['Variant(Int64, String)', 'json', 'text'],
  ['Dynamic', 'json', 'text'],
  ['JSON', 'json', 'text'],
  ['Object(\'json\')', 'json', 'text'],
  ['Point', 'json', 'text'],
  ['Ring', 'json', 'text'],
  ['LineString', 'json', 'text'],
  ['MultiLineString', 'json', 'text'],
  ['Polygon', 'json', 'text'],
  ['MultiPolygon', 'json', 'text'],
  // An AggregateFunction column holds a binary state that is useless to Cube, so it is left
  // unmapped and fails the Cube Store CREATE TABLE. SimpleAggregateFunction holds a plain value.
  ['AggregateFunction(sum, Int64)', 'none', 'AggregateFunction(sum, Int64)'],
  ['AggregateFunction(quantiles(0.5, 0.9), UInt64)', 'none', 'AggregateFunction(quantiles(0.5, 0.9), UInt64)'],
  ['SimpleAggregateFunction(sum, Int64)', 'number', 'bigint'],
  ['SimpleAggregateFunction(max, DateTime64(3))', 'dt3', 'timestamp'],
  ['SimpleAggregateFunction(anyLast, Map(String, Int64))', 'json', 'text'],
  ['SimpleAggregateFunction(groupArrayArray, Array(Int64))', 'none', 'bigint[]'],
  ['SimpleAggregateFunction(anyLast, Map(String, Enum8(\'a,b\' = 1)))', 'json', 'text'],
  ['IntervalDay', 'none', 'text'],
  ['IntervalMonth', 'none', 'text'],
  // unbalanced or unknown input must not throw; an unknown name is reported as is, so that it fails
  // the Cube Store CREATE TABLE instead of silently landing in the wrong column type
  ['Nullable(Int64', 'number', 'bigint'],
  ['Array(Int32', 'none', 'int[]'],
  ['Bogus', 'none', 'Bogus'],
  ['', 'none', ''],
];

describe('type mapping', () => {
  const driver = new TypeProbe({ host: 'localhost', port: '8123', dataSource: 'default' });

  it.each(TYPES)('%s -> %s converter, %s', (type, converter, generic) => {
    expect(converterLabel(type)).toEqual(converter);
    expect(driver.genericType(type)).toEqual(generic);
  });

  it('renders a container as text, and leaves an already textual value alone', () => {
    const converter = getColumnConverter('Map(String, Int32)');

    expect(converter?.({ a: 1 })).toEqual('{"a":1}');
    expect(converter?.([1, 'a'])).toEqual('[1,"a"]');
    expect(converter?.('already text')).toEqual('already text');
    expect(converter?.(null)).toEqual(null);
  });

  it('shares one converter instance per selected converter', () => {
    expect(getColumnConverter('Nullable(Int64)')).toBe(getColumnConverter('UInt256'));
    expect(getColumnConverter('Date32')).toBe(getColumnConverter('Nullable(Date)'));
    expect(getColumnConverter('DateTime64(3)')).not.toBe(getColumnConverter('DateTime64(6)'));
  });
});

// Without the flag every decimal collapses to a bare `decimal`, so this is the only place the
// width to precision table above is observable. Decimal256's 76 digits are clamped to 38: Cube
// Store would otherwise build a Decimal128 past Arrow's maximum precision.
describe('type mapping with CUBEJS_DB_PRECISE_DECIMAL_IN_CUBESTORE', () => {
  const driver = new TypeProbe({ host: 'localhost', port: '8123', dataSource: 'default' });
  let previous: string | undefined;

  beforeAll(() => {
    previous = process.env.CUBEJS_DB_PRECISE_DECIMAL_IN_CUBESTORE;
    process.env.CUBEJS_DB_PRECISE_DECIMAL_IN_CUBESTORE = 'true';
  });

  afterAll(() => {
    if (previous === undefined) {
      delete process.env.CUBEJS_DB_PRECISE_DECIMAL_IN_CUBESTORE;
    } else {
      process.env.CUBEJS_DB_PRECISE_DECIMAL_IN_CUBESTORE = previous;
    }
  });

  const DECIMALS: Array<[type: string, generic: string]> = [
    ['Decimal(9, 2)', 'decimal(9, 2)'],
    ['Decimal(76, 10)', 'decimal(76, 10)'],
    ['Decimal32(2)', 'decimal(9, 2)'],
    ['Decimal64(2)', 'decimal(18, 2)'],
    ['Decimal128(4)', 'decimal(38, 4)'],
    ['Decimal256(4)', 'decimal(38, 4)'],
    ['Nullable(Decimal128(4))', 'decimal(38, 4)'],
    ['LowCardinality(Decimal64(2))', 'decimal(18, 2)'],
    ['SimpleAggregateFunction(sum, Decimal64(2))', 'decimal(18, 2)'],
    ['Array(Decimal(9, 2))', 'decimal(9, 2)[]'],
    // The base driver only renders a precision when both parts are present and non-zero, so an
    // argument-less or zero-scale decimal still reaches Cube Store as its default 18, 5.
    ['Decimal', 'decimal'],
    ['Decimal32(0)', 'decimal'],
    // The 128 and 256 bit integers map to decimal by name, without a precision of their own.
    ['Int128', 'decimal'],
    ['UInt256', 'decimal'],
  ];

  it.each(DECIMALS)('%s -> %s', (type, generic) => {
    expect(driver.genericType(type)).toEqual(generic);
  });
});
