import {
  PRECISION_UNSUPPORTED,
  dateTimePrecision,
  isNumericTypeName,
  isOpaqueTypeName,
  parseType,
  unwrapScalarType,
} from '../../src/TypeParser';

describe('parseType', () => {
  it('lowercases the name and keeps the arguments verbatim', () => {
    expect(parseType('Int64')).toEqual({ name: 'int64', args: [] });
    expect(parseType('  DateTime64(3, \'UTC\')  ')).toEqual({ name: 'datetime64', args: ['3', '\'UTC\''] });
    expect(parseType('Tuple(a Int32, b String)')).toEqual({ name: 'tuple', args: ['a Int32', 'b String'] });
  });

  it('splits on top-level commas only', () => {
    expect(parseType('Map(String, Enum8(\'a,b\' = 1))').args).toEqual(['String', 'Enum8(\'a,b\' = 1)']);
    expect(parseType('Array(Tuple(Int32, String))').args).toEqual(['Tuple(Int32, String)']);
    expect(parseType('Enum8(\'a\' = 1, \'b\' = 2)').args).toEqual(['\'a\' = 1', '\'b\' = 2']);
  });

  it('ignores parens and commas inside quotes', () => {
    expect(parseType('Enum8(\'a)b\' = 1, \'c(d\' = 2)').args).toEqual(['\'a)b\' = 1', '\'c(d\' = 2']);
    expect(parseType('Enum8(\'a\\\'b,c\' = 1)').args).toEqual(['\'a\\\'b,c\' = 1']);
    expect(parseType('Tuple(`a,b` Int32)').args).toEqual(['`a,b` Int32']);
  });

  it('has no arguments for an empty argument list', () => {
    expect(parseType('DateTime64()')).toEqual({ name: 'datetime64', args: [] });
  });

  it('keeps an empty argument in place when there are others', () => {
    expect(parseType('Decimal(,2)').args).toEqual(['', '2']);
    expect(parseType('Decimal(9,)').args).toEqual(['9', '']);
  });

  it('keeps what it has on unbalanced input', () => {
    expect(parseType('Nullable(Int64')).toEqual({ name: 'nullable', args: ['Int64'] });
    expect(parseType('Enum8(\'a\' = 1')).toEqual({ name: 'enum8', args: ['\'a\' = 1'] });
    expect(parseType('')).toEqual({ name: '', args: [] });
  });

  it('does not resolve names off Object.prototype', () => {
    expect(parseType('constructor')).toEqual({ name: 'constructor', args: [] });
    expect(unwrapScalarType('constructor(Int64)').name).toEqual('constructor');
  });
});

describe('unwrapScalarType', () => {
  it('strips the wrappers a value reads back through, in any order', () => {
    expect(unwrapScalarType('Nullable(Int64)').name).toEqual('int64');
    expect(unwrapScalarType('LowCardinality(Nullable(String))').name).toEqual('string');
    expect(unwrapScalarType('Nullable(LowCardinality(String))').name).toEqual('string');
    expect(unwrapScalarType('SimpleAggregateFunction(max, LowCardinality(Nullable(DateTime64(3))))'))
      .toEqual({ name: 'datetime64', args: ['3'] });
  });

  it('keeps the wrappers that change the value shape', () => {
    expect(unwrapScalarType('Array(Int64)').name).toEqual('array');
    expect(unwrapScalarType('AggregateFunction(sum, Int64)').name).toEqual('aggregatefunction');
  });

  it('returns the wrapper itself when the argument it reads back as is missing', () => {
    expect(unwrapScalarType('SimpleAggregateFunction(sum)').name).toEqual('simpleaggregatefunction');
    expect(unwrapScalarType('Nullable()').name).toEqual('nullable');
  });
});

describe('dateTimePrecision', () => {
  const precisionOf = (type: string) => dateTimePrecision(parseType(type));

  it('reads the precision out of the first argument', () => {
    expect(precisionOf('DateTime64(0)')).toEqual(0);
    expect(precisionOf('DateTime64(6, \'UTC\')')).toEqual(6);
    expect(precisionOf('DateTime64(9)')).toEqual(9);
  });

  it('defaults a bare DateTime64 to 3', () => {
    expect(precisionOf('DateTime64')).toEqual(3);
    expect(precisionOf('DateTime64()')).toEqual(3);
  });

  it('treats a plain DateTime as precision 0', () => {
    expect(precisionOf('DateTime')).toEqual(0);
    expect(precisionOf('DateTime(\'Asia/Istanbul\')')).toEqual(0);
  });

  it('rejects a precision ClickHouse does not have', () => {
    expect(precisionOf('DateTime64(10)')).toEqual(PRECISION_UNSUPPORTED);
    expect(precisionOf('DateTime64(\'UTC\')')).toEqual(PRECISION_UNSUPPORTED);
    expect(precisionOf('DateTime64(-1)')).toEqual(PRECISION_UNSUPPORTED);
  });
});

describe('isOpaqueTypeName', () => {
  it('covers the containers and the geo types', () => {
    for (const name of ['map', 'tuple', 'nested', 'variant', 'dynamic', 'json', 'object', 'point', 'multipolygon']) {
      expect(isOpaqueTypeName(name)).toBe(true);
    }

    // Array keeps its element type in the generic type, so it is not opaque.
    for (const name of ['array', 'string', 'int64', 'aggregatefunction', '']) {
      expect(isOpaqueTypeName(name)).toBe(false);
    }
  });
});

describe('isNumericTypeName', () => {
  it('covers every width, including the ones that arrive as strings', () => {
    for (const name of ['int8', 'int256', 'uint64', 'uint256', 'float32', 'bfloat16', 'decimal', 'decimal256']) {
      expect(isNumericTypeName(name)).toBe(true);
    }

    for (const name of ['string', 'uuid', 'bool', 'date32', 'datetime64', 'array', 'ipv4', '']) {
      expect(isNumericTypeName(name)).toBe(false);
    }
  });
});
