import { ClickHouseDriver } from '../../src';

class TypeProbe extends ClickHouseDriver {
  public genericType(columnType: string): string {
    return this.toGenericType(columnType);
  }
}

describe('toGenericType', () => {
  const driver = new TypeProbe({ host: 'localhost', port: '8123', dataSource: 'default' });
  const generic = (columnType: string) => driver.genericType(columnType);

  it('maps scalars', () => {
    expect(generic('Int8')).toEqual('int');
    expect(generic('Int64')).toEqual('bigint');
    expect(generic('UInt32')).toEqual('int');
    expect(generic('Float32')).toEqual('float');
    expect(generic('Float64')).toEqual('double');
    expect(generic('String')).toEqual('text');
    expect(generic('Date')).toEqual('date');
    expect(generic('DateTime')).toEqual('timestamp');
  });

  it('maps parameterized scalars by their name', () => {
    expect(generic('DateTime(\'UTC\')')).toEqual('timestamp');
    expect(generic('DateTime64(3, \'UTC\')')).toEqual('timestamp');
    expect(generic('Decimal(9, 2)')).toEqual('decimal');
    // An enum whose values name other types still maps through the enum
    expect(generic('Enum8(\'Date\' = 1, \'Int\' = 2)')).toEqual('text');
    expect(generic('Enum16(\'hello\' = 1, \'world\' = 1000)')).toEqual('text');
  });

  it('unwraps Nullable and LowCardinality', () => {
    expect(generic('Nullable(Int64)')).toEqual('bigint');
    expect(generic('Nullable(DateTime(\'UTC\'))')).toEqual('timestamp');
    expect(generic('LowCardinality(String)')).toEqual('text');
    expect(generic('LowCardinality(Nullable(String))')).toEqual('text');
  });

  it('resolves the element type of an array', () => {
    expect(generic('Array(Int32)')).toEqual('int[]');
    expect(generic('Array(DateTime)')).toEqual('timestamp[]');
    expect(generic('Array(Nullable(String))')).toEqual('text[]');
    expect(generic('Array(Array(Int32))')).toEqual('int[][]');
  });

  it('maps keyed containers to text', () => {
    expect(generic('Map(String, Int32)')).toEqual('text');
    expect(generic('Tuple(Int32, String)')).toEqual('text');
  });
});
