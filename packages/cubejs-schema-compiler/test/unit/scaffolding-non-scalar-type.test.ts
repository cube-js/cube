import { isNonScalarColumnType } from '../../src/scaffolding/utils';

describe('isNonScalarColumnType', () => {
  it.each([
    'jsonb', 'json', 'JSON', 'VARIANT', 'OBJECT', 'ARRAY', 'text[]', 'VARCHAR[]',
    'ARRAY<STRING>', 'STRUCT<a INT64>', 'RECORD', 'map<string,int>', 'Map(String, UInt8)',
    'Array(String)', 'Nullable(Array(String))', 'Tuple(String, UInt8)', 'bytea', 'BYTES',
    'BINARY', 'varbinary(255)', 'longblob', 'geometry', 'GEOGRAPHY', 'SUPER',
    'row(x varchar)', 'VARCHAR[3]', 'point', 'MULTIPOLYGON', 'RAW(16)', 'xml',
    'Array(DateTime)',
  ])('treats %s as non-scalar', (dbType) => {
    expect(isNonScalarColumnType(dbType)).toBe(true);
  });

  it.each([
    'character varying', 'varchar(255)', 'text', 'STRING', 'String', 'Nullable(String)',
    'LowCardinality(String)', 'uuid', 'boolean', 'timestamp', 'timestamp with time zone',
    'date', 'USER-DEFINED', 'enum', 'integer', 'binary_float', 'BINARY_DOUBLE',
    'sql_variant', 'rowversion', "Enum8('record' = 1, 'draft' = 2)", "ENUM('map', 'list')",
    "LowCardinality(Enum8('object' = 1, 'it''s json' = 2))", undefined, '',
  ])('treats %s as scalar', (dbType) => {
    expect(isNonScalarColumnType(dbType)).toBe(false);
  });
});
