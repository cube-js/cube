import { describe, expect, test } from 'vitest';

import { SnowflakeDriver } from '../src';

// Unit tests: no Snowflake connection is opened, so these run without credentials.
// `toGenericType` is protected, hence the casts.
function genericType(columnType: string): string {
  const driver = new SnowflakeDriver({});
  return (driver as any).toGenericType(columnType);
}

describe('SnowflakeDriver type mapping', () => {
  // Every TIMESTAMP variant must reach Cube Store as `timestamp`. TIMESTAMP_TZ/LTZ used to
  // fall through BaseDriver.toGenericType()'s `|| columnType` default and arrive as the
  // literal type name, which Cube Store rejects when creating the pre-aggregation table
  // ("Custom type 'timestamp_tz' is not supported").
  test.each([
    'TIMESTAMP_NTZ',
    'TIMESTAMP_LTZ',
    'TIMESTAMP_TZ',
  ])('maps %s to timestamp', (columnType) => {
    expect(genericType(columnType)).toEqual('timestamp');
  });

  // INFORMATION_SCHEMA.COLUMNS.DATA_TYPE reports upper case (`TIMESTAMP_TZ`) while the
  // snowflake-sdk column type is lower case (`timestamp_tz`); both must resolve.
  test.each([
    'timestamp_ntz',
    'timestamp_ltz',
    'timestamp_tz',
  ])('maps %s to timestamp regardless of case', (columnType) => {
    expect(genericType(columnType)).toEqual('timestamp');
  });

  // Regression guard for the actual failure mode: a TIMESTAMP variant must never be
  // returned as its raw Snowflake name. (Types like DATE/TEXT/BOOLEAN legitimately map to
  // the same word, so this only applies to the timestamps.)
  test.each([
    'TIMESTAMP_NTZ',
    'TIMESTAMP_LTZ',
    'TIMESTAMP_TZ',
  ])('does not pass %s through verbatim', (columnType) => {
    expect(genericType(columnType).toLowerCase()).not.toEqual(columnType.toLowerCase());
  });

  // Guard the pre-existing mappings so the additions above cannot shadow them.
  test.each([
    ['NUMBER', 'decimal'],
    ['fixed', 'decimal'],
    ['object', 'HLL_SNOWFLAKE'],
    ['DATE', 'date'],
    ['TEXT', 'text'],
    ['VARCHAR', 'text'],
    ['BOOLEAN', 'boolean'],
  ])('still maps %s to %s', (columnType, expected) => {
    expect(genericType(columnType)).toEqual(expected);
  });
});
