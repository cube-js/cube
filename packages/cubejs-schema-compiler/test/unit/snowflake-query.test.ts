import { SnowflakeQuery } from '../../src/adapter/SnowflakeQuery';

describe('SnowflakeQuery', () => {
  it('provides a named timezone timestamptz cast template', () => {
    const templates = SnowflakeQuery.prototype.sqlTemplates();

    expect(templates.expressions.timestamp_tz_named_timezone_cast).toEqual(
      'CONVERT_TIMEZONE(\'{{ timezone }}\', \'UTC\', \'{{ timestamp }}\'::timestamp_ntz)'
    );
  });
});
