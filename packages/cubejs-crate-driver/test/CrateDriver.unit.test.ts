import { CrateDriver } from '../src';

class CrateDriverExtended extends CrateDriver {
  public static inlineParams(sql: string, params: unknown[]): string {
    return super.inlineParams(sql, params);
  }

  public static formatParam(value: unknown): string {
    return super.formatParam(value);
  }
}

describe('CrateDriver', () => {
  it('formatParam formats values by type', () => {
    // strings are quoted, single quotes doubled
    expect(CrateDriverExtended.formatParam('abc')).toBe('\'abc\'');
    expect(CrateDriverExtended.formatParam('O\'Brien')).toBe('\'O\'\'Brien\'');
    expect(CrateDriverExtended.formatParam('a\'b\'c')).toBe('\'a\'\'b\'\'c\'');

    expect(CrateDriverExtended.formatParam(42)).toBe('42');
    expect(CrateDriverExtended.formatParam(3.14)).toBe('3.14');
    expect(CrateDriverExtended.formatParam(0)).toBe('0');

    expect(CrateDriverExtended.formatParam(true)).toBe('TRUE');
    expect(CrateDriverExtended.formatParam(false)).toBe('FALSE');

    expect(CrateDriverExtended.formatParam(null)).toBe('NULL');
    expect(CrateDriverExtended.formatParam(new Date('2020-01-01T00:00:00.000Z'))).toBe(
      '\'2020-01-01T00:00:00.000Z\''
    );
  });

  describe('inlineParams', () => {
    it('returns the SQL unchanged when there are no params', () => {
      expect(CrateDriverExtended.inlineParams('SELECT 1', [])).toBe('SELECT 1');
    });

    it('inlines a single string param and keeps a trailing cast', () => {
      expect(CrateDriverExtended.inlineParams('... WHERE d >= $1::timestamptz', ['2020-01-01T00:00:00.000Z']))
        .toBe('... WHERE d >= \'2020-01-01T00:00:00.000Z\'::timestamptz');
    });

    it('inlines multiple params by position', () => {
      expect(CrateDriverExtended.inlineParams('a = $1 AND b = $2', ['x', 7])).toBe('a = \'x\' AND b = 7');
    });

    it('handles multi-digit placeholders like $10', () => {
      const params = Array.from({ length: 10 }, (_, i) => i + 1);
      expect(CrateDriverExtended.inlineParams('v = $10', params)).toBe('v = 10');
    });

    it('escapes single quotes so a value cannot break the statement', () => {
      expect(CrateDriverExtended.inlineParams('name = $1', ['O\'Brien'])).toBe('name = \'O\'\'Brien\'');
    });

    it('leaves a placeholder without a matching param intact', () => {
      expect(CrateDriverExtended.inlineParams('a = $1 AND b = $2', ['only'])).toBe('a = \'only\' AND b = $2');
    });

    it('inlines the partitioned pre-aggregation CTAS time bounds', () => {
      const sql = 'CREATE TABLE pa AS (SELECT sum("t".v) FROM t AS "t" '
        + 'WHERE ("t".d >= $1::timestamptz AND "t".d <= $2::timestamptz) GROUP BY 1)';
      const out = CrateDriverExtended.inlineParams(sql, ['2020-01-01T00:00:00.000Z', '2020-01-31T23:59:59.999Z']);
      expect(out).not.toMatch(/\$\d/);
      expect(out).toContain('\'2020-01-01T00:00:00.000Z\'::timestamptz');
      expect(out).toContain('\'2020-01-31T23:59:59.999Z\'::timestamptz');
    });
  });
});
