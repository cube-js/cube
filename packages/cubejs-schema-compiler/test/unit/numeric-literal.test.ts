import { isInt64Literal, isNumberLiteral } from '../../src/adapter/numericLiteral';

describe('numericLiteral', () => {
  it('isNumberLiteral accepts decimal literals', () => {
    for (const value of ['1', '1.', '1.5', '.5', '-1e5', ' 2 ', '+3.E-2', 10, 1.5]) {
      expect(isNumberLiteral(value)).toBe(true);
    }
  });

  it('isNumberLiteral rejects non-decimal input', () => {
    for (const value of ['', ' ', '.', '1e', 'e5', '1.2.3', 'abc', '0x1F', 'Infinity', 'NaN', null, undefined]) {
      expect(isNumberLiteral(value)).toBe(false);
    }
  });

  it('isNumberLiteral rejects a long digit run in linear time', () => {
    const start = Date.now();
    expect(isNumberLiteral(`${'0'.repeat(100_000)}x`)).toBe(false);
    expect(Date.now() - start).toBeLessThan(100);
  });

  it('isInt64Literal checks the bigint range', () => {
    expect(isInt64Literal('9223372036854775807')).toBe(true);
    expect(isInt64Literal('-9223372036854775808')).toBe(true);
    expect(isInt64Literal('9223372036854775808')).toBe(false);
    expect(isInt64Literal('1.5')).toBe(false);
  });
});
