import { isInt64Literal } from '../../src/adapter/numericLiteral';

describe('numericLiteral', () => {
  it('isInt64Literal checks the bigint range', () => {
    expect(isInt64Literal('9223372036854775807')).toBe(true);
    expect(isInt64Literal('-9223372036854775808')).toBe(true);
    expect(isInt64Literal('9223372036854775808')).toBe(false);
    expect(isInt64Literal('1.5')).toBe(false);
  });
});
