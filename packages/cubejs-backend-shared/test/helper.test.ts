import { getRealType } from '../src';

test('getRealType', () => {
  expect(getRealType(1)).toBe('number');
  expect(getRealType({})).toBe('object');
  expect(getRealType(null)).toBe('null');
});
