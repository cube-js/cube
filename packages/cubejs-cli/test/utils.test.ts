import { findMaxVersion } from '../src/utils';

test('findMaxVersion', () => {
  expect(findMaxVersion(['0.21.2', '0.22.3']).version).toBe('0.22.3');
  expect(findMaxVersion(['0.22.3', '0.21.2']).version).toBe('0.22.3');
});
