import { formatDuration } from '../src';

test('formatDuration', () => {
  expect(formatDuration(-1, 0)).toBe('-00:00:01');
  expect(formatDuration(59, 0)).toBe('00:00:59');
  expect(formatDuration(61, 0)).toBe('00:01:01');
  expect(formatDuration(125, 0)).toBe('00:02:05');
  expect(formatDuration(3600 + 60 + 1, 0)).toBe('01:01:01');
  expect(formatDuration(3600 * 10 + 60 + 1, 0)).toBe('10:01:01');
  expect(formatDuration(3600 * 17, 0)).toBe('17:00:00');
  expect(formatDuration(3600 * 5 + 60 + 5, 0)).toBe('05:01:05');
  expect(formatDuration(3600 * 100000, 0)).toBe('100000:00:00');

  expect(formatDuration(6738, 3)).toBe('00:00:06');
  expect(formatDuration(10331, 3)).toBe('00:00:10');
  expect(formatDuration(61331, 3)).toBe('00:01:01');
});
