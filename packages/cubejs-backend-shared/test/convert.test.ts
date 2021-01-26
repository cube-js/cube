import { formatDuration } from '../src';

test('formatDuration', () => {
  expect(formatDuration(1)).toBe('00:00:01');
  expect(formatDuration(125)).toBe('00:02:05');
  expect(formatDuration((60 * 60 * 5) + 60 + 5)).toBe('05:01:05');
});
