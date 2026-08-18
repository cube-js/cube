import { ApiError } from '@cubejs-backend/shared';
import { displayCliUpdateSuggestion, findMaxVersion, isCliApiError } from '../src/utils';

test('findMaxVersion', () => {
  expect(findMaxVersion(['0.21.2', '0.22.3']).version).toBe('0.22.3');
  expect(findMaxVersion(['0.22.3', '0.21.2']).version).toBe('0.22.3');
});

test('isCliApiError', () => {
  expect(isCliApiError(new ApiError('HTTP error! status: 500', 500))).toBe(true);
  // Errors thrown by packages that don't use ApiError yet
  expect(isCliApiError(new Error('HTTP error! status: 404'))).toBe(true);
  expect(isCliApiError(new Error('Auth isn\'t set'))).toBe(false);
  expect(isCliApiError(undefined)).toBe(false);
});

test('displayCliUpdateSuggestion', () => {
  const consoleError = jest.spyOn(console, 'error').mockImplementation(() => undefined);

  try {
    displayCliUpdateSuggestion();

    expect(consoleError).toHaveBeenCalledTimes(1);
    expect(consoleError.mock.calls[0][0]).toContain('npm install -g cubejs-cli@latest');
  } finally {
    consoleError.mockRestore();
  }
});
