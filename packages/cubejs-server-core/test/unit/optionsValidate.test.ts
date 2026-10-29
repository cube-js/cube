import { validateOptions } from '../../src/core/optionsValidate';

describe('validateOptions compilerConcurrency', () => {
  test.each([undefined, 1, 2, 100])('accepts %s', (compilerConcurrency) => {
    expect(() => validateOptions({ compilerConcurrency })).not.toThrow();
  });

  test.each([0, -1, 1.5, NaN, Infinity, -Infinity, '2', null, 9007199254740992])('rejects %s', (compilerConcurrency) => {
    expect(() => validateOptions({ compilerConcurrency })).toThrow(/compilerConcurrency/);
  });
});

describe('validateOptions scheduledRefreshTimeZones', () => {
  test('accepts valid IANA timezones', () => {
    expect(() => validateOptions({ scheduledRefreshTimeZones: ['UTC', 'America/New_York'] })).not.toThrow();
  });

  test('accepts a function', () => {
    expect(() => validateOptions({ scheduledRefreshTimeZones: async () => ['UTC'] })).not.toThrow();
  });

  test('accepts timezones case-insensitively', () => {
    expect(() => validateOptions({ scheduledRefreshTimeZones: ['utc', 'america/new_york'] })).not.toThrow();
  });

  test('returns canonical timezone names', () => {
    expect(validateOptions({ scheduledRefreshTimeZones: ['utc', 'america/new_york'] }))
      .toEqual({ scheduledRefreshTimeZones: ['UTC', 'America/New_York'] });
  });

  test('rejects an invalid timezone with a descriptive message', () => {
    expect(() => validateOptions({ scheduledRefreshTimeZones: ['Not/AZone'] }))
      .toThrow(/valid IANA time zone name/);
  });

  test('names the offending value in the error message', () => {
    expect(() => validateOptions({ scheduledRefreshTimeZones: ['UTC', 'Europ/Berlin'] }))
      .toThrow(/Europ\/Berlin/);
  });
});

describe('validateOptions sanitized result', () => {
  test('preserves function options by reference', () => {
    const driverFactory = () => ({ type: 'postgres' });
    const validated = validateOptions({ driverFactory });

    expect(validated.driverFactory).toBe(driverFactory);
  });

  test('does not mutate the input', () => {
    const options = { scheduledRefreshTimeZones: ['utc'] };
    const validated = validateOptions(options);

    expect(options.scheduledRefreshTimeZones).toEqual(['utc']);
    expect(validated).not.toBe(options);
  });
});
