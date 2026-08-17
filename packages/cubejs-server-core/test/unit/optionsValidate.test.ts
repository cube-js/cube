import { validateOptions } from '../../src/core/optionsValidate';

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
