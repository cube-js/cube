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

describe('validateOptions chatCompletion', () => {
  // The schema rejects unknown keys, so without an entry for `chatCompletion`
  // every deployment declaring Cube Cloud's LLM Gateway hook fails to start
  // with "Invalid cube-server-core options" — a boot failure, not a warning.
  test('accepts a function', () => {
    expect(() => validateOptions({ chatCompletion: () => [] })).not.toThrow();
  });

  test('accepts a model instance rather than a factory', () => {
    expect(() => validateOptions({ chatCompletion: { stream: () => [] } })).not.toThrow();
  });

  test('preserves the hook by reference', () => {
    const chatCompletion = () => [];

    expect(validateOptions({ chatCompletion }).chatCompletion).toBe(chatCompletion);
  });

  test('still rejects an unknown option', () => {
    expect(() => validateOptions({ chatCompletionn: () => [] } as any))
      .toThrow(/chatCompletionn/);
  });
});
