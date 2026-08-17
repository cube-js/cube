import optionsValidate from '../../src/core/optionsValidate';

describe('optionsValidate scheduledRefreshTimeZones', () => {
  test('accepts valid IANA timezones', () => {
    expect(() => optionsValidate({ scheduledRefreshTimeZones: ['UTC', 'America/New_York'] })).not.toThrow();
  });

  test('accepts a function', () => {
    expect(() => optionsValidate({ scheduledRefreshTimeZones: async () => ['UTC'] })).not.toThrow();
  });

  test('accepts timezones case-insensitively', () => {
    expect(() => optionsValidate({ scheduledRefreshTimeZones: ['utc', 'america/new_york'] })).not.toThrow();
  });

  test('rejects an invalid timezone with a descriptive message', () => {
    expect(() => optionsValidate({ scheduledRefreshTimeZones: ['Not/AZone'] }))
      .toThrow(/valid IANA time zone name/);
  });

  test('names the offending value in the error message', () => {
    expect(() => optionsValidate({ scheduledRefreshTimeZones: ['UTC', 'Europ/Berlin'] }))
      .toThrow(/Europ\/Berlin/);
  });
});
