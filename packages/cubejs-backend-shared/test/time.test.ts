import moment from 'moment-timezone';
import {
  localTimestampToUtc,
  timeSeries,
  isPredefinedGranularity,
  timeSeriesFromCustomInterval,
  parseUtcIntoLocalDate,
  utcToLocalTimeZone,
  addSecondsToLocalTimestamp,
  reformatInIsoLocal,
} from '../src';

describe('timeSeries', () => {
  it('time series - day', () => {
    expect(timeSeries('day', ['2021-01-01', '2021-01-02'])).toEqual([
      ['2021-01-01T00:00:00.000', '2021-01-01T23:59:59.999'],
      ['2021-01-02T00:00:00.000', '2021-01-02T23:59:59.999']
    ]);

    expect(timeSeries('day', ['2021-01-01', '2021-01-02'], { timestampPrecision: 6 })).toEqual([
      ['2021-01-01T00:00:00.000000', '2021-01-01T23:59:59.999999'],
      ['2021-01-02T00:00:00.000000', '2021-01-02T23:59:59.999999']
    ]);
  });

  it('time series - month', () => {
    expect(timeSeries('month', ['2021-01-01', '2021-06-01'])).toEqual([
      ['2021-01-01T00:00:00.000', '2021-01-31T23:59:59.999'],
      ['2021-02-01T00:00:00.000', '2021-02-28T23:59:59.999'],
      ['2021-03-01T00:00:00.000', '2021-03-31T23:59:59.999'],
      ['2021-04-01T00:00:00.000', '2021-04-30T23:59:59.999'],
      ['2021-05-01T00:00:00.000', '2021-05-31T23:59:59.999'],
      ['2021-06-01T00:00:00.000', '2021-06-30T23:59:59.999'],
    ]);
  });

  it('time series - year', () => {
    expect(timeSeries('year', ['2019-01-01', '2021-12-31'])).toEqual([
      ['2019-01-01T00:00:00.000', '2019-12-31T23:59:59.999'],
      ['2020-01-01T00:00:00.000', '2020-12-31T23:59:59.999'],
      ['2021-01-01T00:00:00.000', '2021-12-31T23:59:59.999'],
    ]);
  });

  it('time series - hour', () => {
    expect(timeSeries('hour', ['2021-01-01T10:00:00', '2021-01-01T12:00:00'])).toEqual([
      ['2021-01-01T10:00:00.000', '2021-01-01T10:59:59.999'],
      ['2021-01-01T11:00:00.000', '2021-01-01T11:59:59.999'],
      ['2021-01-01T12:00:00.000', '2021-01-01T12:59:59.999'],
    ]);
  });

  it('time series - minute', () => {
    expect(timeSeries('minute', ['2021-01-01T10:00:00', '2021-01-01T10:02:00'])).toEqual([
      ['2021-01-01T10:00:00.000', '2021-01-01T10:00:59.999'],
      ['2021-01-01T10:01:00.000', '2021-01-01T10:01:59.999'],
      ['2021-01-01T10:02:00.000', '2021-01-01T10:02:59.999'],
    ]);
  });

  it('time series - second', () => {
    expect(timeSeries('second', ['2021-01-01T10:00:00', '2021-01-01T10:00:02'])).toEqual([
      ['2021-01-01T10:00:00.000', '2021-01-01T10:00:00.999'],
      ['2021-01-01T10:00:01.000', '2021-01-01T10:00:01.999'],
      ['2021-01-01T10:00:02.000', '2021-01-01T10:00:02.999'],
    ]);
  });

  it('time series - week', () => {
    expect(timeSeries('week', ['2021-01-01', '2021-01-31'])).toEqual([
      ['2020-12-28T00:00:00.000', '2021-01-03T23:59:59.999'],
      ['2021-01-04T00:00:00.000', '2021-01-10T23:59:59.999'],
      ['2021-01-11T00:00:00.000', '2021-01-17T23:59:59.999'],
      ['2021-01-18T00:00:00.000', '2021-01-24T23:59:59.999'],
      ['2021-01-25T00:00:00.000', '2021-01-31T23:59:59.999'],
    ]);
  });

  it('time series - quarter', () => {
    expect(timeSeries('quarter', ['2021-01-01', '2021-12-31'])).toEqual([
      ['2021-01-01T00:00:00.000', '2021-03-31T23:59:59.999'],
      ['2021-04-01T00:00:00.000', '2021-06-30T23:59:59.999'],
      ['2021-07-01T00:00:00.000', '2021-09-30T23:59:59.999'],
      ['2021-10-01T00:00:00.000', '2021-12-31T23:59:59.999'],
    ]);

    expect(timeSeries('quarter', ['2021-01-01', '2021-12-31'], { timestampPrecision: 6 })).toEqual([
      ['2021-01-01T00:00:00.000000', '2021-03-31T23:59:59.999999'],
      ['2021-04-01T00:00:00.000000', '2021-06-30T23:59:59.999999'],
      ['2021-07-01T00:00:00.000000', '2021-09-30T23:59:59.999999'],
      ['2021-10-01T00:00:00.000000', '2021-12-31T23:59:59.999999'],
    ]);
  });

  it('should throw an error for unsupported granularity', () => {
    expect(() => timeSeries('decade', ['2020-01-01', '2030-01-01'])).toThrowError(
      'Unsupported time granularity: decade'
    );
  });

  it('should throw an error if timestampPrecision is missing', () => {
    expect(() => timeSeries('day', ['2021-01-01', '2021-01-02'], { timestampPrecision: 0 })).toThrowError(
      'options.timestampPrecision is required, actual: 0'
    );
  });

  it('time series - reach limits', () => {
    expect(() => {
      timeSeries('second', ['1970-01-01', '2021-01-02']);
    }).toThrowError(/The count of generated date ranges.*for the request.*is over limit/);
  });

  it('time series - custom: interval - 1 year, origin - 2021-01-01', () => {
    expect(timeSeriesFromCustomInterval('1 year', ['2021-01-01', '2023-12-31'], moment('2021-01-01'))).toEqual([
      ['2021-01-01T00:00:00.000', '2021-12-31T23:59:59.999'],
      ['2022-01-01T00:00:00.000', '2022-12-31T23:59:59.999'],
      ['2023-01-01T00:00:00.000', '2023-12-31T23:59:59.999']
    ]);
  });

  it('time series - custom: interval - 1 year, origin - 2020-01-01', () => {
    expect(timeSeriesFromCustomInterval('1 year', ['2021-01-01', '2023-12-31'], moment('2020-01-01'))).toEqual([
      ['2021-01-01T00:00:00.000', '2021-12-31T23:59:59.999'],
      ['2022-01-01T00:00:00.000', '2022-12-31T23:59:59.999'],
      ['2023-01-01T00:00:00.000', '2023-12-31T23:59:59.999']
    ]);
  });

  it('time series - custom: interval - 1 year, origin - 2025-01-01', () => {
    expect(timeSeriesFromCustomInterval('1 year', ['2021-01-01', '2023-12-31'], moment('2025-01-01'))).toEqual([
      ['2021-01-01T00:00:00.000', '2021-12-31T23:59:59.999'],
      ['2022-01-01T00:00:00.000', '2022-12-31T23:59:59.999'],
      ['2023-01-01T00:00:00.000', '2023-12-31T23:59:59.999']
    ]);
  });

  it('time series - custom: interval - 1 year, origin - 2025-03-01', () => {
    expect(timeSeriesFromCustomInterval('1 year', ['2021-01-01', '2022-12-31'], moment('2025-03-01'))).toEqual([
      ['2020-03-01T00:00:00.000', '2021-02-28T23:59:59.999'],
      ['2021-03-01T00:00:00.000', '2022-02-28T23:59:59.999'],
      ['2022-03-01T00:00:00.000', '2023-02-28T23:59:59.999']
    ]);
  });

  it('time series - custom: interval - 1 year, origin - 2015-03-01', () => {
    expect(timeSeriesFromCustomInterval('1 year', ['2021-01-01', '2022-12-31'], moment('2015-03-01'))).toEqual([
      ['2020-03-01T00:00:00.000', '2021-02-28T23:59:59.999'],
      ['2021-03-01T00:00:00.000', '2022-02-28T23:59:59.999'],
      ['2022-03-01T00:00:00.000', '2023-02-28T23:59:59.999']
    ]);
  });

  it('time series - custom: interval - 1 year, origin - 2020-03-15', () => {
    expect(timeSeriesFromCustomInterval('1 year', ['2021-01-01', '2022-12-31'], moment('2020-03-15'))).toEqual([
      ['2020-03-15T00:00:00.000', '2021-03-14T23:59:59.999'],
      ['2021-03-15T00:00:00.000', '2022-03-14T23:59:59.999'],
      ['2022-03-15T00:00:00.000', '2023-03-14T23:59:59.999']
    ]);
  });

  it('time series - custom: interval - 1 year, origin - 2019-03-15', () => {
    expect(timeSeriesFromCustomInterval('1 year', ['2021-01-01', '2022-12-31'], moment('2019-03-15'))).toEqual([
      ['2020-03-15T00:00:00.000', '2021-03-14T23:59:59.999'],
      ['2021-03-15T00:00:00.000', '2022-03-14T23:59:59.999'],
      ['2022-03-15T00:00:00.000', '2023-03-14T23:59:59.999']
    ]);
  });

  it('time series - custom: interval - 2 months, origin - 2019-01-01', () => {
    expect(timeSeriesFromCustomInterval('2 months', ['2021-01-01', '2021-12-31'], moment('2019-01-01'))).toEqual([
      ['2021-01-01T00:00:00.000', '2021-02-28T23:59:59.999'],
      ['2021-03-01T00:00:00.000', '2021-04-30T23:59:59.999'],
      ['2021-05-01T00:00:00.000', '2021-06-30T23:59:59.999'],
      ['2021-07-01T00:00:00.000', '2021-08-31T23:59:59.999'],
      ['2021-09-01T00:00:00.000', '2021-10-31T23:59:59.999'],
      ['2021-11-01T00:00:00.000', '2021-12-31T23:59:59.999']
    ]);
  });

  it('time series - custom: interval - 2 months, origin - 2019-03-15', () => {
    expect(timeSeriesFromCustomInterval('2 months', ['2021-01-01', '2021-12-31'], moment('2019-03-15'))).toEqual([
      ['2020-11-15T00:00:00.000', '2021-01-14T23:59:59.999'],
      ['2021-01-15T00:00:00.000', '2021-03-14T23:59:59.999'],
      ['2021-03-15T00:00:00.000', '2021-05-14T23:59:59.999'],
      ['2021-05-15T00:00:00.000', '2021-07-14T23:59:59.999'],
      ['2021-07-15T00:00:00.000', '2021-09-14T23:59:59.999'],
      ['2021-09-15T00:00:00.000', '2021-11-14T23:59:59.999'],
      ['2021-11-15T00:00:00.000', '2022-01-14T23:59:59.999']
    ]);
  });

  it('time series - custom: interval - 1 months 2 weeks 3 days, origin - 2021-01-25', () => {
    expect(timeSeriesFromCustomInterval('1 months 2 weeks 3 days', ['2021-01-01', '2021-12-31'], moment('2021-01-25'))).toEqual([
      ['2020-12-08T00:00:00.000', '2021-01-24T23:59:59.999'],
      ['2021-01-25T00:00:00.000', '2021-03-13T23:59:59.999'],
      ['2021-03-14T00:00:00.000', '2021-04-30T23:59:59.999'],
      ['2021-05-01T00:00:00.000', '2021-06-17T23:59:59.999'],
      ['2021-06-18T00:00:00.000', '2021-08-03T23:59:59.999'],
      ['2021-08-04T00:00:00.000', '2021-09-20T23:59:59.999'],
      ['2021-09-21T00:00:00.000', '2021-11-06T23:59:59.999'],
      ['2021-11-07T00:00:00.000', '2021-12-23T23:59:59.999'],
      ['2021-12-24T00:00:00.000', '2022-02-09T23:59:59.999'],
    ]);
  });

  it('time series - custom: interval - 3 weeks, origin - 2020-12-15', () => {
    expect(timeSeriesFromCustomInterval('3 weeks', ['2021-01-01', '2021-03-01'], moment('2020-12-15'))).toEqual([
      ['2020-12-15T00:00:00.000', '2021-01-04T23:59:59.999'],
      ['2021-01-05T00:00:00.000', '2021-01-25T23:59:59.999'],
      ['2021-01-26T00:00:00.000', '2021-02-15T23:59:59.999'],
      ['2021-02-16T00:00:00.000', '2021-03-08T23:59:59.999']
    ]);
  });

  it('time series - custom: interval - 6 months, origin - 2021-01-01', () => {
    expect(timeSeriesFromCustomInterval('6 months', ['2021-01-01', '2021-12-31'], moment('2021-01-01'))).toEqual([
      ['2021-01-01T00:00:00.000', '2021-06-30T23:59:59.999'],
      ['2021-07-01T00:00:00.000', '2021-12-31T23:59:59.999']
    ]);
  });

  it('time series - custom: interval - 2 months 3 weeks 4 days 5 hours 6 minutes 7 seconds, origin - 2021-01-01', () => {
    expect(timeSeriesFromCustomInterval('2 months 3 weeks 4 days 5 hours 6 minutes 7 seconds', ['2021-01-01', '2021-12-31'], moment('2021-01-01'))).toEqual([
      ['2021-01-01T00:00:00.000', '2021-03-26T05:06:06.999'],
      ['2021-03-26T05:06:07.000', '2021-06-20T10:12:13.999'],
      ['2021-06-20T10:12:14.000', '2021-09-14T15:18:20.999'],
      ['2021-09-14T15:18:21.000', '2021-12-09T20:24:27.999'],
      ['2021-12-09T20:24:28.000', '2022-03-07T01:30:34.999']
    ]);
  });

  it('time series - custom: interval - 10 minutes 15 seconds, origin - 2021-02-01 09:59:45', () => {
    expect(timeSeriesFromCustomInterval('10 minutes 15 seconds', ['2021-02-01 10:00:00', '2021-02-01 12:00:00'], moment('2021-02-01 09:59:45'))).toEqual([
      ['2021-02-01T09:59:45.000', '2021-02-01T10:09:59.999'],
      ['2021-02-01T10:10:00.000', '2021-02-01T10:20:14.999'],
      ['2021-02-01T10:20:15.000', '2021-02-01T10:30:29.999'],
      ['2021-02-01T10:30:30.000', '2021-02-01T10:40:44.999'],
      ['2021-02-01T10:40:45.000', '2021-02-01T10:50:59.999'],
      ['2021-02-01T10:51:00.000', '2021-02-01T11:01:14.999'],
      ['2021-02-01T11:01:15.000', '2021-02-01T11:11:29.999'],
      ['2021-02-01T11:11:30.000', '2021-02-01T11:21:44.999'],
      ['2021-02-01T11:21:45.000', '2021-02-01T11:31:59.999'],
      ['2021-02-01T11:32:00.000', '2021-02-01T11:42:14.999'],
      ['2021-02-01T11:42:15.000', '2021-02-01T11:52:29.999'],
      ['2021-02-01T11:52:30.000', '2021-02-01T12:02:44.999']
    ]);
  });

  it('time series - custom: interval - reach limits', () => {
    expect(() => {
      timeSeriesFromCustomInterval('10 minutes 15 seconds', ['1970-01-01', '2021-01-02'], moment('2021-02-01 09:59:45'));
    }).toThrowError(/The count of generated date ranges.*for the request.*is over limit/);
  });
});

describe('predefinedGranularity', () => {
  it('isPredefinedGranularity', () => {
    expect(isPredefinedGranularity('day')).toBeTruthy();
    expect(isPredefinedGranularity('fiscal_year_by_1st_feb')).toBeFalsy();
  });
});

describe('extractDate', () => {
  const timezone = 'Europe/Kiev';

  it('should return null if data is empty', () => {
    expect(parseUtcIntoLocalDate(null, timezone)).toBeNull();
    expect(parseUtcIntoLocalDate(undefined, timezone)).toBeNull();
    expect(parseUtcIntoLocalDate([], timezone)).toBeNull();
  });

  it('should return null if no valid date is found in data', () => {
    expect(parseUtcIntoLocalDate([{}], timezone)).toBeNull();
    expect(parseUtcIntoLocalDate([{ someKey: 'invalid date' }], timezone)).toBeNull();
  });

  it('should throw an error for unknown timezone', () => {
    const input = [{ date: '2025-02-28T12:00:00+03:00' }];
    expect(() => parseUtcIntoLocalDate(input, 'Invalid/Timezone'))
      .toThrowError('Unknown timezone: Invalid/Timezone');
  });

  it('should parse a date with UTC timezone', () => {
    const input = [{ date: '2025-02-28T12:00:00Z' }];
    const result = parseUtcIntoLocalDate(input, timezone);
    expect(result).toBe('2025-02-28T14:00:00.000');
  });

  it('should parse a date with an offset timezone', () => {
    const input = [{ date: '2025-02-28T12:00:00+03:00' }];
    const result = parseUtcIntoLocalDate(input, timezone);
    expect(result).toBe('2025-02-28T11:00:00.000');
  });

  it('should parse a date without timezone as UTC', () => {
    const input = [{ date: '2025-02-28 12:00:00' }];
    const result = parseUtcIntoLocalDate(input, timezone);
    expect(result).toBe('2025-02-28T14:00:00.000');
  });

  it('should handle multiple formats', () => {
    const input1 = [{ date: '2025-02-28 12:00:00' }];
    const input2 = [{ date: '2025-02-28T12:00:00.000' }];
    const input3 = [{ date: '2025-02-28T12:00:00Z' }];
    const input4 = [{ date: '2025-02-28T12:00:00+03:00' }];

    expect(parseUtcIntoLocalDate(input1, timezone)).toBe('2025-02-28T14:00:00.000');
    expect(parseUtcIntoLocalDate(input2, timezone)).toBe('2025-02-28T14:00:00.000');
    expect(parseUtcIntoLocalDate(input3, timezone)).toBe('2025-02-28T14:00:00.000');
    expect(parseUtcIntoLocalDate(input4, timezone)).toBe('2025-02-28T11:00:00.000');
  });
});

describe('localTimestampToUtc', () => {
  it('should return null if timestamp is empty', () => {
    expect(localTimestampToUtc('UTC', 'YYYY-MM-DDTHH:mm:ss.SSS', '')).toBeNull();
    expect(localTimestampToUtc('UTC', 'YYYY-MM-DDTHH:mm:ss.SSS')).toBeNull();
  });

  it('should throw an error for unknown timezone', () => {
    expect(() => localTimestampToUtc('Invalid/Timezone', 'YYYY-MM-DDTHH:mm:ss.SSS', '2025-02-28T12:00:00.000'))
      .toThrowError('Unknown timezone: Invalid/Timezone');
  });

  it('should convert timestamp with timezone to UTC for format YYYY-MM-DD[T]HH:mm:ss.SSS[Z]', () => {
    const timestamp = '2025-02-28T11:00:00.000';
    const timezone = 'Europe/Kiev';
    const result = localTimestampToUtc(timezone, 'YYYY-MM-DD[T]HH:mm:ss.SSS[Z]', timestamp);
    expect(result).toBe('2025-02-28T09:00:00.000Z');
  });

  it('should convert timestamp with timezone to UTC for format YYYY-MM-DDTHH:mm:ss.SSSZ', () => {
    const timestamp = '2025-02-28T11:00:00.000';
    const timezone = 'Europe/Kiev';
    const result = localTimestampToUtc(timezone, 'YYYY-MM-DDTHH:mm:ss.SSSZ', timestamp);
    expect(result).toBe('2025-02-28T09:00:00.000Z');
  });

  it('should convert timestamp with microseconds to UTC for format YYYY-MM-DD[T]HH:mm:ss.SSSSSS[Z]', () => {
    const timezone = 'Europe/Kiev';
    let timestamp = '2025-02-28T11:00:00.123456';
    let result = localTimestampToUtc(timezone, 'YYYY-MM-DD[T]HH:mm:ss.SSSSSS[Z]', timestamp);
    expect(result).toBe('2025-02-28T09:00:00.123000Z'); // microseconds are zeroed :(

    timestamp = '2025-02-28T11:00:00.000000';
    result = localTimestampToUtc(timezone, 'YYYY-MM-DD[T]HH:mm:ss.SSSSSS[Z]', timestamp);
    expect(result).toBe('2025-02-28T09:00:00.000000Z'); // microseconds are zeroed :(

    timestamp = '2025-02-28T11:00:00.999999';
    result = localTimestampToUtc(timezone, 'YYYY-MM-DD[T]HH:mm:ss.SSSSSS[Z]', timestamp);
    expect(result).toBe('2025-02-28T09:00:00.999999Z'); // microseconds are zeroed :(
  });

  it('should convert timestamp with timezone to UTC for format YYYY-MM-DDTHH:mm:ss.SSSSSS', () => {
    const timezone = 'Europe/Kiev';
    let timestamp = '2025-02-28T11:00:00.123456';
    let result = localTimestampToUtc(timezone, 'YYYY-MM-DDTHH:mm:ss.SSSSSS', timestamp);
    expect(result).toBe('2025-02-28T09:00:00.123000');

    timestamp = '2025-02-28T11:00:00.000000';
    result = localTimestampToUtc(timezone, 'YYYY-MM-DDTHH:mm:ss.SSSSSS', timestamp);
    expect(result).toBe('2025-02-28T09:00:00.000000');

    timestamp = '2025-02-28T11:00:00.999999';
    result = localTimestampToUtc(timezone, 'YYYY-MM-DDTHH:mm:ss.SSSSSS', timestamp);
    expect(result).toBe('2025-02-28T09:00:00.999999');
  });

  it('should convert timestamp without timezone to UTC for format YYYY-MM-DDTHH:mm:ss.SSS', () => {
    const timestamp = '2025-02-28T12:00:00.000';
    const timezone = 'UTC';
    const result = localTimestampToUtc(timezone, 'YYYY-MM-DDTHH:mm:ss.SSS', timestamp);
    expect(result).toBe('2025-02-28T12:00:00.000'); // UTC
  });

  it('should correctly handle timestamp without time zone', () => {
    const timestamp = '2025-02-28T12:00:00.000';
    const timezone = 'America/New_York';
    const result = localTimestampToUtc(timezone, 'YYYY-MM-DDTHH:mm:ss.SSS', timestamp);
    expect(result).toBe('2025-02-28T17:00:00.000'); // America/New_York is UTC-5 during daylight saving time
  });
});

describe('utcToLocalTimeZone', () => {
  it('should return null if no timestamp is provided', () => {
    expect(utcToLocalTimeZone('Europe/Kiev', 'YYYY-MM-DDTHH:mm:ss.SSS', undefined)).toBeNull();
  });

  it('should throw an error for an unknown timezone', () => {
    expect(() => utcToLocalTimeZone('Unknown/Zone', 'YYYY-MM-DDTHH:mm:ss.SSS', '2025-02-28T10:00:00.000'))
      .toThrow('Unknown timezone: Unknown/Zone');
  });

  it('should convert UTC to specified timezone with timestamp format "YYYY-MM-DDTHH:mm:ss.SSS"', () => {
    const timestamp = '2025-02-28T10:00:00.000';
    const expected = '2025-02-28T12:00:00.000';

    expect(utcToLocalTimeZone('Europe/Kiev', 'YYYY-MM-DDTHH:mm:ss.SSS', timestamp)).toBe(expected);
  });

  it('should convert UTC to specified timezone with timestamp format "YYYY-MM-DD[T]HH:mm:ss.SSS[Z]"', () => {
    const timestamp = '2025-02-28T10:00:00.000';
    const expected = '2025-02-28T12:00:00.000Z';

    expect(utcToLocalTimeZone('Europe/Kiev', 'YYYY-MM-DD[T]HH:mm:ss.SSS[Z]', timestamp)).toBe(expected);
  });

  it('should handle timestamps with microseconds correctly', () => {
    const timestamp = '2025-02-28T10:00:00.123456Z';
    const expected = '2025-02-28T12:00:00.123000'; // microseconds are zeroed

    expect(utcToLocalTimeZone('Europe/Kiev', 'YYYY-MM-DDTHH:mm:ss.SSSSSS', timestamp)).toBe(expected);
  });

  it('should handle UTC timestamp and correctly shift to target timezone', () => {
    const timestamp = '2025-02-28T12:00:00.000Z';
    const expected = '2025-02-28T14:00:00.000';

    expect(utcToLocalTimeZone('Europe/Kiev', 'YYYY-MM-DDTHH:mm:ss.SSS', timestamp)).toBe(expected);
  });

  it('should correctly handle UTC to timezone conversion with timestamp format without milliseconds', () => {
    const timestamp = '2025-02-28T10:00:00Z';
    const expected = '2025-02-28T12:00:00';

    expect(utcToLocalTimeZone('Europe/Kiev', 'YYYY-MM-DDTHH:mm:ss', timestamp)).toBe(expected);
  });

  it('should return the local time as UTC format if no milliseconds are present', () => {
    const timestamp = '2025-02-28T10:00:00Z';
    const expected = '2025-02-28T12:00:00.000Z';

    expect(utcToLocalTimeZone('Europe/Kiev', 'YYYY-MM-DD[T]HH:mm:ss.SSS[Z]', timestamp)).toBe(expected);
  });
});

describe('addSecondsToLocalTimestamp', () => {
  it('should throw an error for an unknown timezone', () => {
    const timestamp = '2025-02-28T12:00:00.000';
    const unknownTimezone = 'Unknown/Zone';
    const seconds = 10;

    expect(() => addSecondsToLocalTimestamp(timestamp, unknownTimezone, seconds))
      .toThrow('Unknown timezone: Unknown/Zone');
  });

  it('should correctly add seconds to a timestamp in the specified timezone', () => {
    const timestamp = '2025-02-28T12:00:00.000';
    const timezone = 'Europe/Kiev';
    const seconds = 10;
    const expected = new Date('2025-02-28T12:00:10.000+0200');

    expect(addSecondsToLocalTimestamp(timestamp, timezone, seconds)).toEqual(expected);
  });

  it('should correctly add seconds to a timestamp with UTC timezone', () => {
    const timestamp = '2025-02-28T12:00:00.000Z';
    const timezone = 'Europe/Kiev';
    const seconds = 60;
    const expected = new Date('2025-02-28T14:01:00.000+0200');

    expect(addSecondsToLocalTimestamp(timestamp, timezone, seconds)).toEqual(expected);
  });

  it('should correctly add seconds to a timestamp without milliseconds', () => {
    const timestamp = '2025-02-28T12:00:00';
    const timezone = 'Europe/Kiev';
    const seconds = 30;
    const expected = new Date('2025-02-28T12:00:30.000+0200');

    expect(addSecondsToLocalTimestamp(timestamp, timezone, seconds)).toEqual(expected);
  });

  it('should correctly handle timestamp with microseconds and add seconds', () => {
    const timestamp = '2025-02-28T12:00:00.123456Z';
    const timezone = 'Europe/Kiev';
    const seconds = 60;
    const expected = new Date('2025-02-28T14:01:00.123456+0200');

    expect(addSecondsToLocalTimestamp(timestamp, timezone, seconds)).toEqual(expected);
  });

  it('should correctly add seconds to timestamp with long format', () => {
    const timestamp = '2025-02-28T12:00:00.000';
    const timezone = 'Europe/Kiev';
    const seconds = 100;
    const expected = new Date('2025-02-28T12:01:40.000+0200');

    expect(addSecondsToLocalTimestamp(timestamp, timezone, seconds)).toEqual(expected);
  });

  it('should return the same time if seconds to add is 0', () => {
    const timestamp = '2025-02-28T12:00:00.000';
    const timezone = 'Europe/Kiev';
    const seconds = 0;
    const expected = new Date('2025-02-28T12:00:00.000+0200');

    expect(addSecondsToLocalTimestamp(timestamp, timezone, seconds)).toEqual(expected);
  });
});

describe('reformatInIsoLocal', () => {
  it('should return the same timestamp if its length is 23 characters', () => {
    const timestamp = '2025-02-28T12:00:00.000';
    const expected = '2025-02-28T12:00:00.000';

    expect(reformatInIsoLocal(timestamp)).toBe(expected);
  });

  it('should return timestamp without the "Z" if its length is 24 characters', () => {
    const timestamp = '2025-02-28T12:00:00.000Z';
    const expected = '2025-02-28T12:00:00.000';

    expect(reformatInIsoLocal(timestamp)).toBe(expected);
  });

  it('should reformat timestamp in UTC to ISO 8601 local format', () => {
    const timestamp = '2025-02-28T12:00:00';
    const expected = '2025-02-28T12:00:00.000';

    expect(reformatInIsoLocal(timestamp)).toBe(expected);
  });

  it('should return the same timestamp if it is an empty string', () => {
    const timestamp = '';
    const expected = '';

    expect(reformatInIsoLocal(timestamp)).toBe(expected);
  });
});
