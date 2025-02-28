import moment from 'moment-timezone';
import { localTimestampToUtc, timeSeries, isPredefinedGranularity, timeSeriesFromCustomInterval } from '../src';

describe('time', () => {
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

  it('inDbTimeZone', () => {
    expect(localTimestampToUtc('UTC', 'YYYY-MM-DD[T]HH:mm:ss.SSSSSS[Z]', '2020-01-01T00:00:00.000000')).toEqual(
      '2020-01-01T00:00:00.000000Z'
    );

    expect(localTimestampToUtc('UTC', 'YYYY-MM-DD[T]HH:mm:ss.SSSSSS[Z]', '2020-01-31T23:59:59.999999')).toEqual(
      '2020-01-31T23:59:59.999999Z'
    );
  });

  it('isPredefinedGranularity', () => {
    expect(isPredefinedGranularity('day')).toBeTruthy();
    expect(isPredefinedGranularity('fiscal_year_by_1st_feb')).toBeFalsy();
  });
});
