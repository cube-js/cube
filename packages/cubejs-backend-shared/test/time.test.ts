import { inDbTimeZone, timeSeries } from '../src';

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

  it('inDbTimeZone', () => {
    expect(inDbTimeZone('UTC', 'YYYY-MM-DD[T]HH:mm:ss.SSSSSS[Z]', '2020-01-01T00:00:00.000000')).toEqual(
      '2020-01-01T00:00:00.000000Z'
    );

    expect(inDbTimeZone('UTC', 'YYYY-MM-DD[T]HH:mm:ss.SSSSSS[Z]', '2020-01-31T23:59:59.999999')).toEqual(
      '2020-01-31T23:59:59.999999Z'
    );
  });
});
