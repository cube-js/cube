import { timeSeries, getReversedOffset } from '../src';

describe('timeSeries', () => {
  it('day', () => {
    expect(timeSeries('day', ['2021-01-01', '2021-01-02'])).toEqual([
      ['2021-01-01T00:00:00.000', '2021-01-01T23:59:59.999'],
      ['2021-01-02T00:00:00.000', '2021-01-02T23:59:59.999']
    ]);
  });

  it('quarter', () => {
    expect(timeSeries('quarter', ['2021-01-01', '2021-12-31'])).toEqual([
      ['2021-01-01T00:00:00.000', '2021-03-31T23:59:99.999'],
      ['2021-04-01T00:00:00.000', '2021-06-30T23:59:99.999'],
      ['2021-07-01T00:00:00.000', '2021-09-30T23:59:99.999'],
      ['2021-10-01T00:00:00.000', '2021-12-31T23:59:99.999'],
    ]);
  });
});

describe('getReversedOffset', () => {
  it('Australia/Sydney', () => {
    const parsedTime = Date.parse(`2013-11-18T19:55:00.000Z`);
    const timezone = 'Australia/Sydney'
    expect(getReversedOffset(parsedTime, timezone)).toEqual(-660);
  });

  it('+11:00', () => {
    const parsedTime = Date.parse(`2013-11-18T19:55:00.000Z`);
    const timezone = '+11:00'
    expect(getReversedOffset(parsedTime, timezone)).toEqual(-660);
  });
})
