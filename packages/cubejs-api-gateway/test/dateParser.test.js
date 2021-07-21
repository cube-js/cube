/* globals describe,test,expect,jest */

import { dateParser } from '../src/dateParser';

describe('dateParser', () => {
  test('custom daily ranges returns day aligned dateRange', () => {
    expect(dateParser('from 1 days ago to now', 'UTC')).toStrictEqual(
      [dateParser('yesterday', 'UTC')[0], dateParser('today', 'UTC')[1]]
    );
  });

  test('last 1 day', () => {
    expect(dateParser('last 1 day', 'UTC')).toStrictEqual(
      [dateParser('yesterday', 'UTC')[0], dateParser('yesterday', 'UTC')[1]]
    );
  });

  test('today', () => {
    const start = new Date();
    const end = new Date();
    start.setUTCHours(0, 0, 0, 0);
    end.setUTCHours(23, 59, 59, 999);
    expect(dateParser('today', 'UTC')).toStrictEqual(
      [start.toISOString().replace('Z', ''), end.toISOString().replace('Z', '')]
    );
  });

  test('last 6 hours', () => {
    expect(dateParser('last 6 hours', 'UTC')).toStrictEqual(
      [
        new Date((Math.floor(new Date().getTime() / (1000 * 60 * 60)) - 6) * (1000 * 60 * 60)).toISOString().replace('Z', ''),
        new Date((Math.floor(new Date().getTime() / (1000 * 60 * 60))) * (1000 * 60 * 60) - 1).toISOString().replace('Z', '')
      ]
    );
  });

  test('from 23 hours ago to now', () => {
    expect(dateParser('from 23 hours ago to now', 'UTC')).toStrictEqual(
      [
        new Date((Math.floor(new Date().getTime() / (1000 * 60 * 60)) - 23) * (1000 * 60 * 60)).toISOString().replace('Z', ''),
        new Date((Math.ceil(new Date().getTime() / (1000 * 60 * 60))) * (1000 * 60 * 60) - 1).toISOString().replace('Z', '')
      ]
    );
  });

  test('from 1 hour ago to now LA', () => {
    // 'Z' stands for Zulu time, which is also GMT and UTC.
    const now = '2020-09-22T13:03:20.518Z';
    // LA is GMT-0700, 7 hours diff
    const tz = 'America/Los_Angeles';

    expect(dateParser('from 1 hour ago to now', tz, now)).toStrictEqual(
      [
        '2020-09-22T05:00:00.000',
        '2020-09-22T06:59:59.999'
      ]
    );
  });

  test('from 7 days ago to now', () => {
    expect(dateParser('from 7 days ago to now', 'UTC')).toStrictEqual(
      [dateParser('last 7 days', 'UTC')[0], dateParser('today', 'UTC')[1]]
    );
  });

  test('unexpected date', () => {
    expect(() => dateParser('unexpected date', 'UTC')).toThrowError(
      'Can\'t parse date: \'unexpected date\'',
    );
  });

  test('last 6 months from month with less days than previous month', () => {
    Date.now = jest.fn().mockReturnValue(new Date(2021, 1, 15, 13, 0, 0, 0));

    expect(dateParser('last 6 months', 'UTC', new Date(2021, 1, 15, 13, 0, 0, 0))).toStrictEqual([
      '2020-08-01T00:00:00.000',
      '2021-01-31T23:59:59.999',
    ]);

    Date.now.mockRestore();
  });

  test('last 6 months from month with more days than previous month', () => {
    Date.now = jest.fn().mockReturnValue(new Date(2021, 2, 15, 13, 0, 0, 0));

    expect(dateParser('last 6 months', 'UTC', new Date(2021, 1, 15, 13, 0, 0, 0))).toStrictEqual([
      '2020-09-01T00:00:00.000',
      '2021-02-28T23:59:59.999',
    ]);

    Date.now.mockRestore();
  });
});
