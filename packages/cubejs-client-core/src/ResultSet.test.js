/* globals jest,describe,test,expect */
import ResultSet from './ResultSet';

jest.mock('moment-range', () => {
  const Moment = jest.requireActual('moment');
  const MomentRange = jest.requireActual('moment-range');
  const moment = MomentRange.extendMoment(Moment);
  return {
    extendMoment: () => moment
  };
});

describe('ResultSet', () => {
  describe('timeSeries', () => {
    test('it generates array of dates - granularity month', () => {
      const resultSet = new ResultSet({});
      const timeDimension = {
        dateRange: ['2015-01-01', '2015-12-31'],
        granularity: 'month',
        timeDimension: 'Events.time'
      };
      const output = [
        '2015-01-01T00:00:00.000',
        '2015-02-01T00:00:00.000',
        '2015-03-01T00:00:00.000',
        '2015-04-01T00:00:00.000',
        '2015-05-01T00:00:00.000',
        '2015-06-01T00:00:00.000',
        '2015-07-01T00:00:00.000',
        '2015-08-01T00:00:00.000',
        '2015-09-01T00:00:00.000',
        '2015-10-01T00:00:00.000',
        '2015-11-01T00:00:00.000',
        '2015-12-01T00:00:00.000'
      ];
      expect(resultSet.timeSeries(timeDimension)).toEqual(output);
    });

    test('it generates array of dates - granularity hour', () => {
      const resultSet = new ResultSet({});
      const timeDimension = {
        dateRange: ['2015-01-01', '2015-01-01'],
        granularity: 'hour',
        timeDimension: 'Events.time'
      };
      const output = [
        '2015-01-01T00:00:00.000',
        '2015-01-01T01:00:00.000',
        '2015-01-01T02:00:00.000',
        '2015-01-01T03:00:00.000',
        '2015-01-01T04:00:00.000',
        '2015-01-01T05:00:00.000',
        '2015-01-01T06:00:00.000',
        '2015-01-01T07:00:00.000',
        '2015-01-01T08:00:00.000',
        '2015-01-01T09:00:00.000',
        '2015-01-01T10:00:00.000',
        '2015-01-01T11:00:00.000',
        '2015-01-01T12:00:00.000',
        '2015-01-01T13:00:00.000',
        '2015-01-01T14:00:00.000',
        '2015-01-01T15:00:00.000',
        '2015-01-01T16:00:00.000',
        '2015-01-01T17:00:00.000',
        '2015-01-01T18:00:00.000',
        '2015-01-01T19:00:00.000',
        '2015-01-01T20:00:00.000',
        '2015-01-01T21:00:00.000',
        '2015-01-01T22:00:00.000',
        '2015-01-01T23:00:00.000'
      ];
      expect(resultSet.timeSeries(timeDimension)).toEqual(output);
    });
  });

  describe('normalizePivotConfig', () => {
    test('fills missing x, y', () => {
      const resultSet = new ResultSet({
        query: {
          dimensions: ['Foo.bar'],
          timeDimensions: [{
            granularity: 'day',
            dimension: 'Foo.createdAt'
          }]
        }
      });

      expect(resultSet.normalizePivotConfig({ y: ['Foo.bar'] })).toEqual({
        x: ['Foo.createdAt'],
        y: ['Foo.bar'],
        fillMissingDates: true
      });
    });
  });
});
