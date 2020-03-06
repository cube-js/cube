/* eslint-disable quote-props */
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

    test('it generates array of dates - granularity hour - not full day', () => {
      const resultSet = new ResultSet({});
      const timeDimension = {
        dateRange: ['2015-01-01T10:30:00.000', '2015-01-01T13:59:00.000'],
        granularity: 'hour',
        timeDimension: 'Events.time'
      };
      const output = [
        '2015-01-01T10:00:00.000',
        '2015-01-01T11:00:00.000',
        '2015-01-01T12:00:00.000',
        '2015-01-01T13:00:00.000'
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
        x: ['Foo.createdAt.day'],
        y: ['Foo.bar'],
        fillMissingDates: true
      });
    });

    test('time dimensions with granularity passed without', () => {
      const resultSet = new ResultSet({
        query: {
          dimensions: ['Foo.bar'],
          timeDimensions: [{
            granularity: 'day',
            dimension: 'Foo.createdAt'
          }]
        }
      });

      expect(resultSet.normalizePivotConfig({ x: ['Foo.createdAt'], y: ['Foo.bar'] })).toEqual({
        x: ['Foo.createdAt.day'],
        y: ['Foo.bar'],
        fillMissingDates: true
      });
    });

    test('double time dimensions without granularity', () => {
      const resultSet = new ResultSet({
        "query": {
          "measures": [],
          "timeDimensions": [{
            "dimension": "Orders.createdAt",
            "dateRange": ["2020-01-08T00:00:00.000", "2020-01-14T23:59:59.999"]
          }],
          "dimensions": ["Orders.createdAt"],
          "filters": [],
          "timezone": "UTC"
        },
      });

      expect(resultSet.normalizePivotConfig(resultSet.normalizePivotConfig({}))).toEqual({
        x: ['Orders.createdAt'],
        y: [],
        fillMissingDates: true
      });
    });

    test('single time dimensions with granularity', () => {
      const resultSet = new ResultSet({
        "query": {
          "measures": [],
          "timeDimensions": [{
            "dimension": "Orders.createdAt",
            "granularity": "day",
            "dateRange": ["2020-01-08T00:00:00.000", "2020-01-09T23:59:59.999"]
          }],
          "filters": [],
          "timezone": "UTC"
        }
      });

      expect(resultSet.normalizePivotConfig(resultSet.normalizePivotConfig())).toEqual({
        x: ['Orders.createdAt.day'],
        y: [],
        fillMissingDates: true
      });
    });

    test('double time dimensions with granularity', () => {
      const resultSet = new ResultSet({
        "query": {
          "measures": [],
          "timeDimensions": [{
            "dimension": "Orders.createdAt",
            "granularity": "day",
            "dateRange": ["2020-01-08T00:00:00.000", "2020-01-14T23:59:59.999"]
          }],
          "dimensions": ["Orders.createdAt"],
          "filters": [],
          "timezone": "UTC"
        },
      });

      expect(resultSet.normalizePivotConfig(resultSet.normalizePivotConfig({}))).toEqual({
        x: ['Orders.createdAt.day', "Orders.createdAt"],
        y: [],
        fillMissingDates: true
      });
    });
  });

  describe('pivot', () => {
    test('same dimension and time dimension', () => {
      const resultSet = new ResultSet({
        "query": {
          "measures": [],
          "timeDimensions": [{
            "dimension": "Orders.createdAt",
            "granularity": "day",
            "dateRange": ["2020-01-08T00:00:00.000", "2020-01-14T23:59:59.999"]
          }],
          "dimensions": ["Orders.createdAt"],
          "filters": [],
          "timezone": "UTC"
        },
        "data": [{
          "Orders.createdAt": "2020-01-08T17:04:43.000",
          "Orders.createdAt.day": "2020-01-08T00:00:00.000"
        }, {
          "Orders.createdAt": "2020-01-08T19:28:26.000",
          "Orders.createdAt.day": "2020-01-08T00:00:00.000"
        }, {
          "Orders.createdAt": "2020-01-09T00:13:01.000",
          "Orders.createdAt.day": "2020-01-09T00:00:00.000"
        }, {
          "Orders.createdAt": "2020-01-09T00:25:32.000",
          "Orders.createdAt.day": "2020-01-09T00:00:00.000"
        }, {
          "Orders.createdAt": "2020-01-09T00:43:11.000",
          "Orders.createdAt.day": "2020-01-09T00:00:00.000"
        }, {
          "Orders.createdAt": "2020-01-09T03:04:00.000",
          "Orders.createdAt.day": "2020-01-09T00:00:00.000"
        }, {
          "Orders.createdAt": "2020-01-09T04:30:10.000",
          "Orders.createdAt.day": "2020-01-09T00:00:00.000"
        }, {
          "Orders.createdAt": "2020-01-09T10:25:04.000",
          "Orders.createdAt.day": "2020-01-09T00:00:00.000"
        }, {
          "Orders.createdAt": "2020-01-09T19:47:19.000",
          "Orders.createdAt.day": "2020-01-09T00:00:00.000"
        }, {
          "Orders.createdAt": "2020-01-09T19:48:04.000",
          "Orders.createdAt.day": "2020-01-09T00:00:00.000"
        }, {
          "Orders.createdAt": "2020-01-09T21:46:24.000",
          "Orders.createdAt.day": "2020-01-09T00:00:00.000"
        }, {
          "Orders.createdAt": "2020-01-09T23:49:37.000",
          "Orders.createdAt.day": "2020-01-09T00:00:00.000"
        }, {
          "Orders.createdAt": "2020-01-10T09:07:20.000",
          "Orders.createdAt.day": "2020-01-10T00:00:00.000"
        }, {
          "Orders.createdAt": "2020-01-10T13:50:05.000",
          "Orders.createdAt.day": "2020-01-10T00:00:00.000"
        }, {
          "Orders.createdAt": "2020-01-10T15:30:32.000",
          "Orders.createdAt.day": "2020-01-10T00:00:00.000"
        }, {
          "Orders.createdAt": "2020-01-10T15:32:52.000",
          "Orders.createdAt.day": "2020-01-10T00:00:00.000"
        }, {
          "Orders.createdAt": "2020-01-10T18:55:23.000",
          "Orders.createdAt.day": "2020-01-10T00:00:00.000"
        }, {
          "Orders.createdAt": "2020-01-11T01:13:17.000",
          "Orders.createdAt.day": "2020-01-11T00:00:00.000"
        }, {
          "Orders.createdAt": "2020-01-11T09:17:40.000",
          "Orders.createdAt.day": "2020-01-11T00:00:00.000"
        }, {
          "Orders.createdAt": "2020-01-11T13:23:03.000",
          "Orders.createdAt.day": "2020-01-11T00:00:00.000"
        }, {
          "Orders.createdAt": "2020-01-11T17:28:42.000",
          "Orders.createdAt.day": "2020-01-11T00:00:00.000"
        }, {
          "Orders.createdAt": "2020-01-11T22:34:32.000",
          "Orders.createdAt.day": "2020-01-11T00:00:00.000"
        }, {
          "Orders.createdAt": "2020-01-11T23:03:58.000",
          "Orders.createdAt.day": "2020-01-11T00:00:00.000"
        }, {
          "Orders.createdAt": "2020-01-12T03:46:25.000",
          "Orders.createdAt.day": "2020-01-12T00:00:00.000"
        }, {
          "Orders.createdAt": "2020-01-12T09:57:10.000",
          "Orders.createdAt.day": "2020-01-12T00:00:00.000"
        }, {
          "Orders.createdAt": "2020-01-12T12:28:22.000",
          "Orders.createdAt.day": "2020-01-12T00:00:00.000"
        }, {
          "Orders.createdAt": "2020-01-12T14:34:20.000",
          "Orders.createdAt.day": "2020-01-12T00:00:00.000"
        }, {
          "Orders.createdAt": "2020-01-12T18:45:15.000",
          "Orders.createdAt.day": "2020-01-12T00:00:00.000"
        }, {
          "Orders.createdAt": "2020-01-12T19:38:05.000",
          "Orders.createdAt.day": "2020-01-12T00:00:00.000"
        }, {
          "Orders.createdAt": "2020-01-12T21:43:51.000",
          "Orders.createdAt.day": "2020-01-12T00:00:00.000"
        }, {
          "Orders.createdAt": "2020-01-13T01:42:49.000",
          "Orders.createdAt.day": "2020-01-13T00:00:00.000"
        }, {
          "Orders.createdAt": "2020-01-13T03:19:22.000",
          "Orders.createdAt.day": "2020-01-13T00:00:00.000"
        }, {
          "Orders.createdAt": "2020-01-13T05:20:50.000",
          "Orders.createdAt.day": "2020-01-13T00:00:00.000"
        }, {
          "Orders.createdAt": "2020-01-13T05:46:35.000",
          "Orders.createdAt.day": "2020-01-13T00:00:00.000"
        }, {
          "Orders.createdAt": "2020-01-13T11:24:01.000",
          "Orders.createdAt.day": "2020-01-13T00:00:00.000"
        }, {
          "Orders.createdAt": "2020-01-13T12:13:42.000",
          "Orders.createdAt.day": "2020-01-13T00:00:00.000"
        }, {
          "Orders.createdAt": "2020-01-13T20:21:59.000",
          "Orders.createdAt.day": "2020-01-13T00:00:00.000"
        }, {
          "Orders.createdAt": "2020-01-14T20:16:23.000",
          "Orders.createdAt.day": "2020-01-14T00:00:00.000"
        }],
        "annotation": {
          "measures": {},
          "dimensions": {
            "Orders.createdAt": {
              "title": "Orders Created at",
              "shortTitle": "Created at",
              "type": "time"
            }
          },
          "segments": {},
          "timeDimensions": {
            "Orders.createdAt.day": {
              "title": "Orders Created at",
              "shortTitle": "Created at",
              "type": "time"
            }
          }
        }
      });

      expect(resultSet.tablePivot()).toEqual([
        {
          "Orders.createdAt.day": "2020-01-08T00:00:00.000",
          "Orders.createdAt": "2020-01-08T17:04:43.000"
        },
        {
          "Orders.createdAt.day": "2020-01-08T00:00:00.000",
          "Orders.createdAt": "2020-01-08T19:28:26.000"
        },
        {
          "Orders.createdAt.day": "2020-01-09T00:00:00.000",
          "Orders.createdAt": "2020-01-09T00:13:01.000"
        },
        {
          "Orders.createdAt.day": "2020-01-09T00:00:00.000",
          "Orders.createdAt": "2020-01-09T00:25:32.000"
        },
        {
          "Orders.createdAt.day": "2020-01-09T00:00:00.000",
          "Orders.createdAt": "2020-01-09T00:43:11.000"
        },
        {
          "Orders.createdAt.day": "2020-01-09T00:00:00.000",
          "Orders.createdAt": "2020-01-09T03:04:00.000"
        },
        {
          "Orders.createdAt.day": "2020-01-09T00:00:00.000",
          "Orders.createdAt": "2020-01-09T04:30:10.000"
        },
        {
          "Orders.createdAt.day": "2020-01-09T00:00:00.000",
          "Orders.createdAt": "2020-01-09T10:25:04.000"
        },
        {
          "Orders.createdAt.day": "2020-01-09T00:00:00.000",
          "Orders.createdAt": "2020-01-09T19:47:19.000"
        },
        {
          "Orders.createdAt.day": "2020-01-09T00:00:00.000",
          "Orders.createdAt": "2020-01-09T19:48:04.000"
        },
        {
          "Orders.createdAt.day": "2020-01-09T00:00:00.000",
          "Orders.createdAt": "2020-01-09T21:46:24.000"
        },
        {
          "Orders.createdAt.day": "2020-01-09T00:00:00.000",
          "Orders.createdAt": "2020-01-09T23:49:37.000"
        },
        {
          "Orders.createdAt.day": "2020-01-10T00:00:00.000",
          "Orders.createdAt": "2020-01-10T09:07:20.000"
        },
        {
          "Orders.createdAt.day": "2020-01-10T00:00:00.000",
          "Orders.createdAt": "2020-01-10T13:50:05.000"
        },
        {
          "Orders.createdAt.day": "2020-01-10T00:00:00.000",
          "Orders.createdAt": "2020-01-10T15:30:32.000"
        },
        {
          "Orders.createdAt.day": "2020-01-10T00:00:00.000",
          "Orders.createdAt": "2020-01-10T15:32:52.000"
        },
        {
          "Orders.createdAt.day": "2020-01-10T00:00:00.000",
          "Orders.createdAt": "2020-01-10T18:55:23.000"
        },
        {
          "Orders.createdAt.day": "2020-01-11T00:00:00.000",
          "Orders.createdAt": "2020-01-11T01:13:17.000"
        },
        {
          "Orders.createdAt.day": "2020-01-11T00:00:00.000",
          "Orders.createdAt": "2020-01-11T09:17:40.000"
        },
        {
          "Orders.createdAt.day": "2020-01-11T00:00:00.000",
          "Orders.createdAt": "2020-01-11T13:23:03.000"
        },
        {
          "Orders.createdAt.day": "2020-01-11T00:00:00.000",
          "Orders.createdAt": "2020-01-11T17:28:42.000"
        },
        {
          "Orders.createdAt.day": "2020-01-11T00:00:00.000",
          "Orders.createdAt": "2020-01-11T22:34:32.000"
        },
        {
          "Orders.createdAt.day": "2020-01-11T00:00:00.000",
          "Orders.createdAt": "2020-01-11T23:03:58.000"
        },
        {
          "Orders.createdAt.day": "2020-01-12T00:00:00.000",
          "Orders.createdAt": "2020-01-12T03:46:25.000"
        },
        {
          "Orders.createdAt.day": "2020-01-12T00:00:00.000",
          "Orders.createdAt": "2020-01-12T09:57:10.000"
        },
        {
          "Orders.createdAt.day": "2020-01-12T00:00:00.000",
          "Orders.createdAt": "2020-01-12T12:28:22.000"
        },
        {
          "Orders.createdAt.day": "2020-01-12T00:00:00.000",
          "Orders.createdAt": "2020-01-12T14:34:20.000"
        },
        {
          "Orders.createdAt.day": "2020-01-12T00:00:00.000",
          "Orders.createdAt": "2020-01-12T18:45:15.000"
        },
        {
          "Orders.createdAt.day": "2020-01-12T00:00:00.000",
          "Orders.createdAt": "2020-01-12T19:38:05.000"
        },
        {
          "Orders.createdAt.day": "2020-01-12T00:00:00.000",
          "Orders.createdAt": "2020-01-12T21:43:51.000"
        },
        {
          "Orders.createdAt.day": "2020-01-13T00:00:00.000",
          "Orders.createdAt": "2020-01-13T01:42:49.000"
        },
        {
          "Orders.createdAt.day": "2020-01-13T00:00:00.000",
          "Orders.createdAt": "2020-01-13T03:19:22.000"
        },
        {
          "Orders.createdAt.day": "2020-01-13T00:00:00.000",
          "Orders.createdAt": "2020-01-13T05:20:50.000"
        },
        {
          "Orders.createdAt.day": "2020-01-13T00:00:00.000",
          "Orders.createdAt": "2020-01-13T05:46:35.000"
        },
        {
          "Orders.createdAt.day": "2020-01-13T00:00:00.000",
          "Orders.createdAt": "2020-01-13T11:24:01.000"
        },
        {
          "Orders.createdAt.day": "2020-01-13T00:00:00.000",
          "Orders.createdAt": "2020-01-13T12:13:42.000"
        },
        {
          "Orders.createdAt.day": "2020-01-13T00:00:00.000",
          "Orders.createdAt": "2020-01-13T20:21:59.000"
        },
        {
          "Orders.createdAt.day": "2020-01-14T00:00:00.000",
          "Orders.createdAt": "2020-01-14T20:16:23.000"
        }]);
    });

    test('time dimension backward compatibility', () => {
      const resultSet = new ResultSet({
        "query": {
          "measures": [],
          "timeDimensions": [{
            "dimension": "Orders.createdAt",
            "granularity": "day",
            "dateRange": ["2020-01-08T00:00:00.000", "2020-01-09T23:59:59.999"]
          }],
          "filters": [],
          "timezone": "UTC"
        },
        "data": [{
          "Orders.createdAt": "2020-01-08T00:00:00.000"
        }, {
          "Orders.createdAt": "2020-01-09T00:00:00.000"
        }],
        "annotation": {
          "measures": {},
          "dimensions": {},
          "segments": {},
          "timeDimensions": {
            "Orders.createdAt": {
              "title": "Orders Created at",
              "shortTitle": "Created at",
              "type": "time"
            }
          }
        }
      });

      expect(resultSet.tablePivot()).toEqual([
        {
          "Orders.createdAt.day": "2020-01-08T00:00:00.000",
        },
        {
          "Orders.createdAt.day": "2020-01-09T00:00:00.000",
        }
      ]);
    });

    test('same dimension and time dimension without granularity', () => {
      const resultSet = new ResultSet({
        "query": {
          "measures": [],
          "timeDimensions": [{
            "dimension": "Orders.createdAt",
            "dateRange": ["2020-01-08T00:00:00.000", "2020-01-14T23:59:59.999"]
          }],
          "dimensions": ["Orders.createdAt"],
          "filters": [],
          "timezone": "UTC"
        },
        "data": [
          { "Orders.createdAt": "2020-01-08T17:04:43.000" },
          { "Orders.createdAt": "2020-01-08T19:28:26.000" },
          { "Orders.createdAt": "2020-01-09T00:13:01.000" },
          { "Orders.createdAt": "2020-01-09T00:25:32.000" },
          { "Orders.createdAt": "2020-01-09T00:43:11.000" },
          { "Orders.createdAt": "2020-01-09T03:04:00.000" },
          { "Orders.createdAt": "2020-01-09T04:30:10.000" },
          { "Orders.createdAt": "2020-01-09T10:25:04.000" },
          { "Orders.createdAt": "2020-01-09T19:47:19.000" },
          { "Orders.createdAt": "2020-01-09T19:48:04.000" },
          { "Orders.createdAt": "2020-01-09T21:46:24.000" },
          { "Orders.createdAt": "2020-01-09T23:49:37.000" },
          { "Orders.createdAt": "2020-01-10T09:07:20.000" },
          { "Orders.createdAt": "2020-01-10T13:50:05.000" }
        ],
        "annotation": {
          "measures": {},
          "dimensions": {
            "Orders.createdAt": {
              "title": "Orders Created at",
              "shortTitle": "Created at",
              "type": "time"
            }
          },
          "segments": {},
          "timeDimensions": {}
        }
      });

      expect(resultSet.tablePivot()).toEqual([
        { "Orders.createdAt": "2020-01-08T17:04:43.000" },
        { "Orders.createdAt": "2020-01-08T19:28:26.000" },
        { "Orders.createdAt": "2020-01-09T00:13:01.000" },
        { "Orders.createdAt": "2020-01-09T00:25:32.000" },
        { "Orders.createdAt": "2020-01-09T00:43:11.000" },
        { "Orders.createdAt": "2020-01-09T03:04:00.000" },
        { "Orders.createdAt": "2020-01-09T04:30:10.000" },
        { "Orders.createdAt": "2020-01-09T10:25:04.000" },
        { "Orders.createdAt": "2020-01-09T19:47:19.000" },
        { "Orders.createdAt": "2020-01-09T19:48:04.000" },
        { "Orders.createdAt": "2020-01-09T21:46:24.000" },
        { "Orders.createdAt": "2020-01-09T23:49:37.000" },
        { "Orders.createdAt": "2020-01-10T09:07:20.000" },
        { "Orders.createdAt": "2020-01-10T13:50:05.000" }
      ]);
    });
  });
});
