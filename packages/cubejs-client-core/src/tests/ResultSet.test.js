/**
 * @license Apache-2.0
 * @copyright Cube Dev, Inc.
 * @fileoverview ResultSet class unit tests.
 */

/* globals describe,test,expect */

import 'jest';
import ResultSet from '../ResultSet';

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

    test('it generates array of dates - granularity quarter', () => {
      const resultSet = new ResultSet({});
      const timeDimension = {
        dateRange: ['2015-01-01', '2015-12-31'],
        granularity: 'quarter',
        timeDimension: 'Events.time'
      };
      const output = [
        '2015-01-01T00:00:00.000',
        '2015-04-01T00:00:00.000',
        '2015-07-01T00:00:00.000',
        '2015-10-01T00:00:00.000',
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

  describe('chartPivot', () => {
    test('String field', () => {
      const resultSet = new ResultSet({
        query: {
          measures: ['Foo.count'],
          dimensions: ['Foo.name'],
          filters: [],
          timezone: 'UTC',
          timeDimensions: []
        },
        data: [
          {
            'Foo.name': 'Name 1',
            'Foo.count': 'Some string'
          }
        ],
        lastRefreshTime: '2020-03-18T13:41:04.436Z',
        usedPreAggregations: {},
        annotation: {
          measures: {
            'Foo.count': {
              title: 'Foo Count',
              shortTitle: 'Count',
              type: 'number'
            }
          },
          dimensions: {
            'Foo.name': {
              title: 'Foo Name',
              shortTitle: 'Name',
              type: 'string'
            }
          },
          segments: {},
          timeDimensions: {}
        }
      });

      expect(resultSet.chartPivot()).toEqual([
        {
          x: 'Name 1',
          
          'Foo.count': 'Some string',
          xValues: [
            'Name 1'
          ],
        }
      ]);
    });

    test('Null field', () => {
      const resultSet = new ResultSet({
        query: {
          measures: ['Foo.count'],
          dimensions: ['Foo.name'],
          filters: [],
          timezone: 'UTC',
          timeDimensions: []
        },
        data: [
          {
            'Foo.name': 'Name 1',
            'Foo.count': null
          }
        ],
        lastRefreshTime: '2020-03-18T13:41:04.436Z',
        usedPreAggregations: {},
        annotation: {
          measures: {
            'Foo.count': {
              title: 'Foo Count',
              shortTitle: 'Count',
              type: 'number'
            }
          },
          dimensions: {
            'Foo.name': {
              title: 'Foo Name',
              shortTitle: 'Name',
              type: 'string'
            }
          },
          segments: {},
          timeDimensions: {}
        }
      });

      expect(resultSet.chartPivot()).toEqual([
        {
          x: 'Name 1',
          
          'Foo.count': 0,
          xValues: [
            'Name 1'
          ],
        }
      ]);
    });

    test('Empty field', () => {
      const resultSet = new ResultSet({
        query: {
          measures: ['Foo.count'],
          dimensions: ['Foo.name'],
          filters: [],
          timezone: 'UTC',
          timeDimensions: []
        },
        data: [
          {
            'Foo.name': 'Name 1',
            'Foo.count': undefined
          }
        ],
        lastRefreshTime: '2020-03-18T13:41:04.436Z',
        usedPreAggregations: {},
        annotation: {
          measures: {
            'Foo.count': {
              title: 'Foo Count',
              shortTitle: 'Count',
              type: 'number'
            }
          },
          dimensions: {
            'Foo.name': {
              title: 'Foo Name',
              shortTitle: 'Name',
              type: 'string'
            }
          },
          segments: {},
          timeDimensions: {}
        }
      });

      expect(resultSet.chartPivot()).toEqual([
        {
          x: 'Name 1',
          'Foo.count': 0,
          xValues: [
            'Name 1'
          ],
        }
      ]);
    });

    test('Number field', () => {
      const resultSet = new ResultSet({
        query: {
          measures: ['Foo.count'],
          dimensions: ['Foo.name'],
          filters: [],
          timezone: 'UTC',
          timeDimensions: []
        },
        data: [
          {
            'Foo.name': 'Name 1',
            'Foo.count': '10'
          }
        ],
        lastRefreshTime: '2020-03-18T13:41:04.436Z',
        usedPreAggregations: {},
        annotation: {
          measures: {
            'Foo.count': {
              title: 'Foo Count',
              shortTitle: 'Count',
              type: 'number'
            }
          },
          dimensions: {
            'Foo.name': {
              title: 'Foo Name',
              shortTitle: 'Name',
              type: 'string'
            }
          },
          segments: {},
          timeDimensions: {}
        }
      });

      expect(resultSet.chartPivot()).toEqual([
        {
          x: 'Name 1',
          
          'Foo.count': 10,
          xValues: [
            'Name 1'
          ],
        }
      ]);
    });

    test('time field results', () => {
      const resultSet = new ResultSet(
        {
          query: {
            measures: ['Foo.latestRun'],
            dimensions: ['Foo.name'],
            filters: [],
            timezone: 'UTC',
            timeDimensions: []
          },
          data: [
            {
              'Foo.name': 'Name 1',
              'Foo.latestRun': '2020-03-11T18:06:09.403Z'
            }
          ],
          lastRefreshTime: '2020-03-18T13:41:04.436Z',
          usedPreAggregations: {},
          annotation: {
            measures: {
              'Foo.latestRun': {
                title: 'Foo Latest Run',
                shortTitle: 'Latest Run',
                type: 'number'
              }
            },
            dimensions: {
              'Foo.name': {
                title: 'Foo Name',
                shortTitle: 'Name',
                type: 'string'
              }
            },
            segments: {},
            timeDimensions: {}
          }
        },
        { parseDateMeasures: true }
      );

      expect(resultSet.chartPivot()).toEqual([
        {
          x: 'Name 1',
          
          'Foo.latestRun': new Date('2020-03-11T18:06:09.403Z'),
          xValues: [
            'Name 1'
          ],
        }
      ]);
    });
  });

  describe('seriesNames', () => {
    test('Multiple series with custom alias', () => {
      const resultSet = new ResultSet({
        queryType: 'blendingQuery',
        results: [
          {
            query: {
              measures: ['Users.count'],
              timeDimensions: [
                {
                  dimension: 'Users.ts',
                  granularity: 'month',
                  dateRange: ['2020-07-01T00:00:00.000', '2020-11-01T00:00:00.000'],
                },
              ],
              filters: [],
              order: [],
              dimensions: [],
            },
            data: [
              {
                'Users.ts.month': '2020-08-01T00:00:00.000',
                'Users.ts': '2020-08-01T00:00:00.000',
                'Users.count': 14,
                'time.month': '2020-08-01T00:00:00.000',
              },
            ],
            annotation: {
              measures: {
                'Users.count': {
                  title: 'Users Count',
                  shortTitle: 'Count',
                  type: 'number',
                  drillMembers: ['Users.id', 'Users.name'],
                  drillMembersGrouped: {
                    measures: [],
                    dimensions: ['Users.id', 'Users.name'],
                  },
                },
              },
              dimensions: {},
              segments: {},
              timeDimensions: {
                'Users.ts.month': { title: 'Users Ts', shortTitle: 'Ts', type: 'time' },
                'Users.ts': { title: 'Users Ts', shortTitle: 'Ts', type: 'time' },
              },
            },
          },
          {
            query: {
              measures: ['Users.count'],
              timeDimensions: [
                {
                  dimension: 'Users.ts',
                  granularity: 'month',
                  dateRange: ['2020-07-01T00:00:00.000', '2020-11-01T00:00:00.000'],
                },
              ],
              filters: [
                {
                  member: 'Users.country',
                  operator: 'equals',
                  value: ['USA'],
                },
              ],
              order: [],
            },
            data: [
              {
                'Users.ts.month': '2020-08-01T00:00:00.000',
                'Users.ts': '2020-08-01T00:00:00.000',
                'Users.count': 2,
                'time.month': '2020-08-01T00:00:00.000',
              },
            ],
            annotation: {
              measures: {
                'Users.count': {
                  title: 'Users Count',
                  shortTitle: 'Count',
                  type: 'number',
                  drillMembers: [],
                  drillMembersGrouped: {
                    measures: [],
                    dimensions: [],
                  },
                },
              },
              dimensions: {},
              segments: {},
              timeDimensions: {
                'Users.ts.month': { title: 'Users Ts', shortTitle: 'Ts', type: 'time' },
                'Users.ts': { title: 'Users Ts', shortTitle: 'Ts', type: 'time' },
              },
            },
          },
        ],
        pivotQuery: {
          measures: ['Users.count', 'Users.count'],
          timeDimensions: [
            {
              dimension: 'time',
              granularity: 'month',
              dateRange: ['2020-07-01T00:00:00.000', '2020-11-01T00:00:00.000'],
            },
          ],
          dimensions: [],
        },
      });

      expect(resultSet.seriesNames({ aliasSeries: ['one', 'two'] })).toEqual([
        {
          key: 'one,Users.count',
          title: 'one, Users Count',
          shortTitle: 'one, Count',
          yValues: ['Users.count'],
        },
        {
          key: 'two,Users.count',
          title: 'two, Users Count',
          shortTitle: 'two, Count',
          yValues: ['Users.count'],
        },
      ]);
    });
    test('Multiple series with same measure', () => {
      const resultSet = new ResultSet({
        queryType: 'blendingQuery',
        results: [
          {
            query: {
              measures: ['Users.count'],
              timeDimensions: [
                {
                  dimension: 'Users.ts',
                  granularity: 'month',
                  dateRange: ['2020-07-01T00:00:00.000', '2020-11-01T00:00:00.000'],
                },
              ],
              filters: [],
              order: [],
              dimensions: [],
            },
            data: [
              {
                'Users.ts.month': '2020-08-01T00:00:00.000',
                'Users.ts': '2020-08-01T00:00:00.000',
                'Users.count': 14,
                'time.month': '2020-08-01T00:00:00.000',
              },
            ],
            annotation: {
              measures: {
                'Users.count': {
                  title: 'Users Count',
                  shortTitle: 'Count',
                  type: 'number',
                  drillMembers: ['Users.id', 'Users.name'],
                  drillMembersGrouped: {
                    measures: [],
                    dimensions: ['Users.id', 'Users.name'],
                  },
                },
              },
              dimensions: {},
              segments: {},
              timeDimensions: {
                'Users.ts.month': { title: 'Users Ts', shortTitle: 'Ts', type: 'time' },
                'Users.ts': { title: 'Users Ts', shortTitle: 'Ts', type: 'time' },
              },
            },
          },
          {
            query: {
              measures: ['Users.count'],
              timeDimensions: [
                {
                  dimension: 'Users.ts',
                  granularity: 'month',
                  dateRange: ['2020-07-01T00:00:00.000', '2020-11-01T00:00:00.000'],
                },
              ],
              filters: [
                {
                  member: 'Users.country',
                  operator: 'equals',
                  value: ['USA'],
                },
              ],
              order: [],
            },
            data: [
              {
                'Users.ts.month': '2020-08-01T00:00:00.000',
                'Users.ts': '2020-08-01T00:00:00.000',
                'Users.count': 2,
                'time.month': '2020-08-01T00:00:00.000',
              },
            ],
            annotation: {
              measures: {
                'Users.count': {
                  title: 'Users Count',
                  shortTitle: 'Count',
                  type: 'number',
                  drillMembers: [],
                  drillMembersGrouped: {
                    measures: [],
                    dimensions: [],
                  },
                },
              },
              dimensions: {},
              segments: {},
              timeDimensions: {
                'Users.ts.month': { title: 'Users Ts', shortTitle: 'Ts', type: 'time' },
                'Users.ts': { title: 'Users Ts', shortTitle: 'Ts', type: 'time' },
              },
            },
          },
        ],
        pivotQuery: {
          measures: ['Users.count', 'Users.count'],
          timeDimensions: [
            {
              dimension: 'time',
              granularity: 'month',
              dateRange: ['2020-07-01T00:00:00.000', '2020-11-01T00:00:00.000'],
            },
          ],
          dimensions: [],
        },
      });

      expect(resultSet.seriesNames()).toEqual([
        {
          key: '0,Users.count',
          title: '0, Users Count',
          shortTitle: '0, Count',
          yValues: ['Users.count'],
        },
        {
          key: '1,Users.count',
          title: '1, Users Count',
          shortTitle: '1, Count',
          yValues: ['Users.count'],
        },
      ]);
    });
  });

  describe('normalizePivotConfig', () => {
    test('fills missing x, y', () => {
      const resultSet = new ResultSet({
        query: {
          dimensions: ['Foo.bar'],
          timeDimensions: [
            {
              granularity: 'day',
              dimension: 'Foo.createdAt'
            }
          ]
        }
      });

      expect(resultSet.normalizePivotConfig({ y: ['Foo.bar'] })).toEqual({
        x: ['Foo.createdAt.day'],
        y: ['Foo.bar'],
        fillMissingDates: true,
        joinDateRange: false
      });
    });

    test('time dimensions with granularity passed without', () => {
      const resultSet = new ResultSet({
        query: {
          dimensions: ['Foo.bar'],
          timeDimensions: [
            {
              granularity: 'day',
              dimension: 'Foo.createdAt'
            }
          ]
        }
      });

      expect(
        resultSet.normalizePivotConfig({ x: ['Foo.createdAt'], y: ['Foo.bar'] })
      ).toEqual({
        x: ['Foo.createdAt.day'],
        y: ['Foo.bar'],
        fillMissingDates: true,
        joinDateRange: false
      });
    });

    test('double time dimensions without granularity', () => {
      const resultSet = new ResultSet({
        query: {
          measures: [],
          timeDimensions: [
            {
              dimension: 'Orders.createdAt',
              dateRange: ['2020-01-08T00:00:00.000', '2020-01-14T23:59:59.999']
            }
          ],
          dimensions: ['Orders.createdAt'],
          filters: [],
          timezone: 'UTC'
        }
      });

      expect(
        resultSet.normalizePivotConfig(resultSet.normalizePivotConfig({}))
      ).toEqual({
        x: ['Orders.createdAt'],
        y: [],
        fillMissingDates: true,
        joinDateRange: false
      });
    });

    test('single time dimensions with granularity', () => {
      const resultSet = new ResultSet({
        query: {
          measures: [],
          timeDimensions: [
            {
              dimension: 'Orders.createdAt',
              granularity: 'day',
              dateRange: ['2020-01-08T00:00:00.000', '2020-01-09T23:59:59.999']
            }
          ],
          filters: [],
          timezone: 'UTC'
        }
      });

      expect(
        resultSet.normalizePivotConfig(resultSet.normalizePivotConfig())
      ).toEqual({
        x: ['Orders.createdAt.day'],
        y: [],
        fillMissingDates: true,
        joinDateRange: false
      });
    });

    test('double time dimensions with granularity', () => {
      const resultSet = new ResultSet({
        query: {
          measures: [],
          timeDimensions: [
            {
              dimension: 'Orders.createdAt',
              granularity: 'day',
              dateRange: ['2020-01-08T00:00:00.000', '2020-01-14T23:59:59.999']
            }
          ],
          dimensions: ['Orders.createdAt'],
          filters: [],
          timezone: 'UTC'
        }
      });

      expect(
        resultSet.normalizePivotConfig(resultSet.normalizePivotConfig({}))
      ).toEqual({
        x: ['Orders.createdAt.day', 'Orders.createdAt'],
        y: [],
        fillMissingDates: true,
        joinDateRange: false
      });
    });
  });

  describe('pivot', () => {
    test('same dimension and time dimension', () => {
      const resultSet = new ResultSet({
        query: {
          measures: [],
          timeDimensions: [
            {
              dimension: 'Orders.createdAt',
              granularity: 'day',
              dateRange: ['2020-01-08T00:00:00.000', '2020-01-14T23:59:59.999']
            }
          ],
          dimensions: ['Orders.createdAt'],
          filters: [],
          timezone: 'UTC'
        },
        data: [
          {
            'Orders.createdAt': '2020-01-08T17:04:43.000',
            'Orders.createdAt.day': '2020-01-08T00:00:00.000'
          },
          {
            'Orders.createdAt': '2020-01-08T19:28:26.000',
            'Orders.createdAt.day': '2020-01-08T00:00:00.000'
          },
          {
            'Orders.createdAt': '2020-01-09T00:13:01.000',
            'Orders.createdAt.day': '2020-01-09T00:00:00.000'
          },
          {
            'Orders.createdAt': '2020-01-09T00:25:32.000',
            'Orders.createdAt.day': '2020-01-09T00:00:00.000'
          },
          {
            'Orders.createdAt': '2020-01-09T00:43:11.000',
            'Orders.createdAt.day': '2020-01-09T00:00:00.000'
          },
          {
            'Orders.createdAt': '2020-01-09T03:04:00.000',
            'Orders.createdAt.day': '2020-01-09T00:00:00.000'
          },
          {
            'Orders.createdAt': '2020-01-09T04:30:10.000',
            'Orders.createdAt.day': '2020-01-09T00:00:00.000'
          },
          {
            'Orders.createdAt': '2020-01-09T10:25:04.000',
            'Orders.createdAt.day': '2020-01-09T00:00:00.000'
          },
          {
            'Orders.createdAt': '2020-01-09T19:47:19.000',
            'Orders.createdAt.day': '2020-01-09T00:00:00.000'
          },
          {
            'Orders.createdAt': '2020-01-09T19:48:04.000',
            'Orders.createdAt.day': '2020-01-09T00:00:00.000'
          },
          {
            'Orders.createdAt': '2020-01-09T21:46:24.000',
            'Orders.createdAt.day': '2020-01-09T00:00:00.000'
          },
          {
            'Orders.createdAt': '2020-01-09T23:49:37.000',
            'Orders.createdAt.day': '2020-01-09T00:00:00.000'
          },
          {
            'Orders.createdAt': '2020-01-10T09:07:20.000',
            'Orders.createdAt.day': '2020-01-10T00:00:00.000'
          },
          {
            'Orders.createdAt': '2020-01-10T13:50:05.000',
            'Orders.createdAt.day': '2020-01-10T00:00:00.000'
          },
          {
            'Orders.createdAt': '2020-01-10T15:30:32.000',
            'Orders.createdAt.day': '2020-01-10T00:00:00.000'
          },
          {
            'Orders.createdAt': '2020-01-10T15:32:52.000',
            'Orders.createdAt.day': '2020-01-10T00:00:00.000'
          },
          {
            'Orders.createdAt': '2020-01-10T18:55:23.000',
            'Orders.createdAt.day': '2020-01-10T00:00:00.000'
          },
          {
            'Orders.createdAt': '2020-01-11T01:13:17.000',
            'Orders.createdAt.day': '2020-01-11T00:00:00.000'
          },
          {
            'Orders.createdAt': '2020-01-11T09:17:40.000',
            'Orders.createdAt.day': '2020-01-11T00:00:00.000'
          },
          {
            'Orders.createdAt': '2020-01-11T13:23:03.000',
            'Orders.createdAt.day': '2020-01-11T00:00:00.000'
          },
          {
            'Orders.createdAt': '2020-01-11T17:28:42.000',
            'Orders.createdAt.day': '2020-01-11T00:00:00.000'
          },
          {
            'Orders.createdAt': '2020-01-11T22:34:32.000',
            'Orders.createdAt.day': '2020-01-11T00:00:00.000'
          },
          {
            'Orders.createdAt': '2020-01-11T23:03:58.000',
            'Orders.createdAt.day': '2020-01-11T00:00:00.000'
          },
          {
            'Orders.createdAt': '2020-01-12T03:46:25.000',
            'Orders.createdAt.day': '2020-01-12T00:00:00.000'
          },
          {
            'Orders.createdAt': '2020-01-12T09:57:10.000',
            'Orders.createdAt.day': '2020-01-12T00:00:00.000'
          },
          {
            'Orders.createdAt': '2020-01-12T12:28:22.000',
            'Orders.createdAt.day': '2020-01-12T00:00:00.000'
          },
          {
            'Orders.createdAt': '2020-01-12T14:34:20.000',
            'Orders.createdAt.day': '2020-01-12T00:00:00.000'
          },
          {
            'Orders.createdAt': '2020-01-12T18:45:15.000',
            'Orders.createdAt.day': '2020-01-12T00:00:00.000'
          },
          {
            'Orders.createdAt': '2020-01-12T19:38:05.000',
            'Orders.createdAt.day': '2020-01-12T00:00:00.000'
          },
          {
            'Orders.createdAt': '2020-01-12T21:43:51.000',
            'Orders.createdAt.day': '2020-01-12T00:00:00.000'
          },
          {
            'Orders.createdAt': '2020-01-13T01:42:49.000',
            'Orders.createdAt.day': '2020-01-13T00:00:00.000'
          },
          {
            'Orders.createdAt': '2020-01-13T03:19:22.000',
            'Orders.createdAt.day': '2020-01-13T00:00:00.000'
          },
          {
            'Orders.createdAt': '2020-01-13T05:20:50.000',
            'Orders.createdAt.day': '2020-01-13T00:00:00.000'
          },
          {
            'Orders.createdAt': '2020-01-13T05:46:35.000',
            'Orders.createdAt.day': '2020-01-13T00:00:00.000'
          },
          {
            'Orders.createdAt': '2020-01-13T11:24:01.000',
            'Orders.createdAt.day': '2020-01-13T00:00:00.000'
          },
          {
            'Orders.createdAt': '2020-01-13T12:13:42.000',
            'Orders.createdAt.day': '2020-01-13T00:00:00.000'
          },
          {
            'Orders.createdAt': '2020-01-13T20:21:59.000',
            'Orders.createdAt.day': '2020-01-13T00:00:00.000'
          },
          {
            'Orders.createdAt': '2020-01-14T20:16:23.000',
            'Orders.createdAt.day': '2020-01-14T00:00:00.000'
          }
        ],
        annotation: {
          measures: {},
          dimensions: {
            'Orders.createdAt': {
              title: 'Orders Created at',
              shortTitle: 'Created at',
              type: 'time'
            }
          },
          segments: {},
          timeDimensions: {
            'Orders.createdAt.day': {
              title: 'Orders Created at',
              shortTitle: 'Created at',
              type: 'time'
            }
          }
        }
      });

      expect(resultSet.tablePivot()).toEqual([
        {
          'Orders.createdAt.day': '2020-01-08T00:00:00.000',
          'Orders.createdAt': '2020-01-08T17:04:43.000'
        },
        {
          'Orders.createdAt.day': '2020-01-08T00:00:00.000',
          'Orders.createdAt': '2020-01-08T19:28:26.000'
        },
        {
          'Orders.createdAt.day': '2020-01-09T00:00:00.000',
          'Orders.createdAt': '2020-01-09T00:13:01.000'
        },
        {
          'Orders.createdAt.day': '2020-01-09T00:00:00.000',
          'Orders.createdAt': '2020-01-09T00:25:32.000'
        },
        {
          'Orders.createdAt.day': '2020-01-09T00:00:00.000',
          'Orders.createdAt': '2020-01-09T00:43:11.000'
        },
        {
          'Orders.createdAt.day': '2020-01-09T00:00:00.000',
          'Orders.createdAt': '2020-01-09T03:04:00.000'
        },
        {
          'Orders.createdAt.day': '2020-01-09T00:00:00.000',
          'Orders.createdAt': '2020-01-09T04:30:10.000'
        },
        {
          'Orders.createdAt.day': '2020-01-09T00:00:00.000',
          'Orders.createdAt': '2020-01-09T10:25:04.000'
        },
        {
          'Orders.createdAt.day': '2020-01-09T00:00:00.000',
          'Orders.createdAt': '2020-01-09T19:47:19.000'
        },
        {
          'Orders.createdAt.day': '2020-01-09T00:00:00.000',
          'Orders.createdAt': '2020-01-09T19:48:04.000'
        },
        {
          'Orders.createdAt.day': '2020-01-09T00:00:00.000',
          'Orders.createdAt': '2020-01-09T21:46:24.000'
        },
        {
          'Orders.createdAt.day': '2020-01-09T00:00:00.000',
          'Orders.createdAt': '2020-01-09T23:49:37.000'
        },
        {
          'Orders.createdAt.day': '2020-01-10T00:00:00.000',
          'Orders.createdAt': '2020-01-10T09:07:20.000'
        },
        {
          'Orders.createdAt.day': '2020-01-10T00:00:00.000',
          'Orders.createdAt': '2020-01-10T13:50:05.000'
        },
        {
          'Orders.createdAt.day': '2020-01-10T00:00:00.000',
          'Orders.createdAt': '2020-01-10T15:30:32.000'
        },
        {
          'Orders.createdAt.day': '2020-01-10T00:00:00.000',
          'Orders.createdAt': '2020-01-10T15:32:52.000'
        },
        {
          'Orders.createdAt.day': '2020-01-10T00:00:00.000',
          'Orders.createdAt': '2020-01-10T18:55:23.000'
        },
        {
          'Orders.createdAt.day': '2020-01-11T00:00:00.000',
          'Orders.createdAt': '2020-01-11T01:13:17.000'
        },
        {
          'Orders.createdAt.day': '2020-01-11T00:00:00.000',
          'Orders.createdAt': '2020-01-11T09:17:40.000'
        },
        {
          'Orders.createdAt.day': '2020-01-11T00:00:00.000',
          'Orders.createdAt': '2020-01-11T13:23:03.000'
        },
        {
          'Orders.createdAt.day': '2020-01-11T00:00:00.000',
          'Orders.createdAt': '2020-01-11T17:28:42.000'
        },
        {
          'Orders.createdAt.day': '2020-01-11T00:00:00.000',
          'Orders.createdAt': '2020-01-11T22:34:32.000'
        },
        {
          'Orders.createdAt.day': '2020-01-11T00:00:00.000',
          'Orders.createdAt': '2020-01-11T23:03:58.000'
        },
        {
          'Orders.createdAt.day': '2020-01-12T00:00:00.000',
          'Orders.createdAt': '2020-01-12T03:46:25.000'
        },
        {
          'Orders.createdAt.day': '2020-01-12T00:00:00.000',
          'Orders.createdAt': '2020-01-12T09:57:10.000'
        },
        {
          'Orders.createdAt.day': '2020-01-12T00:00:00.000',
          'Orders.createdAt': '2020-01-12T12:28:22.000'
        },
        {
          'Orders.createdAt.day': '2020-01-12T00:00:00.000',
          'Orders.createdAt': '2020-01-12T14:34:20.000'
        },
        {
          'Orders.createdAt.day': '2020-01-12T00:00:00.000',
          'Orders.createdAt': '2020-01-12T18:45:15.000'
        },
        {
          'Orders.createdAt.day': '2020-01-12T00:00:00.000',
          'Orders.createdAt': '2020-01-12T19:38:05.000'
        },
        {
          'Orders.createdAt.day': '2020-01-12T00:00:00.000',
          'Orders.createdAt': '2020-01-12T21:43:51.000'
        },
        {
          'Orders.createdAt.day': '2020-01-13T00:00:00.000',
          'Orders.createdAt': '2020-01-13T01:42:49.000'
        },
        {
          'Orders.createdAt.day': '2020-01-13T00:00:00.000',
          'Orders.createdAt': '2020-01-13T03:19:22.000'
        },
        {
          'Orders.createdAt.day': '2020-01-13T00:00:00.000',
          'Orders.createdAt': '2020-01-13T05:20:50.000'
        },
        {
          'Orders.createdAt.day': '2020-01-13T00:00:00.000',
          'Orders.createdAt': '2020-01-13T05:46:35.000'
        },
        {
          'Orders.createdAt.day': '2020-01-13T00:00:00.000',
          'Orders.createdAt': '2020-01-13T11:24:01.000'
        },
        {
          'Orders.createdAt.day': '2020-01-13T00:00:00.000',
          'Orders.createdAt': '2020-01-13T12:13:42.000'
        },
        {
          'Orders.createdAt.day': '2020-01-13T00:00:00.000',
          'Orders.createdAt': '2020-01-13T20:21:59.000'
        },
        {
          'Orders.createdAt.day': '2020-01-14T00:00:00.000',
          'Orders.createdAt': '2020-01-14T20:16:23.000'
        }
      ]);
    });

    test('time dimension backward compatibility', () => {
      const resultSet = new ResultSet({
        query: {
          measures: [],
          timeDimensions: [
            {
              dimension: 'Orders.createdAt',
              granularity: 'day',
              dateRange: ['2020-01-08T00:00:00.000', '2020-01-09T23:59:59.999']
            }
          ],
          filters: [],
          timezone: 'UTC'
        },
        data: [
          {
            'Orders.createdAt': '2020-01-08T00:00:00.000'
          },
          {
            'Orders.createdAt': '2020-01-09T00:00:00.000'
          }
        ],
        annotation: {
          measures: {},
          dimensions: {},
          segments: {},
          timeDimensions: {
            'Orders.createdAt': {
              title: 'Orders Created at',
              shortTitle: 'Created at',
              type: 'time'
            }
          }
        }
      });

      expect(resultSet.tablePivot()).toEqual([
        {
          'Orders.createdAt.day': '2020-01-08T00:00:00.000'
        },
        {
          'Orders.createdAt.day': '2020-01-09T00:00:00.000'
        }
      ]);
    });

    test('same dimension and time dimension without granularity', () => {
      const resultSet = new ResultSet({
        query: {
          measures: [],
          timeDimensions: [
            {
              dimension: 'Orders.createdAt',
              dateRange: ['2020-01-08T00:00:00.000', '2020-01-14T23:59:59.999']
            }
          ],
          dimensions: ['Orders.createdAt'],
          filters: [],
          timezone: 'UTC'
        },
        data: [
          { 'Orders.createdAt': '2020-01-08T17:04:43.000' },
          { 'Orders.createdAt': '2020-01-08T19:28:26.000' },
          { 'Orders.createdAt': '2020-01-09T00:13:01.000' },
          { 'Orders.createdAt': '2020-01-09T00:25:32.000' },
          { 'Orders.createdAt': '2020-01-09T00:43:11.000' },
          { 'Orders.createdAt': '2020-01-09T03:04:00.000' },
          { 'Orders.createdAt': '2020-01-09T04:30:10.000' },
          { 'Orders.createdAt': '2020-01-09T10:25:04.000' },
          { 'Orders.createdAt': '2020-01-09T19:47:19.000' },
          { 'Orders.createdAt': '2020-01-09T19:48:04.000' },
          { 'Orders.createdAt': '2020-01-09T21:46:24.000' },
          { 'Orders.createdAt': '2020-01-09T23:49:37.000' },
          { 'Orders.createdAt': '2020-01-10T09:07:20.000' },
          { 'Orders.createdAt': '2020-01-10T13:50:05.000' }
        ],
        annotation: {
          measures: {},
          dimensions: {
            'Orders.createdAt': {
              title: 'Orders Created at',
              shortTitle: 'Created at',
              type: 'time'
            }
          },
          segments: {},
          timeDimensions: {}
        }
      });

      expect(resultSet.tablePivot()).toEqual([
        { 'Orders.createdAt': '2020-01-08T17:04:43.000' },
        { 'Orders.createdAt': '2020-01-08T19:28:26.000' },
        { 'Orders.createdAt': '2020-01-09T00:13:01.000' },
        { 'Orders.createdAt': '2020-01-09T00:25:32.000' },
        { 'Orders.createdAt': '2020-01-09T00:43:11.000' },
        { 'Orders.createdAt': '2020-01-09T03:04:00.000' },
        { 'Orders.createdAt': '2020-01-09T04:30:10.000' },
        { 'Orders.createdAt': '2020-01-09T10:25:04.000' },
        { 'Orders.createdAt': '2020-01-09T19:47:19.000' },
        { 'Orders.createdAt': '2020-01-09T19:48:04.000' },
        { 'Orders.createdAt': '2020-01-09T21:46:24.000' },
        { 'Orders.createdAt': '2020-01-09T23:49:37.000' },
        { 'Orders.createdAt': '2020-01-10T09:07:20.000' },
        { 'Orders.createdAt': '2020-01-10T13:50:05.000' }
      ]);
    });

    test('order is preserved', () => {
      const resultSet = new ResultSet({
        query: {
          measures: ['User.total'],
          dimensions: ['User.visits'],
          filters: [],
          timezone: 'UTC'
        },
        data: [
          {
            'User.total': 1,
            'User.visits': 1
          },
          {
            'User.total': 15,
            'User.visits': 0.9
          },
          {
            'User.total': 20,
            'User.visits': 0.7
          },
          {
            'User.total': 10,
            'User.visits': 0
          },
        ],
        annotation: {
          measures: {
            'User.total': {}
          },
          dimensions: {
            'User.visits': {
              title: 'User Visits',
              shortTitle: 'Visits',
              type: 'number'
            }
          },
          segments: {},
          timeDimensions: {}
        }
      });

      expect(resultSet.pivot()).toEqual(
        [
          { xValues: [1], yValuesArray: [[['User.total'], 1]] },
          { xValues: [0.9], yValuesArray: [[['User.total'], 15]] },
          { xValues: [0.7], yValuesArray: [[['User.total'], 20]] },
          { xValues: [0], yValuesArray: [[['User.total'], 10]] },
        ]
      );
    });
  });
});
