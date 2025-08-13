/**
 * @license Apache-2.0
 * @copyright Cube Dev, Inc.
 * @fileoverview ResultSet class unit tests.
 */

/* globals describe,test,expect */

import 'jest';
import ResultSet from '../src/ResultSet';
import { TimeDimension } from '../src';
import { DescriptiveQueryResponse } from './helpers';

describe('ResultSet', () => {
  describe('timeSeries', () => {
    test('it generates array of dates - granularity month', () => {
      const resultSet = new ResultSet({} as any);
      const timeDimension: TimeDimension = {
        dateRange: ['2015-01-01', '2015-12-31'],
        granularity: 'month',
        dimension: 'Events.time'
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
      const resultSet = new ResultSet({} as any);
      const timeDimension: TimeDimension = {
        dateRange: ['2015-01-01', '2015-12-31'],
        granularity: 'quarter',
        dimension: 'Events.time'
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
      const resultSet = new ResultSet({} as any);
      const timeDimension: TimeDimension = {
        dateRange: ['2015-01-01', '2015-01-01'],
        granularity: 'hour',
        dimension: 'Events.time'
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
      const resultSet = new ResultSet({} as any);
      const timeDimension: TimeDimension = {
        dateRange: ['2015-01-01T10:30:00.000', '2015-01-01T13:59:00.000'],
        granularity: 'hour',
        dimension: 'Events.time'
      };
      const output = [
        '2015-01-01T10:00:00.000',
        '2015-01-01T11:00:00.000',
        '2015-01-01T12:00:00.000',
        '2015-01-01T13:00:00.000'
      ];
      expect(resultSet.timeSeries(timeDimension)).toEqual(output);
    });

    test('it generates array of dates - custom interval - 1 year, origin - 2020-01-01', () => {
      const resultSet = new ResultSet({} as any);
      const timeDimension: TimeDimension = {
        dateRange: ['2021-01-01', '2023-12-31'],
        granularity: 'one_year',
        dimension: 'Events.time'
      };
      const output = [
        '2021-01-01T00:00:00.000',
        '2022-01-01T00:00:00.000',
        '2023-01-01T00:00:00.000'
      ];
      expect(resultSet.timeSeries(timeDimension, 1, {
        'Events.time.one_year': {
          title: 'Time Dimension',
          shortTitle: 'TD',
          type: 'time',
          granularity: {
            name: '1 year',
            title: '1 year',
            interval: '1 year',
            origin: '2020-01-01',
          },
        },
      })).toEqual(output);
    });

    test('it generates array of dates - custom interval - 1 year, origin - 2025-03-01', () => {
      const resultSet = new ResultSet({} as any);
      const timeDimension: TimeDimension = {
        dateRange: ['2021-01-01', '2022-12-31'],
        granularity: 'one_year',
        dimension: 'Events.time'
      };
      const output = [
        '2020-03-01T00:00:00.000',
        '2021-03-01T00:00:00.000',
        '2022-03-01T00:00:00.000',
      ];
      expect(resultSet.timeSeries(timeDimension, 1, {
        'Events.time.one_year': {
          title: 'Time Dimension',
          shortTitle: 'TD',
          type: 'time',
          granularity: {
            name: '1 year',
            title: '1 year',
            interval: '1 year',
            origin: '2025-03-01',
          },
        },
      })).toEqual(output);
    });

    test('it generates array of dates - custom interval - 1 year, offset - 2 months', () => {
      const resultSet = new ResultSet({} as any);
      const timeDimension: TimeDimension = {
        dateRange: ['2021-01-01', '2022-12-31'],
        granularity: 'one_year',
        dimension: 'Events.time'
      };
      const output = [
        '2020-03-01T00:00:00.000',
        '2021-03-01T00:00:00.000',
        '2022-03-01T00:00:00.000',
      ];
      expect(resultSet.timeSeries(timeDimension, 1, {
        'Events.time.one_year': {
          title: 'Time Dimension',
          shortTitle: 'TD',
          type: 'time',
          granularity: {
            name: '1 year',
            title: '1 year',
            interval: '1 year',
            offset: '2 months',
          },
        },
      })).toEqual(output);
    });

    test('it generates array of dates - custom interval - 2 months, origin - 2019-01-01', () => {
      const resultSet = new ResultSet({} as any);
      const timeDimension: TimeDimension = {
        dateRange: ['2021-01-01', '2021-12-31'],
        granularity: 'two_months',
        dimension: 'Events.time'
      };
      const output = [
        '2021-01-01T00:00:00.000',
        '2021-03-01T00:00:00.000',
        '2021-05-01T00:00:00.000',
        '2021-07-01T00:00:00.000',
        '2021-09-01T00:00:00.000',
        '2021-11-01T00:00:00.000',
      ];
      expect(resultSet.timeSeries(timeDimension, 1, {
        'Events.time.two_months': {
          title: 'Time Dimension',
          shortTitle: 'TD',
          type: 'time',
          granularity: {
            name: '2 months',
            title: '2 months',
            interval: '2 months',
            origin: '2019-01-01',
          },
        },
      })).toEqual(output);
    });

    test('it generates array of dates - custom interval - 2 months, no offset', () => {
      const resultSet = new ResultSet({} as any);
      const timeDimension: TimeDimension = {
        dateRange: ['2021-01-01', '2021-12-31'],
        granularity: 'two_months',
        dimension: 'Events.time'
      };
      const output = [
        '2021-01-01T00:00:00.000',
        '2021-03-01T00:00:00.000',
        '2021-05-01T00:00:00.000',
        '2021-07-01T00:00:00.000',
        '2021-09-01T00:00:00.000',
        '2021-11-01T00:00:00.000',
      ];
      expect(resultSet.timeSeries(timeDimension, 1, {
        'Events.time.two_months': {
          title: 'Time Dimension',
          shortTitle: 'TD',
          type: 'time',
          granularity: {
            name: '2 months',
            title: '2 months',
            interval: '2 months',
          },
        },
      })).toEqual(output);
    });

    test('it generates array of dates - custom interval - 2 months, origin - 2019-03-15', () => {
      const resultSet = new ResultSet({} as any);
      const timeDimension: TimeDimension = {
        dateRange: ['2021-01-01', '2021-12-31'],
        granularity: 'two_months',
        dimension: 'Events.time'
      };
      const output = [
        '2020-11-15T00:00:00.000',
        '2021-01-15T00:00:00.000',
        '2021-03-15T00:00:00.000',
        '2021-05-15T00:00:00.000',
        '2021-07-15T00:00:00.000',
        '2021-09-15T00:00:00.000',
        '2021-11-15T00:00:00.000',
      ];
      expect(resultSet.timeSeries(timeDimension, 1, {
        'Events.time.two_months': {
          title: 'Time Dimension',
          shortTitle: 'TD',
          type: 'time',
          granularity: {
            name: '2 months',
            title: '2 months',
            interval: '2 months',
            origin: '2019-03-15',
          },
        },
      })).toEqual(output);
    });

    test('it generates array of dates - custom interval - 1 months 2 weeks 3 days, origin - 2021-01-25', () => {
      const resultSet = new ResultSet({} as any);
      const timeDimension: TimeDimension = {
        dateRange: ['2021-01-01', '2021-12-31'],
        granularity: 'one_mo_two_we_three_d',
        dimension: 'Events.time'
      };
      const output = [
        '2020-12-08T00:00:00.000',
        '2021-01-25T00:00:00.000',
        '2021-03-14T00:00:00.000',
        '2021-05-01T00:00:00.000',
        '2021-06-18T00:00:00.000',
        '2021-08-04T00:00:00.000',
        '2021-09-21T00:00:00.000',
        '2021-11-07T00:00:00.000',
        '2021-12-24T00:00:00.000',
      ];
      expect(resultSet.timeSeries(timeDimension, 1, {
        'Events.time.one_mo_two_we_three_d': {
          title: 'Time Dimension',
          shortTitle: 'TD',
          type: 'time',
          granularity: {
            name: '1 months 2 weeks 3 days',
            title: '1 months 2 weeks 3 days',
            interval: '1 months 2 weeks 3 days',
            origin: '2021-01-25',
          },
        },
      })).toEqual(output);
    });

    test('it generates array of dates - custom interval - 3 weeks, origin - 2020-12-15', () => {
      const resultSet = new ResultSet({} as any);
      const timeDimension: TimeDimension = {
        dateRange: ['2021-01-01', '2021-03-01'],
        granularity: 'three_weeks',
        dimension: 'Events.time'
      };
      const output = [
        '2020-12-15T00:00:00.000',
        '2021-01-05T00:00:00.000',
        '2021-01-26T00:00:00.000',
        '2021-02-16T00:00:00.000',
      ];
      expect(resultSet.timeSeries(timeDimension, 1, {
        'Events.time.three_weeks': {
          title: 'Time Dimension',
          shortTitle: 'TD',
          type: 'time',
          granularity: {
            name: '3 weeks',
            title: '3 weeks',
            interval: '3 weeks',
            origin: '2020-12-15',
          },
        },
      })).toEqual(output);
    });

    test('it generates array of dates - custom interval - 2 months 3 weeks 4 days 5 hours 6 minutes 7 seconds, origin - 2021-01-01', () => {
      const resultSet = new ResultSet({} as any);
      const timeDimension: TimeDimension = {
        dateRange: ['2021-01-01', '2021-12-31'],
        granularity: 'two_mo_3w_4d_5h_6m_7s',
        dimension: 'Events.time'
      };
      const output = [
        '2021-01-01T00:00:00.000',
        '2021-03-26T05:06:07.000',
        '2021-06-20T10:12:14.000',
        '2021-09-14T15:18:21.000',
        '2021-12-09T20:24:28.000',
      ];
      expect(resultSet.timeSeries(timeDimension, 1, {
        'Events.time.two_mo_3w_4d_5h_6m_7s': {
          title: 'Time Dimension',
          shortTitle: 'TD',
          type: 'time',
          granularity: {
            name: 'two_mo_3w_4d_5h_6m_7s',
            title: 'two_mo_3w_4d_5h_6m_7s',
            interval: '2 months 3 weeks 4 days 5 hours 6 minutes 7 seconds',
            origin: '2021-01-01',
          },
        },
      })).toEqual(output);
    });

    test('it generates array of dates - custom interval - 10 minutes 15 seconds, origin - 2021-02-01 09:59:45', () => {
      const resultSet = new ResultSet({} as any);
      const timeDimension: TimeDimension = {
        dateRange: ['2021-02-01 10:00:00', '2021-02-01 12:00:00'],
        granularity: 'ten_min_fifteen_sec',
        dimension: 'Events.time'
      };
      const output = [
        '2021-02-01T09:59:45.000',
        '2021-02-01T10:10:00.000',
        '2021-02-01T10:20:15.000',
        '2021-02-01T10:30:30.000',
        '2021-02-01T10:40:45.000',
        '2021-02-01T10:51:00.000',
        '2021-02-01T11:01:15.000',
        '2021-02-01T11:11:30.000',
        '2021-02-01T11:21:45.000',
        '2021-02-01T11:32:00.000',
        '2021-02-01T11:42:15.000',
        '2021-02-01T11:52:30.000',
      ];
      expect(resultSet.timeSeries(timeDimension, 1, {
        'Events.time.ten_min_fifteen_sec': {
          title: 'Time Dimension',
          shortTitle: 'TD',
          type: 'time',
          granularity: {
            name: '10 minutes 15 seconds',
            title: '10 minutes 15 seconds',
            interval: '10 minutes 15 seconds',
            origin: '2021-02-01 09:59:45',
          },
        },
      })).toEqual(output);
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
      } as any);

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
      } as any);

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
      } as any);

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
      } as any);

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
        } as any,
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

  test('tableColumns', () => {
    const resultSet = new ResultSet(DescriptiveQueryResponse as any);

    expect(resultSet.tableColumns()).toEqual([
      {
        dataIndex: 'base_orders.created_at.month',
        format: undefined,
        key: 'base_orders.created_at.month',
        meta: undefined,
        shortTitle: 'Created at',
        title: 'Base Orders Created at',
        type: 'time',
      },
      {
        dataIndex: 'base_orders.status',
        format: undefined,
        key: 'base_orders.status',
        meta: {
          addDesc: 'The status of order',
          moreNum: 42,
        },
        shortTitle: 'Status',
        title: 'Base Orders Status',
        type: 'string',
      },
      {
        dataIndex: 'base_orders.count',
        format: undefined,
        key: 'base_orders.count',
        meta: undefined,
        shortTitle: 'Count',
        title: 'Base Orders Count',
        type: 'number',
      },
    ]);
  });

  test('totalRow', () => {
    const resultSet = new ResultSet(DescriptiveQueryResponse as any);

    expect(resultSet.totalRow()).toEqual({
      'completed,base_orders.count': 2,
      'processing,base_orders.count': 0,
      'shipped,base_orders.count': 0,
      x: '2023-04-01T00:00:00.000',
      xValues: [
        '2023-04-01T00:00:00.000',
      ],
    });
  });

  test('pivotQuery', () => {
    const resultSet = new ResultSet(DescriptiveQueryResponse as any);

    expect(resultSet.pivotQuery()).toEqual(DescriptiveQueryResponse.pivotQuery);
  });

  test('totalRows', () => {
    const resultSet = new ResultSet(DescriptiveQueryResponse as any);

    expect(resultSet.totalRows()).toEqual(19);
  });

  test('rawData', () => {
    const resultSet = new ResultSet(DescriptiveQueryResponse as any);

    expect(resultSet.rawData()).toEqual(DescriptiveQueryResponse.results[0].data);
  });

  test('annotation', () => {
    const resultSet = new ResultSet(DescriptiveQueryResponse as any);

    expect(resultSet.annotation()).toEqual(DescriptiveQueryResponse.results[0].annotation);
  });

  test('categories', () => {
    const resultSet = new ResultSet(DescriptiveQueryResponse as any);

    expect(resultSet.categories()).toEqual([
      {
        'completed,base_orders.count': 2,
        'processing,base_orders.count': 0,
        'shipped,base_orders.count': 0,
        x: '2023-04-01T00:00:00.000',
        xValues: [
          '2023-04-01T00:00:00.000',
        ],
      },
      {
        'completed,base_orders.count': 6,
        'processing,base_orders.count': 6,
        'shipped,base_orders.count': 9,
        x: '2023-05-01T00:00:00.000',
        xValues: [
          '2023-05-01T00:00:00.000',
        ],
      },
      {
        'completed,base_orders.count': 5,
        'processing,base_orders.count': 5,
        'shipped,base_orders.count': 13,
        x: '2023-06-01T00:00:00.000',
        xValues: [
          '2023-06-01T00:00:00.000',
        ],
      },
      {
        'completed,base_orders.count': 5,
        'processing,base_orders.count': 7,
        'shipped,base_orders.count': 5,
        x: '2023-07-01T00:00:00.000',
        xValues: [
          '2023-07-01T00:00:00.000',
        ],
      },
      {
        'completed,base_orders.count': 11,
        'processing,base_orders.count': 3,
        'shipped,base_orders.count': 4,
        x: '2023-08-01T00:00:00.000',
        xValues: [
          '2023-08-01T00:00:00.000',
        ],
      },
      {
        'completed,base_orders.count': 5,
        'processing,base_orders.count': 10,
        'shipped,base_orders.count': 9,
        x: '2023-09-01T00:00:00.000',
        xValues: [
          '2023-09-01T00:00:00.000',
        ],
      },
      {
        'completed,base_orders.count': 4,
        'processing,base_orders.count': 5,
        'shipped,base_orders.count': 9,
        x: '2023-10-01T00:00:00.000',
        xValues: [
          '2023-10-01T00:00:00.000',
        ],
      },
    ]);
  });

  test('serialize/deserialize', () => {
    const resultSet = new ResultSet(DescriptiveQueryResponse as any);

    const serialized = resultSet.serialize();
    const restoredResultSet = ResultSet.deserialize(serialized);

    expect(restoredResultSet).toEqual(resultSet);
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
      } as any);

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
      } as any);

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
      } as any);

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
      } as any);

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
      } as any);

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
      } as any);

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
      } as any);

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
      } as any);

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
      } as any);

      expect(resultSet.tablePivot()).toEqual([
        {
          'Orders.createdAt.day': '2020-01-08T00:00:00.000'
        },
        {
          'Orders.createdAt.day': '2020-01-09T00:00:00.000'
        }
      ]);
    });

    test('fill missing dates with custom value', () => {
      const resultSet = new ResultSet({
        query: {
          measures: ['Orders.total'],
          timeDimensions: [
            {
              dimension: 'Orders.createdAt',
              granularity: 'day',
              dateRange: ['2020-01-08T00:00:00.000', '2020-01-11T23:59:59.999']
            }
          ],
          filters: [],
          timezone: 'UTC'
        },
        data: [
          {
            'Orders.createdAt': '2020-01-08T00:00:00.000',
            'Orders.total': 1
          },
          {
            'Orders.createdAt': '2020-01-10T00:00:00.000',
            'Orders.total': 10
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
      } as any);

      expect(resultSet.tablePivot({
        fillWithValue: 5
      })).toEqual([
        {
          'Orders.createdAt.day': '2020-01-08T00:00:00.000',
          'Orders.total': 1
        },
        {
          'Orders.createdAt.day': '2020-01-09T00:00:00.000',
          'Orders.total': 5
        },
        {
          'Orders.createdAt.day': '2020-01-10T00:00:00.000',
          'Orders.total': 10
        },
        {
          'Orders.createdAt.day': '2020-01-11T00:00:00.000',
          'Orders.total': 5
        }
      ]);
    });

    test('fill missing dates with custom string', () => {
      const resultSet = new ResultSet({
        query: {
          measures: ['Orders.total'],
          timeDimensions: [
            {
              dimension: 'Orders.createdAt',
              granularity: 'day',
              dateRange: ['2020-01-08T00:00:00.000', '2020-01-11T23:59:59.999']
            }
          ],
          filters: [],
          timezone: 'UTC'
        },
        data: [
          {
            'Orders.createdAt': '2020-01-08T00:00:00.000',
            'Orders.total': 1
          },
          {
            'Orders.createdAt': '2020-01-10T00:00:00.000',
            'Orders.total': 10
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
      } as any);

      expect(resultSet.tablePivot({
        fillWithValue: 'N/A'
      })).toEqual([
        {
          'Orders.createdAt.day': '2020-01-08T00:00:00.000',
          'Orders.total': 1
        },
        {
          'Orders.createdAt.day': '2020-01-09T00:00:00.000',
          'Orders.total': 'N/A'
        },
        {
          'Orders.createdAt.day': '2020-01-10T00:00:00.000',
          'Orders.total': 10
        },
        {
          'Orders.createdAt.day': '2020-01-11T00:00:00.000',
          'Orders.total': 'N/A'
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
      } as any);

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
      } as any);

      expect(resultSet.pivot()).toEqual(
        [
          { xValues: [1], yValuesArray: [[['User.total'], 1]] },
          { xValues: [0.9], yValuesArray: [[['User.total'], 15]] },
          { xValues: [0.7], yValuesArray: [[['User.total'], 20]] },
          { xValues: [0], yValuesArray: [[['User.total'], 10]] },
        ]
      );
    });

    test('keeps null values on non-matching rows', () => {
      const resultSet = new ResultSet({
        query: {
          dimensions: [
            'User.name',
            'Friend.name'
          ],
        },
        data: [
          {
            'User.name': 'Bob',
            'Friend.name': null,
          }
        ],
      } as any);

      expect(resultSet.tablePivot()).toEqual(
        [
          { 'User.name': 'Bob', 'Friend.name': null },
        ]
      );
    });
  });
});
