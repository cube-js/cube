import 'jest';

import ResultSet from '../ResultSet';
import { loadResponse, loadResponseWithoutDateRange } from './fixtures/datablending/load-responses.json';

describe('data blending', () => {
  const resultSet1 = new ResultSet(loadResponse);

  describe('with different dimensions', () => {
    test('normalized pivotConfig', () => {
      expect(resultSet1.normalizePivotConfig()).toStrictEqual({
        x: ['time.day'],
        y: ['Users.country', 'measures'],
        fillMissingDates: true,
        joinDateRange: false,
      });
    });

    test('pivot', () => {
      expect(resultSet1.pivot()).toStrictEqual([
        {
          xValues: ['2020-08-01T00:00:00.000'],
          yValuesArray: [
            [[null, 'Orders.count'], 1],
            [['Australia', 'Users.count'], 20],
            [['Spain', 'Users.count'], 0],
            [['Italy', 'Users.count'], 0],
          ],
        },
        {
          xValues: ['2020-08-02T00:00:00.000'],
          yValuesArray: [
            [[null, 'Orders.count'], 2],
            [['Australia', 'Users.count'], 0],
            [['Spain', 'Users.count'], 0],
            [['Italy', 'Users.count'], 0],
          ],
        },
        {
          xValues: ['2020-08-03T00:00:00.000'],
          yValuesArray: [
            [[null, 'Orders.count'], 0],
            [['Australia', 'Users.count'], 0],
            [['Spain', 'Users.count'], 0],
            [['Italy', 'Users.count'], 0],
          ],
        },
        {
          xValues: ['2020-08-04T00:00:00.000'],
          yValuesArray: [
            [[null, 'Orders.count'], 0],
            [['Australia', 'Users.count'], 0],
            [['Spain', 'Users.count'], 0],
            [['Italy', 'Users.count'], 0],
          ],
        },
        {
          xValues: ['2020-08-05T00:00:00.000'],
          yValuesArray: [
            [[null, 'Orders.count'], 0],
            [['Australia', 'Users.count'], 0],
            [['Spain', 'Users.count'], 34],
            [['Italy', 'Users.count'], 18],
          ],
        },
        {
          xValues: ['2020-08-06T00:00:00.000'],
          yValuesArray: [
            [[null, 'Orders.count'], 0],
            [['Australia', 'Users.count'], 0],
            [['Spain', 'Users.count'], 0],
            [['Italy', 'Users.count'], 0],
          ],
        },
        {
          xValues: ['2020-08-07T00:00:00.000'],
          yValuesArray: [
            [[null, 'Orders.count'], 0],
            [['Australia', 'Users.count'], 0],
            [['Spain', 'Users.count'], 0],
            [['Italy', 'Users.count'], 0],
          ],
        },
      ]);
    });
  });

  test('query with a single measure', () => {
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
            {
              'Users.ts.month': '2020-09-01T00:00:00.000',
              'Users.ts': '2020-09-01T00:00:00.000',
              'Users.count': 23,
              'time.month': '2020-09-01T00:00:00.000',
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
            {
              'Users.ts.month': '2020-09-01T00:00:00.000',
              'Users.ts': '2020-09-05T00:00:00.000',
              'Users.count': 4,
              'time.month': '2020-09-01T00:00:00.000',
            },
            {
              'Users.ts.month': '2020-10-01T00:00:00.000',
              'Users.ts': '2020-10-05T00:00:00.000',
              'Users.count': 7,
              'time.month': '2020-10-01T00:00:00.000',
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

    expect(resultSet.chartPivot()).toEqual([
      {
        x: '2020-07-01T00:00:00.000',
        '0,Users.count': 0,
        '1,Users.count': 0,
        xValues: ['2020-07-01T00:00:00.000'],
      },
      {
        x: '2020-08-01T00:00:00.000',
        '0,Users.count': 14,
        '1,Users.count': 2,
        xValues: ['2020-08-01T00:00:00.000'],
      },
      {
        x: '2020-09-01T00:00:00.000',
        '0,Users.count': 23,
        '1,Users.count': 4,
        xValues: ['2020-09-01T00:00:00.000'],
      },
      {
        x: '2020-10-01T00:00:00.000',
        '0,Users.count': 0,
        '1,Users.count': 7,
        xValues: ['2020-10-01T00:00:00.000'],
      },
      {
        x: '2020-11-01T00:00:00.000',
        '0,Users.count': 0,
        '1,Users.count': 0,
        xValues: ['2020-11-01T00:00:00.000'],
      },
    ]);
  });

  test('query without date range', () => {
    const resultSet = new ResultSet(loadResponseWithoutDateRange);

    expect(resultSet.chartPivot()).toEqual([
      {
        x: '2020-08-01T00:00:00.000',
        xValues: ['2020-08-01T00:00:00.000'],
        'Orders.count': 1,
        'Australia,Users.count': 20,
        'Italy,Users.count': 0,
        'Spain,Users.count': 0
      },
      {
        x: '2020-08-02T00:00:00.000',
        xValues: ['2020-08-02T00:00:00.000'],
        'Orders.count': 2,
        'Australia,Users.count': 0,
        'Spain,Users.count': 34,
        'Italy,Users.count': 18,
      },
    ]);
  });

  test('query with a single measure and a custom series alias', () => {
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
            {
              'Users.ts.month': '2020-09-01T00:00:00.000',
              'Users.ts': '2020-09-01T00:00:00.000',
              'Users.count': 23,
              'time.month': '2020-09-01T00:00:00.000',
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
            {
              'Users.ts.month': '2020-09-01T00:00:00.000',
              'Users.ts': '2020-09-05T00:00:00.000',
              'Users.count': 4,
              'time.month': '2020-09-01T00:00:00.000',
            },
            {
              'Users.ts.month': '2020-10-01T00:00:00.000',
              'Users.ts': '2020-10-05T00:00:00.000',
              'Users.count': 7,
              'time.month': '2020-10-01T00:00:00.000',
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

    expect(resultSet.chartPivot({ aliasSeries: ['one', 'two'] })).toEqual([
      {
        x: '2020-07-01T00:00:00.000',
        'one,Users.count': 0,
        'two,Users.count': 0,
        xValues: ['2020-07-01T00:00:00.000'],
      },
      {
        x: '2020-08-01T00:00:00.000',
        'one,Users.count': 14,
        'two,Users.count': 2,
        xValues: ['2020-08-01T00:00:00.000'],
      },
      {
        x: '2020-09-01T00:00:00.000',
        'one,Users.count': 23,
        'two,Users.count': 4,
        xValues: ['2020-09-01T00:00:00.000'],
      },
      {
        x: '2020-10-01T00:00:00.000',
        'one,Users.count': 0,
        'two,Users.count': 7,
        xValues: ['2020-10-01T00:00:00.000'],
      },
      {
        x: '2020-11-01T00:00:00.000',
        'one,Users.count': 0,
        'two,Users.count': 0,
        xValues: ['2020-11-01T00:00:00.000'],
      },
    ]);
  });
});
