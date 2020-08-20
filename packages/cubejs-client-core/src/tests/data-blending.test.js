import 'jest';
import ResultSet from '../ResultSet';

jest.mock('moment-range', () => {
  const Moment = jest.requireActual('moment');
  const MomentRange = jest.requireActual('moment-range');
  const moment = MomentRange.extendMoment(Moment);
  return {
    extendMoment: () => moment,
  };
});

const loadResponse = {
  queryType: 'BLENDING_QUERY',
  results: [
    {
      query: {
        measures: ['Orders.count'],
        timeDimensions: [
          {
            dimension: 'Orders.ts',
            granularity: 'day',
            dateRange: ['2020-08-01T00:00:00.000', '2020-08-07T23:59:59.999'],
          },
        ],
        filters: [],
        timezone: 'UTC',
        order: [],
        dimensions: [],
      },
      data: [
        {
          'Orders.ts.day': '2020-08-01T00:00:00.000',
          'Orders.ts': '2020-08-01T00:00:00.000',
          'Orders.count': 1,
          'dateTime.day': '2020-08-01T00:00:00.000',
        },
        {
          'Orders.ts.day': '2020-08-02T00:00:00.000',
          'Orders.ts': '2020-08-02T00:00:00.000',
          'Orders.count': 2,
          'dateTime.day': '2020-08-02T00:00:00.000',
        },
      ],
      annotation: {
        measures: {
          'Orders.count': {
            title: 'Orders Count',
            shortTitle: 'Count',
            type: 'number',
            drillMembers: ['Orders.id', 'Orders.title'],
            drillMembersGrouped: {
              measures: [],
              dimensions: ['Orders.id', 'Orders.title'],
            },
          },
        },
        dimensions: {},
        segments: {},
        timeDimensions: {
          'Orders.ts.day': { title: 'Orders Ts', shortTitle: 'Ts', type: 'time' },
          'Orders.ts': { title: 'Orders Ts', shortTitle: 'Ts', type: 'time' },
        },
      },
    },
    {
      query: {
        measures: ['Orders.count'],
        timeDimensions: [
          {
            dimension: 'Orders.ts',
            granularity: 'day',
            dateRange: ['2020-08-01T00:00:00.000', '2020-08-07T23:59:59.999'],
          },
        ],
        filters: [],
        timezone: 'UTC',
        order: [],
        dimensions: ['Users.country'],
      },
      data: [
        {
          'Users.ts.day': '2020-08-01T00:00:00.000',
          'Users.ts': '2020-08-01T00:00:00.000',
          'Users.count': 20,
          'Users.country': 'Australia',
          'dateTime.day': '2020-08-01T00:00:00.000',
        },
        {
          'Users.ts.day': '2020-08-05T00:00:00.000',
          'Users.ts': '2020-08-05T00:00:00.000',
          'Users.count': 34,
          'Users.country': 'Spain',
          'dateTime.day': '2020-08-05T00:00:00.000',
        },
        {
          'Users.ts.day': '2020-08-05T00:00:00.000',
          'Users.ts': '2020-08-05T00:00:00.000',
          'Users.count': 34,
          'Users.country': 'Italy',
          'dateTime.day': '2020-08-05T00:00:00.000',
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
          'Users.ts.day': { title: 'Orders Ts', shortTitle: 'Ts', type: 'time' },
          'Users.ts': { title: 'Orders Ts', shortTitle: 'Ts', type: 'time' },
        },
      },
    },
  ],
};

describe('data blending', () => {
  const resultSet1 = new ResultSet(loadResponse);

  describe('with different dimensions', () => {
    test('normalized pivotConfig', () => {
      expect(resultSet1.normalizePivotConfig()).toStrictEqual({
        x: ['dateTime.day'],
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
          ]
        },
        {
          xValues: ['2020-08-02T00:00:00.000'],
          yValuesArray: [
            [[null, 'Orders.count'], 2],
            [['Australia', 'Users.count'], 0],
            [['Spain', 'Users.count'], 0],
            [['Italy', 'Users.count'], 0],
          ]
        }
      ]);
    });
  });
});
