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

const loadResponse = (query = {}) => ({
  queryType: 'regularQuery',
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
        ...query,
      },
      data: [
        {
          'Orders.ts.day': '2020-08-01T00:00:00.000',
          'Orders.ts': '2020-08-01T00:00:00.000',
          'Orders.count': 1,
        },
        {
          'Orders.ts.day': '2020-08-02T00:00:00.000',
          'Orders.ts': '2020-08-02T00:00:00.000',
          'Orders.count': 2,
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
  ],
  pivotQuery: {
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
    ...query,
  },
});

describe('drill down query', () => {
  const resultSet1 = new ResultSet(loadResponse());
  const resultSet2 = new ResultSet(
    loadResponse({
      timezone: 'America/Los_Angeles',
    })
  );
  const resultSet3 = new ResultSet(
    loadResponse({
      filters: [
        {
          member: 'Users.country',
          operator: 'equals',
          values: ['Los Angeles'],
        },
      ],
    })
  );
  const resultSet4 = new ResultSet(
    loadResponse({
      dimensions: ['Statuses.potential'],
      timeDimensions: []
    })
  );

  it('handles a query with a time dimension', () => {
    expect(
      resultSet1.drillDown({
        xValues: ['2020-08-01T00:00:00.000'],
      })
    ).toEqual({
      measures: [],
      segments: [],
      dimensions: ['Orders.id', 'Orders.title'],
      filters: [
        {
          member: 'Orders.count',
          operator: 'measureFilter',
        },
      ],
      timeDimensions: [
        {
          dimension: 'Orders.ts',
          dateRange: ['2020-08-01T00:00:00.000', '2020-08-01T23:59:59.999'],
        },
      ],
      timezone: 'UTC',
    });
  });

  it('respects the time zone', () => {
    expect(
      resultSet2.drillDown({
        xValues: ['2020-08-01T00:00:00.000'],
      })
    ).toEqual({
      measures: [],
      segments: [],
      dimensions: ['Orders.id', 'Orders.title'],
      filters: [
        {
          member: 'Orders.count',
          operator: 'measureFilter',
        },
      ],
      timeDimensions: [
        {
          dimension: 'Orders.ts',
          dateRange: ['2020-08-01T00:00:00.000', '2020-08-01T23:59:59.999'],
        },
      ],
      timezone: 'America/Los_Angeles',
    });
  });

  it('propagates parent filters', () => {
    expect(
      resultSet3.drillDown({
        xValues: ['2020-08-01T00:00:00.000'],
      })
    ).toEqual({
      measures: [],
      segments: [],
      dimensions: ['Orders.id', 'Orders.title'],
      filters: [
        {
          member: 'Orders.count',
          operator: 'measureFilter',
        },
        {
          member: 'Users.country',
          operator: 'equals',
          values: ['Los Angeles'],
        },
      ],
      timeDimensions: [
        {
          dimension: 'Orders.ts',
          dateRange: ['2020-08-01T00:00:00.000', '2020-08-01T23:59:59.999'],
        },
      ],
      timezone: 'UTC',
    });
  });
  
  it('handles null values', () => {
    expect(
      resultSet4.drillDown({ xvalues: [null] })
    ).toEqual(
      {
        measures: [],
        segments: [],
        dimensions: [
          'Orders.id',
          'Orders.title',
        ],
        filters: [
          {
            member: 'Orders.count',
            operator: 'measureFilter',
          },
          {
            member: 'Statuses.potential',
            operator: 'notSet',
          },
        ],
        timeDimensions: [],
        timezone: 'UTC',
      }
    );
  });
});
